"""
A股股票扫描器 — 扫描脚本

数据源: 东方财富 (免费, 盘中实时更新, 支持分钟线)
缓存: cache/klines.json 存储历史K线, 增量更新
周期: 15m, 60m, 日线(D), 周线(W)
标签: 趋势共振 · 成交量异动 · 大盘方向 · 防追高
      · 龙头股 · 仙人指路 · 波动充足 · 小量大涨 · 盘整突破
      · 潜伏信号 (盘整蓄势 + 底部支撑)
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import math
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import gzip

import aiohttp

# ── 路径 ──
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CACHE_DIR = ROOT / "cache"
CACHE_FILE = CACHE_DIR / "klines.json"  # legacy single file
CACHE_PREFIXES = ["sh", "sz"]

CYCLES = ["W", "D", "60", "15"]

# 每个周期缓存保留根数
# 15m 保留 160 根 (约 2 周), 确保增量模式下有足够数据合成 D/W
CYCLE_MAX_BARS = {"15": 160, "60": 210, "D": 210, "W": 50}

# 东方财富 K线周期映射
EM_KLT = {"15": "15", "60": "60", "D": "101", "W": "102"}
# 东方财富 前复权=1
EM_FQTYPE = "1"

# 代理配置
_cfg_file = ROOT / "config.local.json"
_cfg = json.loads(_cfg_file.read_text()) if _cfg_file.exists() else {}
_proxy_cfg = _cfg.get("proxy", {})
proxy_url = (f"http://{_proxy_cfg['host']}:{_proxy_cfg['port']}"
             if _proxy_cfg.get("enabled") else None)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("scan")

# 东方财富 K 线源开关 (连续失败后自动禁用, 避免浪费重试)
_em_kline_fail_count = 0
_em_kline_disabled = False
_EM_FAIL_THRESHOLD = 5  # 连续失败 5 次后禁用


# ══════════════════════════════════════════════════════════════
#  缓存管理
# ══════════════════════════════════════════════════════════════

def load_cache() -> dict:
    # 优先读取 .gz 压缩分片，兼容旧的未压缩格式
    merged: dict = {}
    found_shards = False
    for prefix in CACHE_PREFIXES:
        gz_shard = CACHE_DIR / f"klines_{prefix}.json.gz"
        json_shard = CACHE_DIR / f"klines_{prefix}.json"
        if gz_shard.exists():
            try:
                merged.update(json.loads(gzip.decompress(gz_shard.read_bytes())))
                found_shards = True
            except Exception as e:
                log.warning("缓存分片 %s 加载失败: %s", gz_shard.name, e)
        elif json_shard.exists():
            try:
                merged.update(json.loads(json_shard.read_text()))
                found_shards = True
            except Exception as e:
                log.warning("缓存分片 %s 加载失败: %s", json_shard.name, e)
    if found_shards:
        # 归一化 key: 去掉 "sh."/"sz."/"bj." 前缀, 统一用纯数字 code
        normalized = {}
        for k, v in merged.items():
            pure = k.split(".")[-1] if "." in k else k
            normalized[pure] = v
        log.info("加载缓存(分片): %d 只股票", len(normalized))
        return normalized
    # fallback: 旧的单文件
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text())
            # 归一化 key
            normalized = {}
            for k, v in data.items():
                pure = k.split(".")[-1] if "." in k else k
                normalized[pure] = v
            log.info("加载缓存(单文件): %d 只股票", len(normalized))
            return normalized
        except Exception as e:
            log.warning("缓存加载失败: %s", e)
    return {}


def save_cache(all_sym: dict):
    slim = {}
    for code, sym in all_sym.items():
        slim[code] = {"name": sym.get("name", "")}
        for cycle in CYCLES:
            if cycle in sym and "data" in sym[cycle]:
                keep = CYCLE_MAX_BARS.get(cycle, 50)
                slim[code][cycle] = {"data": sym[cycle]["data"][-keep:]}
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # 按交易所前缀拆分写入 (code 为纯数字, 6/9 开头归 sh, 其余归 sz)
    def _prefix(code: str) -> str:
        return "sh" if code.startswith(("6", "9")) else "sz"

    def _compress_shard(prefix: str, shard: dict) -> tuple[str, int, float]:
        gz_file = CACHE_DIR / f"klines_{prefix}.json.gz"
        raw = json.dumps(shard, ensure_ascii=False).encode()
        gz_file.write_bytes(gzip.compress(raw))
        size_mb = gz_file.stat().st_size / 1024 / 1024
        # 删除旧的未压缩 JSON
        json_file = CACHE_DIR / f"klines_{prefix}.json"
        if json_file.exists():
            json_file.unlink()
        return prefix, len(shard), size_mb

    shards = {prefix: {k: v for k, v in slim.items() if _prefix(k) == prefix}
              for prefix in CACHE_PREFIXES}
    total_mb = 0.0
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(CACHE_PREFIXES)) as pool:
        futures = {pool.submit(_compress_shard, p, s): p
                   for p, s in shards.items() if s}
        for f in concurrent.futures.as_completed(futures):
            prefix, count, size_mb = f.result()
            total_mb += size_mb
            log.info("缓存分片 %s: %d 只股票, %.1f MB (gz)", prefix, count, size_mb)
    # 删除旧的单文件
    if CACHE_FILE.exists():
        CACHE_FILE.unlink()
        log.info("已删除旧缓存文件 klines.json")
    log.info("缓存已保存: %d 只股票, 总计 %.1f MB (gz)", len(slim), total_mb)


def merge_klines(old: list[list], new: list[list], cycle: str = "D") -> list[list]:
    if not old:
        return new
    if not new:
        return old
    merged = {bar[0]: bar for bar in old}
    for bar in new:
        merged[bar[0]] = bar
    keep = CYCLE_MAX_BARS.get(cycle, 50)
    return [merged[k] for k in sorted(merged.keys())][-keep:]


# ══════════════════════════════════════════════════════════════
#  15m → 60m / D / W 合成
# ══════════════════════════════════════════════════════════════

# A股交易时段 (15m K线时间戳 → 所属60m段)
# 上午: 09:30-10:30, 10:30-11:30  下午: 13:00-14:00, 14:00-15:00
_60M_SLOT = {
    "09:45": "10:30", "10:00": "10:30", "10:15": "10:30", "10:30": "10:30",
    "10:45": "11:30", "11:00": "11:30", "11:15": "11:30", "11:30": "11:30",
    "13:15": "14:00", "13:30": "14:00", "13:45": "14:00", "14:00": "14:00",
    "14:15": "15:00", "14:30": "15:00", "14:45": "15:00", "15:00": "15:00",
}


def _ts_date(ts: str) -> str:
    """从 '2026-04-18 10:30' 或 '2026-04-18' 提取日期部分"""
    return ts[:10]


def _ts_time(ts: str) -> str:
    """从 '2026-04-18 10:30' 提取时间部分"""
    return ts[11:16] if len(ts) > 10 else ""


def _monday_of(date_str: str) -> str:
    """返回 date_str 所在周的周一日期字符串"""
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")


def _merge_bar(base: list, bar: list) -> list:
    """将 bar 合并到 base (OHLCVA), base 就地更新并返回"""
    # base: [ts, open, high, low, close, volume, amount]
    base[2] = max(base[2], bar[2])   # high
    base[3] = min(base[3], bar[3])   # low
    base[4] = bar[4]                 # close = 最新 close
    base[5] += bar[5]                # volume 累加
    base[6] += bar[6]                # amount 累加
    return base


def aggregate_15m_to_60m(bars_15m: list[list]) -> list[list]:
    """将 15m K线合成 60m K线"""
    groups: dict[str, list] = {}  # key = "2026-04-18 10:30"
    for bar in bars_15m:
        t = _ts_time(bar[0])
        slot = _60M_SLOT.get(t)
        if not slot:
            continue
        key = f"{_ts_date(bar[0])} {slot}"
        if key not in groups:
            groups[key] = [key, bar[1], bar[2], bar[3], bar[4], bar[5], bar[6]]
        else:
            _merge_bar(groups[key], bar)
    return [groups[k] for k in sorted(groups.keys())]


def aggregate_15m_to_daily(bars_15m: list[list]) -> list[list]:
    """将 15m K线合成日线"""
    groups: dict[str, list] = {}  # key = "2026-04-18"
    for bar in bars_15m:
        d = _ts_date(bar[0])
        if d not in groups:
            groups[d] = [d, bar[1], bar[2], bar[3], bar[4], bar[5], bar[6]]
        else:
            _merge_bar(groups[d], bar)
    return [groups[k] for k in sorted(groups.keys())]


def aggregate_daily_to_weekly(bars_d: list[list]) -> list[list]:
    """将日线合成周线"""
    groups: dict[str, list] = {}  # key = 周一日期
    for bar in bars_d:
        w = _monday_of(bar[0])
        if w not in groups:
            groups[w] = [w, bar[1], bar[2], bar[3], bar[4], bar[5], bar[6]]
        else:
            _merge_bar(groups[w], bar)
    return [groups[k] for k in sorted(groups.keys())]


def aggregate_from_15m(cached_sym: dict, fresh_15m: list[list]) -> dict:
    """用新拉取的 15m 数据增量更新 60m / D / W 缓存.

    策略:
    - 15m: 直接 merge
    - 60m: 从 fresh_15m 合成新的 60m bars, merge 到缓存
    - D:   从 fresh_15m 合成新的日线 bars, merge 到缓存
    - W:   从更新后的 D 数据合成周线 (保证完整性)
    """
    sym = {}
    for k, v in cached_sym.items():
        if k == "name":
            sym[k] = v
        else:
            sym[k] = {"data": list(v.get("data", []))}

    # 15m: merge
    old_15 = sym.get("15", {}).get("data", [])
    sym["15"] = {"data": merge_klines(old_15, fresh_15m, "15")}

    # 60m: 从 fresh_15m 合成增量 60m bars, merge 到缓存
    new_60 = aggregate_15m_to_60m(fresh_15m)
    old_60 = sym.get("60", {}).get("data", [])
    sym["60"] = {"data": merge_klines(old_60, new_60, "60")}

    # D: 从 fresh_15m 合成增量日线, merge 到缓存
    new_d = aggregate_15m_to_daily(fresh_15m)
    old_d = sym.get("D", {}).get("data", [])
    sym["D"] = {"data": merge_klines(old_d, new_d, "D")}

    # W: 从完整 D 数据重新合成 (确保周线完整)
    all_d = sym["D"]["data"]
    sym["W"] = {"data": aggregate_daily_to_weekly(all_d)[-CYCLE_MAX_BARS["W"]:]}

    return sym


# ══════════════════════════════════════════════════════════════
#  数据获取 (东方财富)
# ══════════════════════════════════════════════════════════════

def _em_secid(code: str) -> str:
    """将股票代码转为东方财富 secid 格式 (市场.代码)
    支持输入: sh.600519 / sz.000001 / 600519 / 000001 等"""
    code = code.replace("sh.", "").replace("sz.", "").replace("bj.", "")
    if code.startswith(("6", "9")):
        return f"1.{code}"  # 上海
    elif code.startswith(("0", "3")):
        return f"0.{code}"  # 深圳
    elif code.startswith(("4", "8")):
        return f"0.{code}"  # 北交所/新三板
    return f"1.{code}"


async def fetch_json(session: aiohttp.ClientSession, url: str) -> any:
    """带重试的 HTTP GET"""
    domain = url.split("/")[2] if "/" in url else url[:40]
    for attempt in range(3):
        try:
            await asyncio.sleep(random.uniform(0.1, 0.5))
            async with session.get(url, proxy=proxy_url,
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                log.warning("[%s] HTTP %d (attempt %d/3)", domain, resp.status, attempt + 1)
        except Exception as e:
            log.warning("[%s] 请求失败 (%d/3): %s", domain, attempt + 1, str(e)[:60])
        await asyncio.sleep(2 * (attempt + 1))
    return None


async def fetch_all_stocks(session: aiohttp.ClientSession) -> list[dict]:
    """获取全部A股股票列表 (东方财富 datacenter-web 接口, 分页获取)"""
    stocks = []
    page = 1
    while True:
        url = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
               "?reportName=RPT_DMSK_TS_STOCKNEW"
               "&columns=SECURITY_CODE,SECURITY_NAME_ABBR"
               f"&pageSize=500&pageNumber={page}"
               "&sortColumns=SECURITY_CODE&sortTypes=1")
        data = await fetch_json(session, url)
        if not data or not data.get("success") or data.get("result") is None:
            if page == 1:
                log.error("获取股票列表失败")
                return []
            break
        rows = data["result"].get("data", [])
        if not rows:
            break
        for item in rows:
            code = str(item.get("SECURITY_CODE", ""))
            name = str(item.get("SECURITY_NAME_ABBR", ""))
            if not code or "ST" in name or "退" in name:
                continue
            stocks.append({"code": code, "name": name})
        total_pages = data["result"].get("pages", 0)
        if page >= total_pages:
            break
        page += 1
    log.info("获取到 %d 只A股", len(stocks))
    return stocks


# ══════════════════════════════════════════════════════════════
#  通达信 (pytdx) — 分钟线主源, 券商级接口, 几乎不封 IP
# ══════════════════════════════════════════════════════════════

TDX_SERVERS = [
    ("180.153.18.170", 7709),
    ("60.191.117.167", 7709),
    ("115.238.56.198", 7709),
    ("218.75.126.9", 7709),
    ("106.120.74.166", 7709),
    ("112.95.140.74", 7709),
    ("113.105.142.136", 7709),
    ("119.147.212.81", 7709),
    ("221.231.141.60", 7709),
    ("101.227.73.20", 7709),
    ("101.227.77.254", 7709),
    ("114.80.63.12", 7709),
]

# 每个通达信服务器开几个并行连接 (连接数 × 服务器数 = 总并发)
TDX_CONNS_PER_SERVER = 3

# pytdx category 映射: 0=5m, 1=15m, 2=30m, 3=60m, 4=日线, 11=周线
_TDX_CATEGORY = {"15": 1, "60": 3, "D": 4, "W": 11}


def _tdx_market(code: str) -> int:
    """股票代码 -> 通达信市场: 1=上海, 0=深圳"""
    return 1 if code.startswith(("6", "9")) else 0


def _fetch_klines_tdx_batch(server: tuple, tasks: list[tuple],
                            cycle: str, limit: int) -> dict[str, list[list]]:
    """同步: 单个通达信连接批量拉取一组股票的 K 线.
    tasks: [(code, market), ...]
    返回: {code: [[ts, o, h, l, c, vol, amt], ...]}
    """
    import socket as _socket
    from pytdx.hq import TdxHq_API
    category = _TDX_CATEGORY.get(cycle)
    if category is None:
        return {}
    api = TdxHq_API(auto_retry=False)
    result = {}
    host, port = server
    total = len(tasks)
    try:
        conn = api.connect(host, port, time_out=10)
        if not conn:
            log.warning("通达信 %s:%d 连接返回 False, 跳过", host, port)
            return result
        with conn:
            for i, (code, market) in enumerate(tasks):
                try:
                    data = api.get_security_bars(category, market, code, 0, limit)
                    if not data:
                        continue
                    bars = []
                    for bar in data:
                        bars.append([
                            bar["datetime"],
                            float(bar["open"]),
                            float(bar["high"]),
                            float(bar["low"]),
                            float(bar["close"]),
                            float(bar["vol"]),
                            float(bar["amount"]),
                        ])
                    if bars:
                        result[code] = bars
                except Exception:
                    continue
                if (i + 1) % 100 == 0 or i + 1 == total:
                    log.info("  通达信 %s: %d/%d (%.0f%%)",
                             host, i + 1, total, (i + 1) / total * 100)
    except Exception as e:
        log.warning("通达信 %s:%d 连接失败: %s", host, port, str(e)[:60])
    finally:
        # 强制关闭底层 socket, 防止 disconnect 卡住
        try:
            if hasattr(api, 'client') and api.client:
                sock = getattr(api.client, 'client', None)
                if sock and isinstance(sock, _socket.socket):
                    sock.settimeout(0)
                    try:
                        sock.shutdown(_socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()
            api.disconnect()
        except Exception:
            pass
    return result


def _probe_tdx_server(server: tuple) -> bool:
    """快速探测通达信服务器是否可连接."""
    from pytdx.hq import TdxHq_API
    import socket as _socket
    api = TdxHq_API(auto_retry=False)
    host, port = server
    try:
        conn = api.connect(host, port, time_out=5)
        if not conn:
            return False
        # 连接成功, 立即断开
        try:
            if hasattr(api, 'client') and api.client:
                sock = getattr(api.client, 'client', None)
                if sock and isinstance(sock, _socket.socket):
                    sock.settimeout(0)
                    try:
                        sock.shutdown(_socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()
            api.disconnect()
        except Exception:
            pass
        return True
    except Exception:
        return False


async def fetch_klines_tdx(stocks: list[dict], cycle: str,
                           limit: int = 160,
                           timeout: int = 0) -> dict[str, list[list]]:
    """用通达信多服务器并行拉取 K 线 (线程池).
    每个服务器开 TDX_CONNS_PER_SERVER 个连接并行拉取.
    先探测存活服务器, 只分配任务给可用的; 超时后放弃未完成的 worker.
    timeout=0 表示自动根据任务量计算超时."""
    conns = TDX_CONNS_PER_SERVER

    # ── 第一步: 并行探测服务器存活性 ──
    loop = asyncio.get_event_loop()
    probe_pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(TDX_SERVERS))
    try:
        probe_futures = {
            loop.run_in_executor(probe_pool, _probe_tdx_server, s): s
            for s in TDX_SERVERS
        }
        probe_done, _ = await asyncio.wait(probe_futures.keys(), timeout=15)
        servers = []
        for f in probe_done:
            s = probe_futures[f]
            try:
                if f.result():
                    servers.append(s)
                else:
                    log.warning("通达信 %s:%d 探测失败, 跳过", s[0], s[1])
            except Exception:
                log.warning("通达信 %s:%d 探测异常, 跳过", s[0], s[1])
        # 探测超时的也跳过
        for f in probe_futures:
            if f not in probe_done:
                s = probe_futures[f]
                log.warning("通达信 %s:%d 探测超时, 跳过", s[0], s[1])
    finally:
        probe_pool.shutdown(wait=False)

    if not servers:
        log.warning("所有通达信服务器不可用")
        return {}

    log.info("通达信存活服务器: %d/%d — %s",
             len(servers), len(TDX_SERVERS),
             ", ".join(s[0] for s in servers))

    # ── 第二步: 只分配任务给存活服务器 ──
    n_workers = len(servers) * conns
    all_tasks = [(s["code"], _tdx_market(s["code"])) for s in stocks]
    chunks = [all_tasks[i::n_workers] for i in range(n_workers)]
    worker_servers = [servers[i // conns] for i in range(n_workers)]

    # 动态超时: 根据最大分片大小, 每只股票约 0.5s, 给 2 倍余量, 最低 120s
    max_chunk = max(len(c) for c in chunks) if chunks else 0
    if timeout <= 0:
        timeout = max(120, max_chunk * 1 + 30)

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=n_workers)
    try:
        futures = [
            loop.run_in_executor(pool, _fetch_klines_tdx_batch,
                                 worker_servers[i], chunks[i], cycle, limit)
            for i in range(n_workers)
        ]
        done, pending = await asyncio.wait(futures, timeout=timeout)
        if pending:
            for i, f in enumerate(futures):
                if f in pending:
                    s = worker_servers[i]
                    log.warning("通达信 %s:%d 超时 (%ds), 跳过", s[0], s[1], timeout)
            for p in pending:
                p.cancel()
    finally:
        pool.shutdown(wait=False)

    merged: dict[str, list[list]] = {}
    for f in done:
        try:
            r = f.result()
        except Exception:
            continue
        if isinstance(r, dict):
            merged.update(r)
    return merged


def _tx_code(code: str) -> str:
    """股票代码转腾讯/新浪格式: 600519 -> sh600519, 000001 -> sz000001"""
    if code.startswith(("6", "9")):
        return f"sh{code}"
    return f"sz{code}"


# 腾讯 K 线周期映射 (仅支持日线/周线)
_TX_KLT = {"D": "day", "W": "week"}


async def _fetch_klines_sina(session: aiohttp.ClientSession, code: str,
                             cycle: str, limit: int = 210) -> list[list]:
    """新浪财经 K 线 (支持分钟线: 15/60, 也支持日线/周线)"""
    sina_code = _tx_code(code)
    scale_map = {"15": 15, "60": 60, "D": 240, "W": 1680}
    scale = scale_map.get(cycle)
    if scale is None:
        return []
    # 使用 json_v2 接口, 直接返回 JSON (无需解析 JSONP)
    url = (f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData"
           f"?symbol={sina_code}&scale={scale}&datalen={limit}&ma=")
    for attempt in range(3):
        try:
            await asyncio.sleep(random.uniform(0.01, 0.1))
            async with session.get(url, proxy=proxy_url,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    continue
                items = await resp.json(content_type=None)
                if not isinstance(items, list):
                    continue
                result = []
                for item in items:
                    try:
                        result.append([
                            item["day"],
                            float(item["open"]),
                            float(item["high"]),
                            float(item["low"]),
                            float(item["close"]),
                            float(item["volume"]),
                            float(item.get("amount", 0)),
                        ])
                    except (ValueError, KeyError):
                        continue
                return result
        except Exception as e:
            if attempt == 2:
                log.warning("新浪请求失败 (%d/3) %s: %s", attempt + 1, sina_code, str(e)[:60])
        await asyncio.sleep(0.5 * (attempt + 1))
    return []


async def _fetch_klines_tx(session: aiohttp.ClientSession, code: str,
                           cycle: str, limit: int = 210) -> list[list]:
    """腾讯财经 K 线 (仅日线/周线)"""
    tx_code = _tx_code(code)
    klt = _TX_KLT.get(cycle)
    if not klt:
        return []
    url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
           f"?param={tx_code},{klt},,,{limit},qfq")
    data = await fetch_json(session, url)
    if not data or data.get("code") != 0 or not isinstance(data.get("data"), dict):
        return []
    stock_data = data["data"].get(tx_code, {})
    klines_raw = (stock_data.get(f"qfq{klt}") or stock_data.get(klt) or [])
    result = []
    for bar in klines_raw:
        # 腾讯格式: [date, open, close, high, low, volume]
        if not isinstance(bar, list) or len(bar) < 6:
            continue
        try:
            close = float(bar[2])
            result.append([
                bar[0],
                float(bar[1]),      # open
                float(bar[3]),      # high
                float(bar[4]),      # low
                close,              # close
                float(bar[5]),      # volume
                float(bar[5]) * close,  # 近似 amount
            ])
        except (ValueError, IndexError):
            continue
    return result


async def _fetch_klines_em(session: aiohttp.ClientSession, code: str,
                           cycle: str, limit: int = 210) -> list[list]:
    """东方财富 K 线 (最后备用, 连续失败自动禁用)"""
    global _em_kline_fail_count, _em_kline_disabled
    if _em_kline_disabled:
        return []
    secid = _em_secid(code)
    klt = EM_KLT[cycle]
    url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
           f"?secid={secid}&klt={klt}&fqt={EM_FQTYPE}"
           f"&lmt={limit}&end=20500101&fields1=f1,f2,f3"
           f"&fields2=f51,f52,f53,f54,f55,f56,f57")
    data = await fetch_json(session, url)
    if not data or data.get("data") is None:
        _em_kline_fail_count += 1
        if _em_kline_fail_count >= _EM_FAIL_THRESHOLD and not _em_kline_disabled:
            _em_kline_disabled = True
            log.warning("东方财富 K 线连续失败 %d 次, 已禁用", _em_kline_fail_count)
        return []
    _em_kline_fail_count = 0  # 成功则重置
    klines_raw = data["data"].get("klines", [])
    result = []
    for line in klines_raw:
        parts = line.split(",")
        if len(parts) < 7:
            continue
        try:
            result.append([
                parts[0],
                float(parts[1]),    # open
                float(parts[2]),    # high
                float(parts[3]),    # low
                float(parts[4]),    # close
                float(parts[5]),    # volume
                float(parts[6]),    # amount
            ])
        except (ValueError, IndexError):
            continue
    return result


async def fetch_klines(session: aiohttp.ClientSession, code: str, cycle: str,
                       limit: int = 210) -> list[list]:
    """获取K线 — 多源容错:
       分钟线 (15/60): 新浪(主) -> 东方财富(备)
       日线/周线:       腾讯(主) -> 新浪(备) -> 东方财富(备)
    """
    if cycle in ("15", "60"):
        result = await _fetch_klines_sina(session, code, cycle, limit)
        if result:
            return result
        return await _fetch_klines_em(session, code, cycle, limit)
    else:
        result = await _fetch_klines_tx(session, code, cycle, limit)
        if result:
            return result
        result = await _fetch_klines_sina(session, code, cycle, limit)
        if result:
            return result
        return await _fetch_klines_em(session, code, cycle, limit)


def _is_post_close() -> bool:
    """判断当前北京时间是否在收盘后 (15:00 之后, 当天内)"""
    bj = datetime.now(timezone(timedelta(hours=8)))
    return bj.weekday() < 5 and bj.hour >= 15


async def fetch_all_data(session: aiohttp.ClientSession, stocks: list[dict],
                         cache: dict, max_concurrent: int = 15) -> dict:
    """异步并发批量获取K线.

    增量模式 (缓存中已有 D/W 数据):
        只拉 15m K线, 然后合成 60m / D / W — 请求量减少 75%.
    冷启动 (无缓存):
        全量拉 4 个周期.
    收盘校准 (增量模式 + 15:00 后):
        先走增量, 再全量拉 60m/D/W 覆盖合成数据, 确保准确.
    """
    all_sym: dict = {}
    cached_codes_set = set(cache.keys())
    sem = asyncio.Semaphore(max_concurrent)

    # 判断是否可以走增量模式: 缓存中有足够的历史数据
    # 条件: 缓存中至少有 100 只股票且都有 D 数据
    sample = list(cached_codes_set)[:200]
    has_history = (len(sample) >= 100
                   and sum(1 for c in sample
                           if "D" in cache.get(c, {}) and len(cache[c]["D"].get("data", [])) >= 26)
                   > len(sample) * 0.8)
    incremental = has_history
    post_close = _is_post_close()

    if not incremental:
        log.info("冷启动模式: 全量拉取 4 个周期")
    elif post_close:
        log.info("收盘校准模式: 先增量合成, 再全量拉取 60m/D/W 校准")
    else:
        log.info("增量模式: 仅拉取 15m K线, 合成其他周期")

    # 恢复缓存
    for s in stocks:
        code = s["code"]
        if code in cached_codes_set:
            all_sym[code] = {"name": s["name"]}
            for cycle in CYCLES:
                if cycle in cache[code] and cache[code][cycle].get("data"):
                    all_sym[code][cycle] = {"data": cache[code][cycle]["data"]}

    async def _limited(code: str, name: str, cycle: str):
        async with sem:
            limit = CYCLE_MAX_BARS.get(cycle, 210)
            data = await fetch_klines(session, code, cycle, limit)
            return code, name, cycle, data

    def _apply_results(results, all_sym, fresh_15m=None):
        ok = 0
        for r in results:
            if isinstance(r, Exception):
                continue
            if not isinstance(r, tuple) or len(r) != 4:
                continue
            code, name, cycle, data = r
            if not data:
                continue
            ok += 1
            if code not in all_sym:
                all_sym[code] = {"name": name}
            if fresh_15m is not None and cycle == "15":
                fresh_15m[code] = data
            elif cycle in all_sym[code] and all_sym[code][cycle].get("data"):
                all_sym[code][cycle]["data"] = merge_klines(
                    all_sym[code][cycle]["data"], data, cycle)
            else:
                all_sym[code][cycle] = {"data": data}
        return ok

    t0 = time.time()

    if incremental:
        # ── 第一阶段: 通达信批量拉 15m (主源, 多服务器并行) ──
        cached_stocks = [s for s in stocks if s["code"] in cached_codes_set]
        log.info("通达信拉取 15m K线: %d 只股票 (配置 %d 服务器 × %d 连接)",
                 len(cached_stocks), len(TDX_SERVERS), TDX_CONNS_PER_SERVER)
        tdx_result = await fetch_klines_tdx(cached_stocks, "15",
                                            limit=CYCLE_MAX_BARS.get("15", 160))
        fresh_15m: dict[str, list[list]] = {}
        ok_count = 0
        name_map = {s["code"]: s["name"] for s in cached_stocks}
        for code, bars in tdx_result.items():
            if not bars:
                continue
            ok_count += 1
            if code not in all_sym:
                all_sym[code] = {"name": name_map.get(code, "")}
            fresh_15m[code] = bars
            all_sym[code]["15"] = {"data": bars}
        log.info("通达信完成: %d/%d 只成功", ok_count, len(cached_stocks))

        # 通达信拉不到的, 用新浪补 (少量, 不会触发封 IP)
        missing = [s for s in cached_stocks if s["code"] not in tdx_result]
        if missing:
            log.info("新浪补拉 15m: %d 只", len(missing))
            tasks_fallback = [_limited(s["code"], s["name"], "15") for s in missing]
            results_fb = await asyncio.gather(*tasks_fallback, return_exceptions=True)
            fb_ok = _apply_results(results_fb, all_sym, fresh_15m)
            ok_count += fb_ok
            log.info("新浪补拉完成: %d 只成功", fb_ok)

        # 合成 60m/D/W
        agg_count = 0
        for code, bars_15m in fresh_15m.items():
            cached_sym = all_sym.get(code, {})
            updated = aggregate_from_15m(cached_sym, bars_15m)
            name = all_sym.get(code, {}).get("name", "")
            all_sym[code] = {"name": name}
            for cycle in CYCLES:
                if cycle in updated and updated[cycle].get("data"):
                    all_sym[code][cycle] = updated[cycle]
            agg_count += 1
        log.info("增量合成完成: %d 只股票从 15m 合成 60m/D/W", agg_count)

        # ── 新股票: 缓存中没有的, 全量拉 4 个周期 ──
        new_stocks = [s for s in stocks if s["code"] not in cached_codes_set]
        if new_stocks:
            log.info("新股票全量拉取: %d 只", len(new_stocks))
            name_map_new = {s["code"]: s["name"] for s in new_stocks}
            for cycle in CYCLES:
                log.info("  新股票拉取 %s ...", cycle)
                new_result = await fetch_klines_tdx(
                    new_stocks, cycle, limit=CYCLE_MAX_BARS.get(cycle, 210))
                new_ok = 0
                for code, bars in new_result.items():
                    if not bars:
                        continue
                    new_ok += 1
                    if code not in all_sym:
                        all_sym[code] = {"name": name_map_new.get(code, "")}
                    all_sym[code][cycle] = {"data": bars}
                ok_count += new_ok
            log.info("新股票完成: %d 只有数据", sum(1 for s in new_stocks if s["code"] in all_sym))

        # ── 腾讯补拉: 通达信周线/日线不够的, 用腾讯补 ──
        short_stocks = [{"code": code} for code, sym in all_sym.items()
                        if len(sym.get("W", {}).get("data", [])) < 20
                        or len(sym.get("D", {}).get("data", [])) < 26]
        if short_stocks:
            for sup_cycle in ("W", "D"):
                need = [s for s in short_stocks
                        if len(all_sym.get(s["code"], {}).get(sup_cycle, {}).get("data", [])) <
                        (20 if sup_cycle == "W" else 26)]
                if not need:
                    continue
                log.info("腾讯补拉 %s: %d 只 (通达信数据不足)", sup_cycle, len(need))
                tasks_sup = [_limited(s["code"],
                                      all_sym.get(s["code"], {}).get("name", ""),
                                      sup_cycle) for s in need]
                results_sup = await asyncio.gather(*tasks_sup, return_exceptions=True)
                sup_ok = _apply_results(results_sup, all_sym)
                ok_count += sup_ok
                log.info("腾讯补拉 %s 完成: %d 只成功", sup_cycle, sup_ok)

        # ── 第二阶段: 收盘后全量校准 60m/D/W ──
        if post_close:
            calibrate_cycles = ["60", "D", "W"]
            for cal_cycle in calibrate_cycles:
                log.info("收盘校准: 通达信拉取 %s", cal_cycle)
                cal_result = await fetch_klines_tdx(
                    cached_stocks, cal_cycle,
                    limit=CYCLE_MAX_BARS.get(cal_cycle, 210))
                cal_ok = 0
                for code, bars in cal_result.items():
                    if not bars:
                        continue
                    cal_ok += 1
                    if code not in all_sym:
                        continue
                    if cal_cycle in all_sym[code] and all_sym[code][cal_cycle].get("data"):
                        all_sym[code][cal_cycle]["data"] = merge_klines(
                            all_sym[code][cal_cycle]["data"], bars, cal_cycle)
                    else:
                        all_sym[code][cal_cycle] = {"data": bars}
                ok_count += cal_ok
                log.info("收盘校准 %s 完成: %d 只成功", cal_cycle, cal_ok)
    else:
        # ── 冷启动: 通达信全量拉 4 个周期 ──
        for cycle in CYCLES:
            log.info("冷启动: 通达信拉取 %s", cycle)
            cycle_result = await fetch_klines_tdx(
                stocks, cycle, limit=CYCLE_MAX_BARS.get(cycle, 210))
            cycle_ok = 0
            for code, bars in cycle_result.items():
                if not bars:
                    continue
                cycle_ok += 1
                name_map = {s["code"]: s["name"] for s in stocks}
                if code not in all_sym:
                    all_sym[code] = {"name": name_map.get(code, "")}
                all_sym[code][cycle] = {"data": bars}
            ok_count += cycle_ok
            log.info("冷启动 %s 完成: %d 只成功", cycle, cycle_ok)

    elapsed = round(time.time() - t0, 1)
    log.info("K线完成: %d 只股票有数据, %d 个请求成功, 耗时 %ss", len(all_sym), ok_count, elapsed)
    return all_sym


def calc_market_turnover(all_sym: dict) -> dict:
    """从全市场日线数据计算每日总成交额, 返回市场热度判断.

    策略:
    - 汇总每个交易日所有股票的成交额 (bar[6])
    - 用近 60 个交易日的成交额分布来判断当前热度
    - 分位数判断: >75% 火热, <25% 冷清, 中间正常
    - 额外输出: 当日总成交额 (亿), 近期均值, 分位数

    返回: {"level": "hot"/"normal"/"cold"/"unknown",
           "today_amount": 亿, "avg_amount": 亿, "percentile": 0~100}
    """
    # 汇总每个交易日的全市场成交额
    daily_totals: dict[str, float] = {}  # date -> 总成交额
    for code, sym in all_sym.items():
        d_data = sym.get("D", {}).get("data", [])
        for bar in d_data:
            date = bar[0][:10]
            daily_totals[date] = daily_totals.get(date, 0) + float(bar[6])

    if len(daily_totals) < 10:
        return {"level": "unknown", "today_amount": 0, "avg_amount": 0, "percentile": 50}

    # 按日期排序, 取最近 60 个交易日
    sorted_dates = sorted(daily_totals.keys())
    recent_dates = sorted_dates[-60:]
    recent_amounts = [daily_totals[d] for d in recent_dates]

    today_amount = recent_amounts[-1]
    avg_amount = sum(recent_amounts) / len(recent_amounts)

    # 计算当日成交额在近期的分位数
    rank = sum(1 for a in recent_amounts if a <= today_amount)
    percentile = round(rank / len(recent_amounts) * 100)

    # 判断热度
    if percentile >= 75:
        level = "hot"
    elif percentile <= 25:
        level = "cold"
    else:
        level = "normal"

    yi = 1e8  # 亿
    log.info("市场成交额: 今日 %.0f亿, 近%d日均值 %.0f亿, 分位 %d%% → %s",
             today_amount / yi, len(recent_amounts), avg_amount / yi, percentile, level)

    return {
        "level": level,
        "today_amount": round(today_amount / yi),
        "avg_amount": round(avg_amount / yi),
        "percentile": percentile,
    }


async def get_index_direction(session: aiohttp.ClientSession) -> str:
    """上证指数方向 (东方财富接口) — 保留作为辅助参考"""
    try:
        data = await fetch_klines(session, "000001", "D", 30)
        if not data:
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
                   f"?secid=1.000001&klt=101&fqt=1&lmt=30&end=20500101"
                   f"&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55,f56,f57")
            raw = await fetch_json(session, url)
            if raw and raw.get("data") and raw["data"].get("klines"):
                data = []
                for line in raw["data"]["klines"]:
                    parts = line.split(",")
                    if len(parts) >= 7:
                        data.append([parts[0], float(parts[1]), float(parts[2]),
                                     float(parts[3]), float(parts[4]),
                                     float(parts[5]), float(parts[6])])
        if data and len(data) >= 7:
            close = data[-1][4]
            open_p = data[-1][1]
            close_7d = data[-7][4]
            if close > open_p * 1.005 and close > close_7d:
                return "up"
            if close < open_p * 0.995 and close < close_7d:
                return "down"
            return "neutral"
    except Exception as e:
        log.warning("获取上证指数失败: %s", e)
    return "unknown"


# ══════════════════════════════════════════════════════════════
#  技术指标计算
# ══════════════════════════════════════════════════════════════

def ema(data: list[float], span: int) -> list[float]:
    k = 2 / (span + 1)
    result = [data[0]]
    for i in range(1, len(data)):
        result.append(data[i] * k + result[-1] * (1 - k))
    return result


def sma(data: list[float], window: int) -> list[float]:
    result = []
    for i in range(len(data)):
        if i < window - 1:
            result.append(float("nan"))
        else:
            result.append(sum(data[i - window + 1: i + 1]) / window)
    return result


def calc_bollinger(closes, window=20, num_std=2):
    mid = sma(closes, window)
    upper, lower = [], []
    for i in range(len(closes)):
        if math.isnan(mid[i]):
            upper.append(float("nan")); lower.append(float("nan")); continue
        sum_sq = sum((closes[j] - mid[i]) ** 2 for j in range(i - window + 1, i + 1))
        std = math.sqrt(sum_sq / window)
        upper.append(mid[i] + std * num_std); lower.append(mid[i] - std * num_std)
    return {"mid": mid, "upper": upper, "lower": lower}


def calc_macd(closes, short_span=12, long_span=26, signal_span=9):
    ema_s = ema(closes, short_span); ema_l = ema(closes, long_span)
    macd_line = [s - l for s, l in zip(ema_s, ema_l)]
    return {"macdLine": macd_line, "signalLine": ema(macd_line, signal_span)}


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return [float("nan")] * len(closes)
    result = [float("nan")] * period
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d > 0: gains += d
        else: losses -= d
    ag, al = gains / period, losses / period
    rs = ag / al if al else float("inf")
    result.append(100 - 100 / (1 + rs))
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        g = d if d > 0 else 0; l = -d if d < 0 else 0
        ag = (ag * (period - 1) + g) / period
        al = (al * (period - 1) + l) / period
        rs = ag / al if al else float("inf")
        result.append(100 - 100 / (1 + rs))
    return result


def calc_volume_osc(data, short=5, long=20):
    vols = [float(x[6]) for x in data]
    s_ma, l_ma = sma(vols, short), sma(vols, long)
    return [(s - l) / l * 100 if not (math.isnan(s) or math.isnan(l) or l == 0) else float("nan")
            for s, l in zip(s_ma, l_ma)]


def compute_indicators(all_sym: dict):
    for code in list(all_sym.keys()):
        for cycle in list(all_sym[code].keys()):
            if cycle == "name": continue
            try:
                data = all_sym[code][cycle]["data"]
                closes = [float(x[4]) for x in data]
                if len(closes) < 26: continue
                all_sym[code][cycle]["bolling"] = calc_bollinger(closes)
                all_sym[code][cycle]["macd"] = calc_macd(closes)
                if cycle == "60" and len(closes) >= 200:
                    for w in (30, 60, 120, 160, 200):
                        all_sym[code][cycle][f"ma{w}"] = sma(closes, w)
                    all_sym[code][cycle]["rsi"] = calc_rsi(closes)
                    all_sym[code][cycle]["volume_osc"] = calc_volume_osc(data)
            except (KeyError, IndexError, ValueError): pass


# ══════════════════════════════════════════════════════════════
#  策略
# ══════════════════════════════════════════════════════════════

def is_15m_trend_up(sym):
    b, m = sym["15"]["bolling"], sym["15"]["macd"]
    return b["mid"][-1] > b["mid"][-2] and m["macdLine"][-1] > 0

def is_60m_trend_up(sym):
    b, m = sym["60"]["bolling"], sym["60"]["macd"]
    if b["mid"][-1] <= b["mid"][-2] * 0.999: return False
    return (m["macdLine"][-1] >= m["signalLine"][-1]
            and m["macdLine"][-1] >= m["macdLine"][-2]
            and m["signalLine"][-1] >= m["signalLine"][-2])

def is_daily_trend_up(sym):
    b, m = sym["D"]["bolling"], sym["D"]["macd"]
    if b["mid"][-1] <= b["mid"][-2] * 0.999: return False
    return m["macdLine"][-1] >= m["macdLine"][-2]

def is_weekly_trend_up(sym):
    b = sym["W"]["bolling"]
    close = float(sym["W"]["data"][-1][4])
    return (b["mid"][-1] > b["mid"][-2] > b["mid"][-3] > b["mid"][-4]
            and b["upper"][-1] > b["upper"][-2] > b["upper"][-3] > b["upper"][-4]
            and close > b["mid"][-1])


# ══════════════════════════════════════════════════════════════
#  加分项检测
# ══════════════════════════════════════════════════════════════

def _sum_vol(data, n, j, frm, to):
    return sum(float(data[n + i + j][6]) for i in range(frm, to) if 0 <= n + i + j < len(data))

def _is_15m_step_up(sym, j):
    mid = sym["15"]["bolling"]["mid"]
    for i in range(-2, 0):
        dc = mid[len(mid)+i+j] - mid[len(mid)+i-1+j]
        dp = mid[len(mid)+i-1+j] - mid[len(mid)+i-2+j]
        if dc < dp * 0.9999 or dp * 0.9999 < 0: return False
    return True

def _is_15m_anomaly(all_sym, code, j):
    sym = all_sym[code]; data = sym["15"]["data"]; n = len(data)
    u = sym["15"]["bolling"]["upper"][n-3+j]; l = sym["15"]["bolling"]["lower"][n-3+j]
    if u > l * 1.1 and not _is_15m_step_up(sym, j): return False
    bar = data[n-1+j]; vol = float(bar[6])
    if vol < _sum_vol(data, n, j, -10, -1) or vol < 50_000_000: return False
    move = abs(float(bar[4]) - float(bar[1])) / float(bar[1]) if float(bar[1]) else 0
    return 0.008 <= move <= 0.1

def _is_60m_anomaly(all_sym, code, j):
    sym = all_sym[code]; data = sym["60"]["data"]; n = len(data)
    bar = data[n-1+j]; vol = float(bar[6])
    if vol < _sum_vol(data, n, j, -7, -1) or vol < 200_000_000: return False
    move = abs(float(bar[4]) - float(bar[1])) / float(bar[1]) if float(bar[1]) else 0
    return 0.02 <= move <= 0.1

def _is_daily_anomaly(all_sym, code, j):
    sym = all_sym[code]; data = sym["D"]["data"]; n = len(data)
    bar = data[n-1+j]; vol = float(bar[6])
    if vol < _sum_vol(data, n, j, -6, -1) or vol < 500_000_000: return False
    move = abs(float(bar[4]) - float(bar[1])) / float(bar[1]) if float(bar[1]) else 0
    return 0.03 <= move <= 0.1

def _has_recent_anomaly(all_sym, code):
    try:
        for i in range(-3, 0):
            if _is_15m_anomaly(all_sym, code, i): return True
            if _is_60m_anomaly(all_sym, code, i): return True
        for i in range(-5, 0):
            if _is_daily_anomaly(all_sym, code, i): return True
    except (IndexError, KeyError, ValueError): pass
    return False

def detect_volume_anomaly(all_sym, code):
    try:
        if _has_recent_anomaly(all_sym, code): return ""
        if _is_15m_anomaly(all_sym, code, 0): return "15m"
        if _is_60m_anomaly(all_sym, code, 0): return "60m"
        if _is_daily_anomaly(all_sym, code, 0): return "日线"
    except (IndexError, KeyError, ValueError): pass
    return ""

def _min_price_7d(sym):
    data = sym["D"]["data"]; days = min(7, len(data))
    return min(float(data[-i][3]) for i in range(1, days + 1))

def check_anti_chase(sym):
    try:
        close = float(sym["D"]["data"][-1][4]); b = sym["D"]["bolling"]
        return (close < _min_price_7d(sym) * 1.5 and b["upper"][-1] < b["lower"][-1] * 1.5
                and close < b["upper"][-1] * 1.05)
    except (IndexError, KeyError, ValueError): return False

def find_leading_stocks(all_sym):
    result = set()
    for code in all_sym:
        data = all_sym[code].get("D", {}).get("data")
        if not data or len(data) < 20: continue
        for i in range(-5, -1):
            try:
                if float(data[len(data)-1+i][4]) > float(data[len(data)-5+i][4]) * 1.2:
                    result.add(code); break
            except (IndexError, ValueError): continue
    return result

def find_fairy_guide(all_sym, candidates):
    result = set()
    for code in candidates:
        if code not in all_sym or "D" not in all_sym[code]: continue
        data = all_sym[code]["D"]["data"]
        if len(data) < 20: continue
        for i in range(len(data) - 10, len(data)):
            if i < 10: continue
            try:
                vol_sum = sum(float(data[j][6]) for j in range(i-10, i-1))
                o, h, c, v = float(data[i][1]), float(data[i][2]), float(data[i][4]), float(data[i][6])
                if v > vol_sum and o*1.05 < h < o*1.1 and h*0.97 > c > o:
                    result.add(code); break
            except (IndexError, ValueError): continue
    return result

def is_not_rubbish(sym):
    try:
        data = sym["D"]["data"]
        return any(float(data[i][2]) > float(data[i][3]) * 1.03 for i in range(-3, 0))
    except (IndexError, KeyError, ValueError): return False

def is_low_vol_good_move(sym):
    try:
        bar = sym["D"]["data"][-1]
        return float(bar[2]) > float(bar[1]) * 1.05 and float(bar[6]) < 50_000_000
    except (IndexError, KeyError, ValueError): return False

def detect_consolidation_breakout(sym, cycle="60"):
    try:
        c = sym.get(cycle)
        if not c: return False
        data, rsi, vol_osc = c.get("data",[]), c.get("rsi",[]), c.get("volume_osc",[])
        if len(data)<200 or len(rsi)<12 or len(vol_osc)<2: return False
        ma_keys = ["ma30","ma60","ma120","ma160","ma200"]
        for k in ma_keys:
            if k not in c or len(c[k])<12: return False
        close, open_, high = float(data[-1][4]), float(data[-1][1]), float(data[-1][2])
        zf = (close-open_)/open_ if open_>0 else 0
        vals = [c[k][-1] for k in ma_keys]
        valid = [v for v in vals if v is not None and v==v]
        if len(valid)<5: return False
        mx, mn = max(valid), min(valid)
        if not(0.01<zf<0.06) or close<=mx or (close-open_)<=(high-close): return False
        vo = vol_osc[-1]
        if math.isnan(vo): return False
        bull = all(c[k][-1] for k in ma_keys) and c["ma30"][-1]>c["ma60"][-1]>c["ma120"][-1]>c["ma200"][-1]
        if not(vo>40 or (vo>15 and bull)): return False
        if close<=max(float(data[i][4]) for i in range(-121,-1)): return False
        if not all(close>float(data[i][2]) for i in range(-4,-1)): return False
        if mn<=0 or (mx-mn)/mn>=0.028: return False
        cr = rsi[-1]
        if math.isnan(cr) or not(58<cr<80): return False
        for i in range(2,11):
            iv = [c[k][-i] for k in ma_keys]
            ivv = [v for v in iv if v is not None and v==v]
            if len(ivv)<5: return False
            imx, imn = max(ivv), min(ivv)
            if imn<=0 or (imx-imn)/imx>=0.035: return False
            if len(rsi)>=i and (math.isnan(rsi[-i]) or not(38<rsi[-i]<82)): return False
        return True
    except (KeyError,IndexError,ValueError,TypeError): return False


# ══════════════════════════════════════════════════════════════
#  潜伏信号 — 盘整蓄势 + 底部支撑 (突破前夜)
# ══════════════════════════════════════════════════════════════

def _boll_width_percentile(bolling, lookback=120):
    """当前布林带宽度在近 lookback 根K线中的分位数 (0~100).
    宽度 = (upper - lower) / mid. 分位数越低说明波动越收敛."""
    upper, lower, mid = bolling["upper"], bolling["lower"], bolling["mid"]
    n = len(mid)
    if n < lookback or n < 2:
        return 50
    widths = []
    for i in range(max(0, n - lookback), n):
        if math.isnan(mid[i]) or mid[i] <= 0:
            continue
        widths.append((upper[i] - lower[i]) / mid[i])
    if len(widths) < 20:
        return 50
    cur = widths[-1]
    rank = sum(1 for w in widths if w <= cur)
    return round(rank / len(widths) * 100)


def _boll_width_narrowing(bolling, window=10):
    """布林带宽度是否在持续收窄 (近 window 根的宽度斜率为负).
    返回 True 表示波动正在进一步压缩."""
    upper, lower, mid = bolling["upper"], bolling["lower"], bolling["mid"]
    n = len(mid)
    if n < window + 1:
        return False
    widths = []
    for i in range(n - window, n):
        if math.isnan(mid[i]) or mid[i] <= 0:
            return False
        widths.append((upper[i] - lower[i]) / mid[i])
    # 简单线性回归斜率
    x_mean = (window - 1) / 2
    y_mean = sum(widths) / len(widths)
    num = sum((i - x_mean) * (w - y_mean) for i, w in enumerate(widths))
    den = sum((i - x_mean) ** 2 for i in range(len(widths)))
    if den == 0:
        return False
    slope = num / den
    return slope < 0


def _intermittent_accumulation(data, lookback=20):
    """间歇性放量不跌: 近 lookback 根K线中, 是否出现过放量但不跌的K线.
    条件: 成交量 > 近期均量 2 倍, 但跌幅 < 1%.
    返回符合条件的K线数量."""
    n = len(data)
    if n < lookback + 20:
        return 0
    # 近 60 根的平均成交量作为基准
    base_start = max(0, n - 60)
    base_vols = [float(data[i][6]) for i in range(base_start, n - 1)]
    if not base_vols:
        return 0
    avg_vol = sum(base_vols) / len(base_vols)
    if avg_vol <= 0:
        return 0
    count = 0
    for i in range(n - lookback, n):
        vol = float(data[i][6])
        open_p = float(data[i][1])
        close = float(data[i][4])
        if open_p <= 0:
            continue
        drop = (open_p - close) / open_p
        if vol > avg_vol * 2 and drop < 0.01:
            count += 1
    return count


def _lower_shadow_density(data, lookback=20):
    """下影线密度: 近 lookback 根K线中, 下影线 > 实体的K线占比.
    下影线 = min(open, close) - low, 实体 = abs(close - open).
    占比高说明下方支撑强."""
    n = len(data)
    if n < lookback:
        return 0.0
    count = 0
    for i in range(n - lookback, n):
        open_p, high, low, close = (float(data[i][1]), float(data[i][2]),
                                     float(data[i][3]), float(data[i][4]))
        body = abs(close - open_p)
        lower_shadow = min(open_p, close) - low
        if lower_shadow > body and lower_shadow > 0:
            count += 1
    return count / lookback


def _relative_strength(sym, all_sym, lookback=20):
    """个股相对大盘的强度: 个股近 lookback 日涨幅 - 大盘近 lookback 日涨幅.
    正值说明比大盘强. 大盘用 000001 (上证指数) 近似."""
    try:
        d_data = sym["D"]["data"]
        if len(d_data) < lookback + 1:
            return 0.0
        stock_ret = (float(d_data[-1][4]) - float(d_data[-lookback - 1][4])) / float(d_data[-lookback - 1][4])
        # 尝试从 all_sym 中找上证指数
        idx = all_sym.get("000001")
        if not idx or "D" not in idx or len(idx["D"].get("data", [])) < lookback + 1:
            return stock_ret  # 没有大盘数据, 返回绝对涨幅
        idx_data = idx["D"]["data"]
        idx_ret = (float(idx_data[-1][4]) - float(idx_data[-lookback - 1][4])) / float(idx_data[-lookback - 1][4])
        return stock_ret - idx_ret
    except (IndexError, KeyError, ValueError, ZeroDivisionError):
        return 0.0


def detect_ambush_setup(sym, all_sym, code, cycle="D"):
    """潜伏信号检测: 盘整蓄势 + 底部支撑 (突破前夜).

    综合评分, 满足 4 项及以上视为潜伏信号:
    1. 布林带宽度处于近120根K线最低25%分位
    2. 布林带宽度持续收窄 (斜率为负)
    3. 均线粘合 (MA散度 < 3%, 需要60m有MA数据)
    4. 成交量萎缩 (近10根均量 < 近60根均量的65%)
    5. RSI在中性区 (38~62, 不过热也不过冷)
    6. 间歇性放量不跌 (有主力吸筹迹象)
    7. 下影线密度高 (下方支撑强)
    8. 相对大盘强势 (盘整期间跌得比大盘少)

    返回: (bool, list[str]) — 是否触发, 满足的子条件列表
    """
    try:
        c = sym.get(cycle)
        if not c or "bolling" not in c:
            return False, []
        data = c.get("data", [])
        bolling = c["bolling"]
        if len(data) < 60:
            return False, []

        score = 0
        details = []

        # 1. 布林带宽度低分位
        pct = _boll_width_percentile(bolling, 120)
        if pct <= 25:
            score += 1
            details.append("带宽收敛")

        # 2. 布林带持续收窄
        if _boll_width_narrowing(bolling, 10):
            score += 1
            details.append("持续收窄")

        # 3. 均线粘合 (用60m的MA数据, 如果有的话)
        c60 = sym.get("60", {})
        ma_keys = ["ma30", "ma60", "ma120"]
        if all(k in c60 and len(c60[k]) >= 1 for k in ma_keys):
            vals = [c60[k][-1] for k in ma_keys]
            valid = [v for v in vals if v is not None and v == v and v > 0]
            if len(valid) >= 3:
                mx, mn = max(valid), min(valid)
                if mn > 0 and (mx - mn) / mn < 0.03:
                    score += 1
                    details.append("均线粘合")

        # 4. 成交量萎缩
        n = len(data)
        if n >= 60:
            recent_vol = sum(float(data[i][6]) for i in range(n - 10, n)) / 10
            base_vol = sum(float(data[i][6]) for i in range(n - 60, n - 10)) / 50
            if base_vol > 0 and recent_vol < base_vol * 0.65:
                score += 1
                details.append("缩量蓄势")

        # 5. RSI中性区
        if "rsi" in c60 and len(c60.get("rsi", [])) >= 1:
            rsi_val = c60["rsi"][-1]
            if not math.isnan(rsi_val) and 38 <= rsi_val <= 62:
                score += 1
                details.append("RSI中性")
        elif "macd" in c:
            # 没有RSI时, 用MACD柱状图接近零轴作为替代
            macd = c["macd"]
            hist = macd["macdLine"][-1] - macd["signalLine"][-1]
            if abs(hist) < abs(macd["macdLine"][-1]) * 0.3:
                score += 1
                details.append("MACD收敛")

        # 6. 间歇性放量不跌
        accum = _intermittent_accumulation(data, 20)
        if accum >= 2:
            score += 1
            details.append(f"吸筹{accum}次")

        # 7. 下影线密度
        shadow = _lower_shadow_density(data, 20)
        if shadow >= 0.25:
            score += 1
            details.append("下方支撑")

        # 8. 相对强度
        rs = _relative_strength(sym, all_sym, 20)
        if rs > 0.01:
            score += 1
            details.append("强于大盘")

        # 需要至少 4 项满足
        return score >= 4, details

    except (KeyError, IndexError, ValueError, TypeError):
        return False, []


# ══════════════════════════════════════════════════════════════
#  数据校验 + 主流程
# ══════════════════════════════════════════════════════════════

def is_valid(sym):
    if "D" not in sym or not sym["D"].get("data") or len(sym["D"]["data"]) < 26: return False
    if "W" not in sym or not sym["W"].get("data") or len(sym["W"]["data"]) < 20: return False
    return True

def has_indicators(sym):
    for tf in ("W", "D"):
        if tf not in sym or "bolling" not in sym[tf] or "macd" not in sym[tf]: return False
    return True


# ══════════════════════════════════════════════════════════════
#  交易时间探测 — 用少量探针股票判断市场是否有新数据
# ══════════════════════════════════════════════════════════════

# 高流动性探针: 贵州茅台(沪), 平安银行(深), 中国平安(深)
_PROBE_CODES = ["sh.600519", "sz.000001", "sz.601318"]

async def is_market_active(session: aiohttp.ClientSession, cache: dict) -> bool:
    """拉取探针股票最新15m K线, 与缓存对比; 有任一更新即视为交易中."""
    for code in _PROBE_CODES:
        try:
            fresh = await fetch_klines(session, code, "15", limit=1)
            if not fresh:
                continue
            fresh_ts = fresh[-1][0]
            cached_bars = cache.get(code, {}).get("15", {}).get("data", [])
            if not cached_bars:
                # 无缓存, 视为首次运行, 继续扫描
                log.info("探针 %s 无缓存, 视为活跃", code)
                return True
            if fresh_ts != cached_bars[-1][0]:
                log.info("探针 %s 数据已更新 (%s -> %s)", code, cached_bars[-1][0], fresh_ts)
                return True
        except Exception as e:
            log.warning("探针 %s 检测失败: %s", code, e)
            continue
    return False


async def main():
    scan_start = time.time()
    bj_tz = timezone(timedelta(hours=8))
    scan_time = datetime.now(bj_tz).strftime("%Y-%m-%d %H:%M:%S")
    log.info("========== SCAN START: %s ==========", scan_time)

    cache = load_cache()

    # 如果 data 目录不存在或没有数据文件, 强制扫描 (首次部署必须生成数据)
    has_data = DATA_DIR.exists() and any(DATA_DIR.glob("*.json"))

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/",
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        if not has_data:
            log.info("data 目录无数据, 强制执行首次扫描")

        # 1. 股票列表
        stocks = await fetch_all_stocks(session)
        if not stocks:
            log.error("无法获取股票列表")
            sys.exit(1)

        # 2. K线 (带缓存, 异步并发)
        all_sym = await fetch_all_data(session, stocks, cache, max_concurrent=40)

        # 3. 大盘方向 (上证指数, 辅助参考)
        index_direction = await get_index_direction(session)

    # 4. 保存缓存
    save_cache(all_sym)

    # 5. 计算指标
    log.info("计算技术指标...")
    compute_indicators(all_sym)
    log.info("上证指数方向: %s", index_direction)

    # 5.5 市场成交额热度
    market_turnover = calc_market_turnover(all_sym)

    # 6. 扫描
    log.info("全市场扫描...")
    result_tokens = []
    valid_count = 0
    leading = find_leading_stocks(all_sym)

    for code in all_sym:
        sym = all_sym[code]
        if not is_valid(sym) or not has_indicators(sym): continue
        valid_count += 1
        tags = []

        try:
            d_up = is_daily_trend_up(sym); w_up = is_weekly_trend_up(sym)
            m_up = True
            if "60" in sym and "bolling" in sym.get("60",{}) and "macd" in sym.get("60",{}):
                m_up = m_up and is_60m_trend_up(sym)
            if "15" in sym and "bolling" in sym.get("15",{}) and "macd" in sym.get("15",{}):
                m_up = m_up and is_15m_trend_up(sym)
            if d_up and w_up and m_up: tags.append("趋势共振")
        except (IndexError, KeyError, ValueError): pass

        a = detect_volume_anomaly(all_sym, code)
        if a: tags.append(f"成交量异动({a})")
        if market_turnover["level"] == "hot": tags.append("大盘火热")
        elif market_turnover["level"] == "cold": tags.append("大盘冷清")
        if check_anti_chase(sym): tags.append("未追高")
        if is_not_rubbish(sym): tags.append("波动充足")
        if code in leading: tags.append("龙头股")
        if is_low_vol_good_move(sym): tags.append("小量大涨")
        if detect_consolidation_breakout(sym, "60"): tags.append("盘整突破")
        ambush, ambush_details = detect_ambush_setup(sym, all_sym, code, "D")
        if ambush: tags.append(f"潜伏信号({'+'.join(ambush_details)})")

        if not tags: continue
        bar = sym["D"]["data"][-1]
        close, open_p = float(bar[4]), float(bar[1])
        result_tokens.append({
            "code": code, "name": sym.get("name", ""),
            "price": close, "high_24h": float(bar[2]), "low_24h": float(bar[3]),
            "change_pct": round((close - open_p) / open_p * 100, 2) if open_p else 0,
            "tags": tags,
        })

    fairy = find_fairy_guide(all_sym, [t["code"] for t in result_tokens])
    for t in result_tokens:
        if t["code"] in fairy: t["tags"].append("仙人指路")

    result_tokens.sort(key=lambda x: len(x["tags"]), reverse=True)
    DEFAULT_TAGS = {"趋势共振", "波动充足", "未追高"}
    default_count = sum(1 for t in result_tokens
                        if DEFAULT_TAGS.issubset({tag.split("(")[0] for tag in t["tags"]}))
    elapsed = round(time.time() - scan_start, 1)
    log.info("完成: %d只, %d可分析, %d有标签, 默认%d只, %ss",
             len(all_sym), valid_count, len(result_tokens), default_count, elapsed)

    result = {
        "scanTime": scan_time, "totalSymbols": len(all_sym),
        "validSymbols": valid_count, "filteredCount": default_count,
        "totalTagged": len(result_tokens), "indexDirection": index_direction,
        "marketTurnover": market_turnover,
        "elapsed": elapsed, "tokens": result_tokens,
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    scan_file = DATA_DIR / f"{datetime.now(bj_tz).strftime('%Y-%m-%dT%H-%M-%S')}.json"
    scan_file.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    log.info("写入 %s", scan_file)

    # 清理 7 天前
    cutoff = time.time() - 7 * 86400
    for f in DATA_DIR.glob("*.json"):
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})\.json", f.name)
        if m:
            y, mo, d, h, mi, s = m.groups()
            ts = datetime(int(y),int(mo),int(d),int(h),int(mi),int(s),tzinfo=timezone.utc).timestamp()
            if ts < cutoff: f.unlink()


if __name__ == "__main__":
    asyncio.run(main())
