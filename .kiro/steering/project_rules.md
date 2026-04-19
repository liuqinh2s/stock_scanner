---
inclusion: always
---

# 项目规则

- `cache/` 目录必须提交到 GitHub，不能加入 `.gitignore`。GitHub Actions 依赖缓存的 K 线数据进行增量抓取，每 15 分钟扫描一次，速度至关重要。
