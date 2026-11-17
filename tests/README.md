# 复测核定验证脚本

两个脚本均只通过 HTTP API + 生产库直连验证，不依赖 Docker；本地或 Compose 环境都可运行。

## retest_e2e.py — 复测核定功能回归（41 项断言）

覆盖：合格核定/均值两位小数、同事务写入、超差待复核、幂等重试、同版本冲突、
版本递增、多线程并发（含同版本幂等并发）、源库直改失效、删除/同号重录、查询入口校验。

```bash
# 需要一个可达的 PostgreSQL 16（库 water，口令 postgres）与运行中的 API
SOURCE_HOST=127.0.0.1 SOURCE_PORT=5432 \
ARCHIVE_DIR=/tmp/pg16/archive BACKUP_DIR=/tmp/api-test/backups \
RECOVERY_DIR=/tmp/api-test/recoveries STATE_DB=/tmp/api-test/state.db \
RECOVERY_SOCKET_DIR=/tmp/pg16/run \
PYTHONPATH=<api 目录> python3 -I retest_e2e.py
```

脚本启动时会重置 `samples`/`ph_retests` 到干净状态，请勿对有数据的环境运行。

## pitr_retest_check.py — PITR 可见性验证

依赖已存在的核对点 `p1`（复测前）、`p2`（v1 核定后、v2 前），验证：

- p1 恢复实例复测列全为 NULL（点后复测不提前出现）；
- p2 恢复实例中 v1 的两次原始读数与核定 pH 一同出现，v2 不出现；
- 旧核对点可反复恢复查询原样；恢复实例只读拦截写操作。
