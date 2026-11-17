# 水质数仓单实例 PITR 恢复服务

面向"值班员误删样品后，要取回删前核对时点数据、同时保留生产库后续写入"的场景，
并承接"水质实验室复测入仓后，把两次平行检测核定为可供分析的 pH"的复测核定业务。
基于 **FastAPI + PostgreSQL 16 + Docker Compose**：

- 生产源库可写样品库（样品号 / 站号 / pH），开启 **WAL 持续归档**；
- API 自动制作一份 **物理基准备份**（`pg_basebackup`，源库不停写）；
- 可随时登记 **命名核对点**（`pg_create_restore_point`）；
- 按核对点 **PITR 恢复到独立只读实例** 并查询样品；
- 恢复结果只含核对点前已提交的数据，**跨点后才提交的事务不会出现**；
- 查询只打恢复实例，**绝不用当前源库冒充恢复结果**；
- WAL 未归档齐备时任务显示等待、不开放目标查询；
- 失败或 API 重启后可按原核对点重试；一次只恢复一项；全程不改写生产库；
- **复测核定 API**：两次平行 pH 读数同事务登记/核定，超差只待复核，
  版本递增、幂等重试、源库改值/删除后旧核定自动失效。

## 快速开始

```bash
docker compose up -d --build        # 启动源库 + API（首次自动做基准备份）
python3 scripts/demo.py             # 端到端演示：更正 / 误删 / 跨点事务 + 恢复校验
```

Swagger 文档：<http://localhost:8000/docs>

演示脚本依次验证：

| 场景 | 操作 | 恢复校验 |
| --- | --- | --- |
| 更正 | `UPDATE` 修改 S-2026-0002 的 pH | cp1 中仍是旧值 6.95，cp3 中是新值 7.55 |
| 误删 | `DELETE` 删除 S-2026-0003 | cp1 中查得到，cp3 中已删除 |
| 跨点事务 | 事务在 cp3 前 `INSERT`、cp3 后才 `COMMIT` | cp3 恢复结果中**不含**该行 |
| 源库不受打扰 | 恢复进行期间向源库写入新样品 | 写入成功；恢复实例中也看不到该行（证明没拿源库冒充） |

## 架构

```
                 ┌──────────────────────────── compose ───────────────────────────┐
                 │                                                                  │
  写入/变更 ───▶ │  db (PostgreSQL 16, 可写)                                        │
                 │    archive_mode=on ──WAL 段(原子改名)──▶ [wal_archive 卷]         │
                 │                                          │ (只读挂载)             │
                 │  api (FastAPI + PG16 工具)               ▼                       │
                 │    ① pg_basebackup ──▶ [/backups/base]   核对 WAL 段齐备          │
                 │    ② pg_create_restore_point(核对点) ──▶ 源库（仅 WAL 元数据）    │
                 │    ③ 拷贝备份 + recovery.signal ──▶ [/recoveries/job-N]           │
                 │       restore_command 从归档重放 ─▶ pause@核对点 ─▶ 校验 ─▶ 提升  │
                 │    ④ 只读查询 ◀── 恢复实例 127.0.0.1:55433（独立数据目录）        │
                 │                                                                  │
                 └──────────────────────────────────────────────────────────────────┘
```

## 关键机制与需求对照

| 需求 | 实现 |
| --- | --- |
| 物理基准备份 + 持续归档 | `pg_basebackup -Fp -X stream`（自洽含备份期 WAL）；源库 `archive_command` 先写临时文件再原子 `mv`，API 只会看到完整 WAL 段 |
| 登记命名核对点 | `POST /api/checkpoints` → `pg_create_restore_point` + `pg_switch_wal`（促使该段立即归档） |
| 只含核对点前已提交数据 | `recovery_target_name` + `recovery_target_action=pause`，重放精确停在核对点；跨点后提交的事务不重放 |
| 不用源库冒充恢复 | 查询连接的是 `/recoveries/job-N` 下独立实例（端口 55433），响应中标注实例与 as-of LSN |
| WAL 未齐备显示等待 | 核对点 `waiting_wal` 状态；任务逐段核对"备份起点→核对点"全部 WAL 段，缺段时停留 `waiting_wal` 并列出缺失段 |
| 不把半库报成功 | pause 后校验 `pg_last_wal_replay_lsn() >= 核对点 LSN` 才 promote；再查 `samples`  sanity + `default_transaction_read_only=on` 才置 `ready`；任何异常 → `failed` + 日志 |
| 失败/重启可重试 | 任务状态落 SQLite；`POST /api/recoveries/{id}/retry` 按原点重试；API 重启时进行中任务自动重新排队、ready 实例自动拉起 |
| 一次只恢复一项 | 有进行中任务时 `POST /api/recoveries` 返回 409；新任务就绪前会把旧 ready 实例停掉并标记 `superseded` |
| 源库可写、不被改写 | 基准备份/恢复点均为在线只读操作；API 对源库不执行任何 DML；归档卷对 API 只读挂载 |

## 复测核定（两次平行 pH 检测）

实验室对入仓样品复测后，由数仓管理员把两次平行检测核定为可供分析的 pH。

### 数据模型（PostgreSQL 16，生产库 `water`）

- `public.samples` 扩展列：`retest_version`（现行核定版本）、`ph_reading_a/b`
  （两次原始读数）、`approved_ph`（核定值）、`retest_row_xmin`（核定时行 xmin 快照）、
  `approved_retest_id`；原 `ph` 列始终保留，核定不覆盖原始读数；
- `public.ph_retests`：每次提交留痕（合格 `approved` / 超差 `pending_review` /
  被取代 `superseded`），`(sample_row_id, version)` 与
  `(sample_row_id, idempotency_key)` 两组唯一约束；
- 建表 DDL 位于 `db/init/03_retest.sql`，API 启动时也会幂等执行，老容器卷自动升级；
- 不设外键：源库直接 `DELETE` 样品不被业务表阻止，删除后据行缺失判定旧核定失效。

### 核定规则

| 规则 | 实现 |
| --- | --- |
| 差值 ≤ 0.20 取均值、保留两位小数 | 服务端用 `Decimal` + `ROUND_HALF_UP` 计算，杜绝二进制浮点误差；与读数、版本在**同一事务**写入 `samples` 并登记 `ph_retests` |
| 超差只登记待复核 | 差值 > 0.20 时仅插入 `pending_review` 记录，不更新 `samples.ph` 与任何现行核定字段；旧合格核定同时标记 `superseded`，不再称为现行 |
| 并发不得旧版本覆盖新版本 | 事务内先 `SELECT ... FOR UPDATE` 锁定样品行串行化；版本必须严格递增，低版本返回 409；唯一约束兜底，无重复版本、无脏写 |
| 同版本同内容重试 | 以 `version|读数a|读数b` 为幂等指纹，重试返回原结论并标记 `idempotent_replay=true`，不新增任何写入 |
| 同版本内容冲突 | 指纹不同即返回 409，拒绝覆盖 |
| 源库直接改值 | 任何 `UPDATE` 推进行 `xmin`，与 `retest_row_xmin` 快照不符即判定旧核定失效（另以核定字段链接一致性双保险） |
| 源库删除样品 | 按行 ID/样品号均查不到现行核定；删后同号重录得到**新行 ID**，对旧行 ID 的提交返回 404，样品号与行 ID 不匹配返回 409，新行不能承接旧行复测 |
| PITR 一同恢复 | 读数与核定值是 `samples` 的同行列，恢复到核对点后必然与样品同时出现；点后提交的复测不会提前出现 |

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/retests` | 提交复测核定，body：`sample_no`、`sample_row_id`、`version`（递增）、`ph_reading_a`、`ph_reading_b` |
| GET | `/api/retests?sample_no=` 或 `?sample_row_id=` | 查询当前样品复测结论（现行核定 / 待复核 / 失效原因 + 历史登记） |

```bash
# 合格核定（差 0.08 ≤ 0.20，均值 7.16）
curl -s -X POST localhost:8000/api/retests -H 'content-type: application/json' -d '{
  "sample_no":"S-2026-0001","sample_row_id":1,"version":1,
  "ph_reading_a":7.12,"ph_reading_b":7.20}'
# 当前结论
curl -s 'localhost:8000/api/retests?sample_no=S-2026-0001'
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查（源库连通性、已归档 WAL 数） |
| POST | `/api/basebackup?force=` | 制作物理基准备份（默认启动时自动做一次） |
| GET | `/api/basebackup` | 备份状态（起始 LSN、大小、历史） |
| POST | `/api/checkpoints` | 登记命名核对点 `{"name": "cp1"}` |
| GET | `/api/checkpoints` `/api/checkpoints/{name}` | 核对点状态（`waiting_wal`/`ready`/`expired`） |
| POST | `/api/recoveries` | 按核对点发起恢复 `{"checkpoint": "cp1"}`（一次一项） |
| GET | `/api/recoveries` `/api/recoveries/{id}` | 任务状态查询 |
| POST | `/api/recoveries/{id}/retry` | 失败/取消/被取代后按原点重试 |
| DELETE | `/api/recoveries/{id}` | 取消进行中任务 / 停止已就绪实例 |
| GET | `/api/recoveries/{id}/samples` | 查询恢复实例样品（含原始读数与核定 pH 列，`station_no`/`sample_no` 过滤，仅 `ready` 后开放） |
| POST | `/api/recoveries/{id}/query` | 只读 SQL（演示用，限单条 SELECT，连接强制只读兜底） |
| POST | `/api/retests` | 复测核定提交（样品号/行 ID/递增版本/两次 pH，同事务写入） |
| GET | `/api/retests` | 当前样品复测结论查询（按 `sample_no` 或 `sample_row_id`） |

### 恢复任务状态机

```
queued → waiting_wal → preparing → recovering → verifying → ready
           (等待WAL归档齐备)                            ↘ failed（可 retry）
ready --(新任务就绪前)--> superseded        ready --(DELETE)--> stopped
进行中 --(DELETE)--> cancelled             API 重启：进行中→重新排队；ready→自动拉起
```

## 手动操作示例（curl）

```bash
# 1. 等基准备份完成
curl -s localhost:8000/api/basebackup | jq .latest_done

# 2. 登记核对点（等 status 变 ready，通常几秒）
curl -s -X POST localhost:8000/api/checkpoints -H 'content-type: application/json' -d '{"name":"cp1"}'
curl -s localhost:8000/api/checkpoints/cp1

# 3. 源库继续变更（更正、误删……）
docker compose exec db psql -U postgres -d water -c "DELETE FROM samples WHERE sample_no='S-2026-0003'"

# 4. 发起恢复并等 ready
curl -s -X POST localhost:8000/api/recoveries -H 'content-type: application/json' -d '{"checkpoint":"cp1"}'
curl -s localhost:8000/api/recoveries/1

# 5. 查询删前时点（S-2026-0003 回来了）
curl -s 'localhost:8000/api/recoveries/1/samples?station_no=ST-02'
```

## 限制与说明

- 演示环境口令为 `postgres/postgres`（scram-sha-256，复制连接已在 `db/init/02_hba.sh` 放行），生产使用请更换口令并收敛 pg_hba；
- 归档卷只增不减，生产需自行配置 WAL 保留/清理策略（如 `pg_archivecleanup` + 备份周期）；
- 重做基准备份（`force=true`）会使早于新备份起点的核对点过期（`expired`）；
- 恢复实例监听容器内 55433，仅 API 经本机 socket 访问，不对外暴露；
- `/query` 为演示便利，真正兜底是连接级 `default_transaction_read_only=on`，任何写操作都会被数据库拒绝。

## 已验证项（PostgreSQL 16.10 实测）

在 PostgreSQL 16.10 上按 Compose 同拓扑实测通过：

- 冷启动 → 自动基准备份 → `scripts/demo.py` 全部 10 项断言 PASS（更正/误删/跨点事务/恢复期间源库可写/不拿源库冒充）；
- 归档滞后时核对点与任务停留 `waiting_wal`、目标查询返回 409，归档恢复后任务自动续跑到 `ready`；
- 损坏 WAL 段 → 任务如实 `failed` 并附日志，修复后 `retry` 按原点恢复成功；
- API 优雅重启后 `ready` 实例自动拉起可查；API 被 SIGKILL 于恢复中途（遗留孤儿恢复进程）→ 重启后自动清理并按原点重试至 `ready`；
- 并发第二个恢复请求返回 409；新任务就绪时旧实例被 `superseded`；
- 恢复实例直连写入被数据库拒绝（read-only transaction），`/query` 非 SELECT 与多语句被 400 拦截；
- 强制重做基准备份后旧核对点自动 `expired`，恢复过期核对点返回 409。

### 复测核定已验证项（PostgreSQL 16.16 实测）

- 合格核定：差值 ≤0.20 取均值两位小数（含 half-up 边界 7.015→7.02、临界差值 0.20），
  核定值、两次原始读数、版本在同一事务写入 `samples`，原 `ph` 保留；
- 超差：仅登记 `pending_review`，原 pH 与现行核定字段不变，旧合格核定标记 `superseded`，
  查询返回无现行核定及原因；超差后重新合格可恢复现行；
- 同版本同内容并发/串行重试均返回原结论（`idempotent_replay=true`）且只产生一条登记；
  同版本内容冲突返回 409；旧版本提交返回 409；
- 5 线程 × 5 轮并发压测：行锁串行化，无 500、无重复版本、无脏写，最终版本恒为最高版本，
  乱序到达的低版本被拒绝；事务回滚不留任何部分写入；
- 源库直接 `UPDATE ph` 后旧核定判定失效（xmin 快照机制），历史登记仍可追溯；
- 源库 `DELETE` 后按行 ID/样品号均查不到现行核定；同号重录为新行，对旧行提交 404、
  行号不匹配 409，新行可独立建立核定；
- PITR：cp1（核定前）恢复实例复测列全为 NULL；cp2（v1 后、v2 前）恢复实例中
  v1 的读数 7.12/7.20 与核定 7.16 一同出现、v2 与超差登记不出现；旧核对点可反复恢复查询原样。
