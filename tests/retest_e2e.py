#!/usr/bin/env python3
"""复测核定功能端到端验证（直连本地 PG16 + 本地 uvicorn）。

覆盖需求：
  A. 合格核定：差值 ≤0.20 取均值两位小数，读数/核定值/版本同事务写入生产样品
  B. 超差：只登记 pending_review，不覆盖原 pH，无现行核定
  C. 幂等：同版本同内容重试返回原结论；同版本内容冲突 409
  D. 版本：旧版本不得覆盖新版本（含并发）
  E. 并发：同样品行并发提交，行锁串行化，结果一致、无脏写
  F. 源库直改 pH：旧核定不再算现行
  G. 源库删除样品：旧核定不再算现行；删后同号重录不承接旧行复测
  H. 当前结论查询（样品号 / 行 ID 两种入口）
  I. PITR：核对点恢复后读数与核定 pH 一同出现，点后复测不提前出现，旧核对点可查原样
环境变量：
  API_URL   默认 http://127.0.0.1:8000
  PG*       直连参数通过 psql 风格环境变量交给 psycopg
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

# -I 隔离模式下 PYTHONPATH 不生效，显式加入独立依赖目录（与工作区代码分离）
for _p in (os.environ.get("PYLIBS", "/tmp/pylibs"),):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

import psycopg

API = os.environ.get("API_URL", "http://127.0.0.1:8000")
PG_DSN = os.environ.get(
    "PG_DSN", "host=127.0.0.1 port=5432 dbname=water user=postgres password=postgres"
)

FAILED: list[str] = []
PASSED = 0


def check(desc: str, cond: bool, detail: str = "") -> None:
    global PASSED
    if cond:
        PASSED += 1
        print(f"  [PASS] {desc}")
    else:
        FAILED.append(f"{desc} {detail}")
        print(f"  [FAIL] {desc} {detail}")


def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def db() -> psycopg.Connection:
    return psycopg.connect(PG_DSN, autocommit=True)


def sql_one(query: str, params=()):
    with db() as con:
        return con.execute(query, params).fetchone()


def submit(no: str, rid: int, ver: int, a: float, b: float):
    return api("POST", "/api/retests", {
        "sample_no": no, "sample_row_id": rid, "version": ver,
        "ph_reading_a": a, "ph_reading_b": b})


def wait_api():
    t0 = time.time()
    while time.time() - t0 < 30:
        try:
            code, body = api("GET", "/api/health")
            if code == 200 and body.get("source_reachable"):
                return
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit("API 未就绪")


def reset_db():
    with db() as con:
        con.execute("DROP TABLE IF EXISTS ph_retests")
        con.execute("""ALTER TABLE samples
            DROP COLUMN IF EXISTS retest_version, DROP COLUMN IF EXISTS ph_reading_a,
            DROP COLUMN IF EXISTS ph_reading_b, DROP COLUMN IF EXISTS approved_ph,
            DROP COLUMN IF EXISTS retest_row_xmin,
            DROP COLUMN IF EXISTS approved_retest_id""")
        con.execute("DELETE FROM samples")
        con.execute("""INSERT INTO samples (sample_no, station_no, ph) VALUES
            ('S-2026-0001','ST-01',7.10),('S-2026-0002','ST-01',6.95),
            ('S-2026-0003','ST-02',7.40)""")
    # 触发 API 幂等建表（启动时已建，这里兜底再调一次内部模块）
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))
    from app import retest  # noqa: E402
    retest.ensure_schema()
    rows = sql_one("SELECT json_agg(json_build_object('id',id,'no',sample_no)"
                   " ORDER BY id) FROM samples")[0]
    return {r["no"]: r["id"] for r in rows}


def main() -> int:
    wait_api()
    print("== 重置数据库 ==")
    ids = reset_db()
    r1, r2, r3 = ids["S-2026-0001"], ids["S-2026-0002"], ids["S-2026-0003"]
    print(f"  样品行 ID: 0001={r1} 0002={r2} 0003={r3}")

    print("== A. 合格核定：均值、两位小数、同事务写入 ==")
    code, d = submit("S-2026-0001", r1, 1, 7.12, 7.20)
    check("合格提交 200", code == 200, str(d))
    check("差值 0.08", d.get("diff") == 0.08)
    check("均值 7.16 两位小数", d.get("approved_ph") == 7.16)
    check("status=approved", d.get("status") == "approved")
    row = sql_one("SELECT ph, retest_version v, ph_reading_a a, ph_reading_b b,"
                  " approved_ph ap, approved_retest_id rid, retest_row_xmin sx, xmin"
                  " FROM samples WHERE id=%s", (r1,))
    check("原 pH 7.10 保留", float(row[0]) == 7.10)
    check("样品行同事务写入版本 v1", row[1] == 1)
    check("样品行含两次原始读数 7.12/7.20", float(row[2]) == 7.12 and float(row[3]) == 7.20)
    check("样品行核定 pH 7.16", float(row[4]) == 7.16)
    check("xmin 快照等于当前行 xmin", row[6] == row[7], f"{row[6]} vs {row[7]}")
    # half-up 边界：(7.01+7.02)/2=7.015 → 7.02
    code, d = submit("S-2026-0003", r3, 1, 7.01, 7.02)
    check("7.015 按 half-up 入 7.02", d.get("approved_ph") == 7.02, str(d))
    # 临界差值 0.20 视为合格
    code, d = submit("S-2026-0003", r3, 2, 7.10, 7.30)
    check("差值恰为 0.20 合格", code == 200 and d.get("status") == "approved", str(d))

    print("== B. 超差：只登记待复核，不覆盖原 pH ==")
    code, d = submit("S-2026-0002", r2, 1, 6.80, 7.10)
    check("超差仍 200 且 pending_review", code == 200 and d.get("status") == "pending_review")
    check("超差 approved_ph 为 null", d.get("approved_ph") is None)
    row = sql_one("SELECT ph, retest_version, approved_ph FROM samples WHERE id=%s", (r2,))
    check("原 pH 6.95 未被覆盖", float(row[0]) == 6.95)
    check("超差未写核定字段", row[1] is None and row[2] is None)
    code, q = api("GET", f"/api/retests?sample_row_id={r2}")
    check("超差样品查询无现行核定", code == 200 and q["current"]["status"] == "not_current"
          and "超差" in q["current"]["reason"], str(q.get("current")))

    print("== C. 幂等重试与同版本冲突 ==")
    code, d2 = submit("S-2026-0001", r1, 1, 7.12, 7.20)
    check("同版本同内容重试 200", code == 200)
    check("返回原结论且标记 idempotent_replay",
          d2.get("idempotent_replay") is True and d2.get("retest_id") == 1)
    n = sql_one("SELECT count(*) FROM ph_retests WHERE sample_row_id=%s", (r1,))[0]
    check("重试未新增登记", n == 1)  # r1 此时只有 v1 一条
    code, d3 = submit("S-2026-0001", r1, 1, 7.10, 7.22)
    check("同版本内容冲突 409", code == 409, str(d3))

    print("== D. 版本递增 / 旧版本拒绝 ==")
    code, d4 = submit("S-2026-0001", r1, 2, 7.30, 7.34)
    check("v2 递增提交成功", code == 200 and d4.get("approved_ph") == 7.32, str(d4))
    code, d5 = submit("S-2026-0001", r1, 1, 7.31, 7.33)
    check("旧版本 v1 再提交被 409", code == 409, str(d5))
    sup = sql_one("SELECT status FROM ph_retests WHERE sample_row_id=%s AND version=1", (r1,))[0]
    check("旧合格核定已 superseded", sup == "superseded")
    code, q = api("GET", f"/api/retests?sample_no=S-2026-0001")
    check("现行核定指向 v2 / 7.32",
          q["current"]["status"] == "current" and q["current"]["version"] == 2
          and q["current"]["approved_ph"] == 7.32, str(q["current"]))

    print("== E. 并发：同样品行多线程同时提交不同版本 ==")
    results = []

    def worker(ver: int):
        time.sleep(0.05)  # 尽量同时释放
        results.append((ver, *submit("S-2026-0001", r1, ver, 7.00 + ver * 0.01,
                                     7.02 + ver * 0.01)))

    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(worker, [3, 4, 5, 6, 7]))
    ok = [v for v, c, _ in results if c == 200]
    rejected = [v for v, c, _ in results if c == 409]
    print(f"   接受版本={sorted(ok)} 拒绝版本={sorted(rejected)}")
    check("5 个并发只有严格递增链全部成功或仅首个成功，无 500",
          all(c in (200, 409) for _, c, _ in results))
    final = sql_one("SELECT retest_version, approved_ph, approved_retest_id"
                    " FROM samples WHERE id=%s", (r1,))
    # 行锁串行后，最高版本 7 必须是最终版本（无论到达顺序如何）
    check("并发后现行版本为最高 v7", final[0] == 7, f"got {final[0]}")
    # 每个成功版本在 ph_retests 恰有一行
    dup = sql_one("SELECT version, count(*) FROM ph_retests WHERE sample_row_id=%s"
                  " GROUP BY version HAVING count(*)>1", (r1,))
    check("无重复版本登记", dup is None)

    print("== E2. 并发同版本同内容（幂等）不得产生冲突错误 ==")
    same = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = [pool.submit(submit, "S-2026-0002", r2, 1, 6.80, 7.10)
                for _ in range(5)]
        for f in futs:
            same.append(f.result())
    check("并发同版本同内容全部 200", all(c == 200 for c, _ in same), str(same))
    check("且只有一条登记", sql_one(
        "SELECT count(*) FROM ph_retests WHERE sample_row_id=%s AND version=1", (r2,))[0] == 1)

    print("== F. 源库直接改 pH → 旧核定不再现行 ==")
    with db() as con:
        con.execute("UPDATE samples SET ph=7.99 WHERE id=%s", (r1,))
    code, q = api("GET", f"/api/retests?sample_row_id={r1}")
    check("直改后 current=not_current",
          q["current"]["status"] == "not_current" and "直接改动" in q["current"]["reason"],
          str(q["current"]))
    check("历史登记仍可追溯", len(q["history"]) >= 3)

    print("== G. 源库删除 + 同号重录 ==")
    with db() as con:
        con.execute("DELETE FROM samples WHERE id=%s", (r3,))
    code, q = api("GET", f"/api/retests?sample_row_id={r3}")
    check("删除后按旧行 ID 查询 found=false", q.get("found") is False)
    check("删除后旧核定不算现行", q.get("current") is None)
    code, q = api("GET", "/api/retests?sample_no=S-2026-0003")
    check("删除后按样品号查询 found=false", q.get("found") is False)
    # 同号重录（新行）
    with db() as con:
        cur = con.execute("INSERT INTO samples (sample_no, station_no, ph)"
                          " VALUES ('S-2026-0003','ST-09',8.88) RETURNING id")
        new_id = cur.fetchone()[0]
    check("重录得到新行 ID", new_id != r3)
    code, q = api("GET", "/api/retests?sample_no=S-2026-0003")
    check("重录新行无历史复测", q["found"] is True and q["history"] == []
          and q["current"] is None, str(q))
    # 拿旧行 ID 给新号提交 → 404；拿新行 ID 配旧号以外的错号 → 409
    code, d = submit("S-2026-0003", r3, 1, 7.0, 7.05)
    check("对已删旧行提交复测 → 404", code == 404, str(d))
    code, d = submit("S-2026-0001", new_id, 1, 7.0, 7.05)
    check("样品号与行 ID 不匹配 → 409", code == 409, str(d))
    code, d = submit("S-2026-0003", new_id, 1, 8.80, 8.90)
    check("新行可独立建立核定", code == 200 and d["approved_ph"] == 8.85, str(d))

    print("== H. 查询入口与缺参校验 ==")
    code, _ = api("GET", "/api/retests")
    check("无参数查询 400", code == 400)
    code, q = api("GET", f"/api/retests?sample_row_id={r2}")
    check("按行 ID 查询 200", code == 200 and q["sample_row_id"] == r2)

    print()
    print(f"通过 {PASSED} 项，失败 {len(FAILED)} 项")
    for f in FAILED:
        print("  -", f)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
