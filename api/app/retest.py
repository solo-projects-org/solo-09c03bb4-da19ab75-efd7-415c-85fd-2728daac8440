"""复测核定：两次平行 pH 检测的核定写入与结论查询（生产库 PostgreSQL 16）。

并发与正确性约定：
- 一次提交在**单事务**内完成：锁定样品行 → 版本/幂等校验 → 登记复测 →
  合格则更新样品行（核定值 + 原始读数 + 版本），超差只登记待复核；
- 行锁 `SELECT ... FOR UPDATE` 串行化同样品行的并发提交，旧版本不可能覆盖新版本；
- `(sample_row_id, version)` 与 `(sample_row_id, idempotency_key)` 唯一约束兜底：
  同版本同内容重试返回原结论，同版本内容冲突拒绝（409）；
- 现行性不单独维护“当前标记”，而是按事实实时判定：
  * 样品行已删除 → 旧核定不算现行；
  * 源库直接改了 ph（行 xmin 变化或 ph 与核定时不一致）→ 旧核定不算现行；
  * 同号删后重录是新行（新 ID），无法承接旧行的复测。
"""
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from . import config

TOLERANCE = Decimal("0.20")


class RetestError(Exception):
    """业务拒绝（携带建议 HTTP 状态码）。"""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = status_code


def _conn() -> psycopg.Connection:
    """生产库连接（autocommit=False，由显式事务包裹）。"""
    return psycopg.connect(
        host=config.SOURCE_HOST,
        port=config.SOURCE_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=False,
        connect_timeout=5,
        application_name="pitr-api-retest",
        row_factory=dict_row,
    )


def _q2(value: Decimal) -> Decimal:
    """两位小数、四舍五入（实验室惯例 half-up），避免二进制浮点误差。"""
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _idem_key(version: int, a: Decimal, b: Decimal) -> str:
    return f"v{version}|{_q2(a)}|{_q2(b)}"


def ensure_schema() -> None:
    """幂等建表/加列（容器老卷升级、本地开发均适用）。"""
    with _conn() as con:
        con.execute(_SCHEMA_SQL)
        con.commit()


# ---------------------------------------------------------------- 提交核定

def submit_retest(sample_no: str, sample_row_id: int, version: int,
                  reading_a: float, reading_b: float) -> dict[str, Any]:
    a = _q2(Decimal(str(reading_a)))
    b = _q2(Decimal(str(reading_b)))
    diff = _q2(abs(a - b))
    within = diff <= TOLERANCE
    approved = _q2((a + b) / 2) if within else None
    key = _idem_key(version, a, b)

    with _conn() as con:
        # 1) 锁定样品行：串行化同样品行的并发提交，且立即确认行/样品号仍匹配
        row = con.execute(
            "SELECT id, sample_no, ph, xmin AS row_xmin,"
            "       retest_version, approved_retest_id"
            " FROM public.samples WHERE id = %s FOR UPDATE",
            (sample_row_id,),
        ).fetchone()
        if row is None:
            raise RetestError(404, f"样品行 #{sample_row_id} 不存在（可能已从源库删除）；"
                                   "删后同号重录为新行，不能承接旧行复测")
        if row["sample_no"] != sample_no:
            raise RetestError(
                409, f"样品行 #{sample_row_id} 当前样品号为 {row['sample_no']}，"
                     f"与请求样品号 {sample_no} 不一致（可能删后同号重录），拒绝承接旧行复测")

        # 2) 同版本处理：先查既有登记（行锁保护，并发同版本也会在唯一约束上再兜底）
        existing = con.execute(
            "SELECT * FROM public.ph_retests WHERE sample_row_id = %s AND version = %s",
            (sample_row_id, version),
        ).fetchone()
        if existing is not None:
            if existing["idempotency_key"] == key:
                con.rollback()  # 只读重试：原样返回既有结论，不产生任何写入
                return _conclusion_payload(existing, retry=True)
            raise RetestError(
                409, f"样品行 #{sample_row_id} 版本 {version} 已存在内容不同的复测登记，"
                     "同版本内容冲突，拒绝覆盖")

        # 3) 版本必须递增：旧版本不得覆盖新版本
        if row["retest_version"] is not None and version <= row["retest_version"]:
            raise RetestError(
                409, f"样品行 #{sample_row_id} 现行核定版本为 {row['retest_version']}，"
                     f"提交版本 {version} 不是递增版本，拒绝旧版本覆盖")

        # 4) 登记本次复测（合格 / 超差待复核），同内容跨版本重试也拒绝
        status = "approved" if within else "pending_review"
        if within:
            conclusion = (f"两次读数差值 {diff} ≤ {TOLERANCE}，核定 pH 取均值 {approved}")
        else:
            conclusion = (f"两次读数差值 {diff} > {TOLERANCE}，超差待人工复核，"
                          "不覆盖原 pH")
        try:
            cur = con.execute(
                "INSERT INTO public.ph_retests"
                " (sample_no, sample_row_id, version, ph_reading_a, ph_reading_b,"
                "  diff, approved_ph, status, conclusion, idempotency_key)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                " RETURNING *",
                (sample_no, sample_row_id, version, a, b, diff, approved,
                 status, conclusion, key),
            )
            record = cur.fetchone()
        except psycopg.errors.UniqueViolation as e:
            con.rollback()
            raise RetestError(409, f"复测登记冲突（可能为重复/并发提交）：{e.diag.message_primary}")

        # 5) 合格：同一事务把核定值、原始读数、版本写到生产样品行。
        #    PostgreSQL 的 `SET col = xmin` 取的是更新前的旧 xmin，因此先完成业务列更新，
        #    再在同一事务里把"新行 xmin"回写到快照列；此后源库任何直改都会再推进 xmin。
        if within:
            con.execute(
                "UPDATE public.samples SET"
                "  retest_version = %s,"
                "  ph_reading_a = %s,"
                "  ph_reading_b = %s,"
                "  approved_ph = %s,"
                "  approved_retest_id = %s"
                " WHERE id = %s",
                (version, a, b, approved, record["id"], sample_row_id),
            )
            con.execute(
                "UPDATE public.samples SET retest_row_xmin ="
                "  (SELECT xmin FROM public.samples WHERE id = %s)"
                " WHERE id = %s",
                (sample_row_id, sample_row_id),
            )

        # 6) 旧的合格核定不再称为现行：本次合格则旧记录退位；
        #    本次超差同样让旧合格记录退位（超差登记本身从未成为现行，状态保留）。
        con.execute(
            "UPDATE public.ph_retests SET status = 'superseded'"
            " WHERE sample_row_id = %s AND status = 'approved' AND id <> %s",
            (sample_row_id, record["id"]),
        )
        # 超差：不更新 samples.ph，也不动 samples 上的任何现行核定字段。

        con.commit()
        return _conclusion_payload(record, retry=False, approved_now=within)


# ---------------------------------------------------------------- 结论查询

def get_retest_conclusion(sample_no: str | None = None,
                          sample_row_id: int | None = None) -> dict[str, Any]:
    """查询当前样品的复测结论（按生产库实时状态判定现行性）。"""
    if not sample_no and sample_row_id is None:
        raise RetestError(400, "请至少提供 sample_no 或 sample_row_id 之一")
    with _conn() as con:
        if sample_row_id is not None:
            row = con.execute(
                "SELECT id, sample_no, station_no, ph, xmin AS row_xmin,"
                "       retest_version, ph_reading_a, ph_reading_b, approved_ph,"
                "       retest_row_xmin, approved_retest_id"
                " FROM public.samples WHERE id = %s",
                (sample_row_id,),
            ).fetchone()
        else:
            row = con.execute(
                "SELECT id, sample_no, station_no, ph, xmin AS row_xmin,"
                "       retest_version, ph_reading_a, ph_reading_b, approved_ph,"
                "       retest_row_xmin, approved_retest_id"
                " FROM public.samples WHERE sample_no = %s",
                (sample_no,),
            ).fetchone()

        if row is None:
            # 删后同号重录不能承接：只按当前库事实回答“无此行”
            return {
                "found": False,
                "sample_no": sample_no,
                "sample_row_id": sample_row_id,
                "current": None,
                "message": "当前生产库无此样品行（已删除或从未录入）；旧核定不再算现行",
                "history": _history(con, sample_row_id, sample_no),
            }

        rid = row["id"]
        latest = con.execute(
            "SELECT * FROM public.ph_retests WHERE sample_row_id = %s"
            " ORDER BY version DESC, id DESC LIMIT 1",
            (rid,),
        ).fetchone()
        history = con.execute(
            "SELECT * FROM public.ph_retests WHERE sample_row_id = %s"
            " ORDER BY version DESC, id DESC",
            (rid,),
        ).fetchall()

        current = _current_for_row(row, latest, con)
        return {
            "found": True,
            "sample_no": row["sample_no"],
            "sample_row_id": rid,
            "station_no": row["station_no"],
            "original_ph": _float(row["ph"]),
            "current": current,
            "history": [_retest_brief(r) for r in history],
        }


def _current_for_row(row: dict, latest: dict | None, con: psycopg.Connection) -> dict | None:
    """按事实判定现行核定：样品行实时状态 vs 核定时快照。"""
    if latest is None:
        return None
    # 源库直接改值：任何 UPDATE 都会推进 xmin。用 age() 比较而非裸等号，
    # 规避 xid 回绕（epoch）带来的误判；快照为当前行 xmin 时两者 age 相同。
    if row["retest_row_xmin"] is None:
        xmin_ok = False
    else:
        xmin_ok = con.execute(
            "SELECT age(%s::xid) = age(%s::xid) AS ok",
            (row["row_xmin"], row["retest_row_xmin"]),
        ).fetchone()["ok"]
    # 双保险：核定值/版本与样品行上的现行字段一致，且指向最新合格登记
    link_ok = (row["approved_retest_id"] == latest["id"]
               and row["retest_version"] == latest["version"])
    if latest["status"] == "approved" and xmin_ok and link_ok:
        return {
            "status": "current",
            "retest_id": latest["id"],
            "version": latest["version"],
            "ph_reading_a": _float(latest["ph_reading_a"]),
            "ph_reading_b": _float(latest["ph_reading_b"]),
            "diff": _float(latest["diff"]),
            "approved_ph": _float(latest["approved_ph"]),
            "conclusion": latest["conclusion"],
            "created_at": latest["created_at"].isoformat(),
        }
    # 否则没有现行核定：说明最新一次是超差待复核，或源库已直接改值
    reason = None
    if latest["status"] == "pending_review":
        reason = "最新复测超差，登记待复核，原 pH 未被覆盖，故无现行核定"
    elif not xmin_ok:
        reason = "源库在核定后直接改动过样品行，旧核定不再算现行"
    elif not link_ok:
        reason = "样品行现行核定字段与最新复测登记不一致，旧核定不再算现行"
    else:
        reason = "最新复测登记不是现行合格核定"
    return {
        "status": "not_current",
        "latest_retest": _retest_brief(latest),
        "reason": reason,
    }


def _history(con: psycopg.Connection, row_id: int | None,
             sample_no: str | None) -> list[dict]:
    if row_id is not None:
        rows = con.execute(
            "SELECT * FROM public.ph_retests WHERE sample_row_id = %s"
            " ORDER BY version DESC, id DESC", (row_id,)).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM public.ph_retests WHERE sample_no = %s"
            " ORDER BY version DESC, id DESC", (sample_no,)).fetchall()
    return [_retest_brief(r) for r in rows]


# ---------------------------------------------------------------- 序列化

def _conclusion_payload(record: dict, retry: bool,
                        approved_now: bool = False) -> dict:
    return {
        "retest_id": record["id"],
        "sample_no": record["sample_no"],
        "sample_row_id": record["sample_row_id"],
        "version": record["version"],
        "ph_reading_a": _float(record["ph_reading_a"]),
        "ph_reading_b": _float(record["ph_reading_b"]),
        "diff": _float(record["diff"]),
        "tolerance": float(TOLERANCE),
        "approved_ph": _float(record["approved_ph"]),
        "status": record["status"],
        "conclusion": record["conclusion"],
        "idempotent_replay": retry,
        "sample_updated": approved_now,
        "created_at": record["created_at"].isoformat(),
    }


def _retest_brief(r: dict) -> dict:
    return {
        "retest_id": r["id"],
        "version": r["version"],
        "ph_reading_a": _float(r["ph_reading_a"]),
        "ph_reading_b": _float(r["ph_reading_b"]),
        "diff": _float(r["diff"]),
        "approved_ph": _float(r["approved_ph"]),
        "status": r["status"],
        "conclusion": r["conclusion"],
        "created_at": r["created_at"].isoformat(),
    }


def _float(v):
    return float(v) if v is not None else None


# 与 db/init/03_retest.sql 保持一致（API 启动幂等执行，老卷自动升级）
_SCHEMA_SQL = """
ALTER TABLE public.samples
    ADD COLUMN IF NOT EXISTS retest_version     integer,
    ADD COLUMN IF NOT EXISTS ph_reading_a       numeric(4,2),
    ADD COLUMN IF NOT EXISTS ph_reading_b       numeric(4,2),
    ADD COLUMN IF NOT EXISTS approved_ph        numeric(4,2),
    ADD COLUMN IF NOT EXISTS retest_row_xmin    xid,
    ADD COLUMN IF NOT EXISTS approved_retest_id bigint;

CREATE TABLE IF NOT EXISTS public.ph_retests (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_no       text NOT NULL,
    sample_row_id   bigint NOT NULL,
    version         integer NOT NULL,
    ph_reading_a    numeric(4,2) NOT NULL CHECK (ph_reading_a BETWEEN 0 AND 14),
    ph_reading_b    numeric(4,2) NOT NULL CHECK (ph_reading_b BETWEEN 0 AND 14),
    diff            numeric(4,2) NOT NULL,
    approved_ph     numeric(4,2),
    status          text NOT NULL CHECK (status IN
                        ('approved', 'pending_review', 'superseded', 'obsolete')),
    conclusion      text NOT NULL,
    idempotency_key text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sample_row_id, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ph_retests_idem
    ON public.ph_retests (sample_row_id, idempotency_key);

CREATE INDEX IF NOT EXISTS idx_ph_retests_sample
    ON public.ph_retests (sample_row_id, version DESC);
"""
