"""同源多表抽取顺序锁：表清单、顺序板、跳表策略与表序争抢。

领域约定：
- 同一管道内按 pipeline_tables.seq 的声明顺序抽表，序位由服务端裁定。
- 上游表未齐（未抽取完成）时按跳表策略处理：
  block 写顺序锁并拒绝抽取；allow_pending 允许抽取但标待对齐且禁止落库。
- 同一时刻只允许一张表占用抽取流程。同刻点击不同表时，声明序最前的一张
  胜出，其余登记为「表序争抢失败」，并出现在顺序板的争抢记录里。
"""

import os
import sqlite3
import threading
import time
from datetime import date, datetime

from app.db import (
    EXTRACTABLE_STATUSES,
    IN_FLOW_STATUSES,
    POLICY_ALLOW,
    POLICY_BLOCK,
    POLICY_DESCRIPTIONS,
    POLICY_KEYWORDS,
    POLICY_LABELS,
    POLICY_TITLES,
    READY_STATUSES,
    STATUS_ALIGN,
    STATUS_EXTRACTED,
    STATUS_EXTRACTING,
    STATUS_FAILED,
    STATUS_LOADED,
    STATUS_PENDING,
    LabError,
    get_db,
    get_pipeline,
)

# 同刻窗口与模拟抽取占用时长，可用环境变量调整（测试可缩短）。
COALESCE_SECONDS = float(os.environ.get("ETL_RACE_COALESCE_SECONDS", "0.15"))
HOLD_SECONDS = float(os.environ.get("ETL_EXTRACT_HOLD_SECONDS", "0.30"))

TABLE_TAG = {
    STATUS_PENDING: "",
    STATUS_EXTRACTING: "busy",
    STATUS_EXTRACTED: "warn",
    STATUS_ALIGN: "align",
    STATUS_LOADED: "ok",
    STATUS_FAILED: "err",
}

_DAY = date.today().isoformat()

TABLE_COLUMNS = (
    "id, pipeline_id, name, seq, status, extracted, loaded, "
    "pending_since, extracted_at, updated_at"
)

# ---------------------------------------------------------------------------
# 运行期协调：同刻窗口裁定 + 当前占用表
# ---------------------------------------------------------------------------

_RUNTIME_LOCK = threading.RLock()
_WINDOWS: dict[int, "_Window"] = {}
_ACTIVE: dict[int, dict] = {}
_INTENT_COUNTER = [0]
# 拿不到占用表的请求在裁定批里最多等这么久，保证「同刻只成最前表」。
LOST_WAIT_SECONDS = 0.6


class _Intent:
    __slots__ = ("table_id", "table_name", "seq", "arrival")

    def __init__(self, table_id: int, table_name: str, seq: int, arrival: int):
        self.table_id = table_id
        self.table_name = table_name
        self.seq = seq
        self.arrival = arrival


class _Window:
    __slots__ = ("deadline", "intents", "decision", "winner_intent")

    def __init__(self, deadline: float):
        self.deadline = deadline
        self.intents: list[_Intent] = []
        self.decision: _Intent | None = None
        self.winner_intent: _Intent | None = None


def reset_runtime_state() -> None:
    """仅测试使用：清空同刻窗口与占用表。"""
    with _RUNTIME_LOCK:
        _WINDOWS.clear()
        _ACTIVE.clear()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _require_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        raise LabError(404, "管道不存在", "pipeline_not_found")
    return item


# ---------------------------------------------------------------------------
# 表清单
# ---------------------------------------------------------------------------

def list_tables(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    return _rows(
        conn,
        "SELECT " + TABLE_COLUMNS + " FROM pipeline_tables "
        "WHERE pipeline_id = ? ORDER BY seq, id",
        (pipeline_id,),
    )


def find_table(conn: sqlite3.Connection, pipeline_id: int, ref) -> dict | None:
    if isinstance(ref, int) or (
        isinstance(ref, str) and ref.strip().isdigit()
    ):
        row = conn.execute(
            "SELECT " + TABLE_COLUMNS + " FROM pipeline_tables "
            "WHERE pipeline_id = ? AND id = ?",
            (pipeline_id, int(ref)),
        ).fetchone()
        if row:
            return dict(row)
    row = conn.execute(
        "SELECT " + TABLE_COLUMNS + " FROM pipeline_tables "
        "WHERE pipeline_id = ? AND name = ?",
        (pipeline_id, str(ref or "").strip()),
    ).fetchone()
    return dict(row) if row else None


def _upstream(conn: sqlite3.Connection, pipeline_id: int, seq: int) -> list[dict]:
    return _rows(
        conn,
        "SELECT id, name, seq, status FROM pipeline_tables "
        "WHERE pipeline_id = ? AND seq < ? ORDER BY seq, id",
        (pipeline_id, seq),
    )


def _missing_upstream(conn: sqlite3.Connection, pipeline_id: int, seq: int) -> list[str]:
    return [
        r["name"] for r in _upstream(conn, pipeline_id, seq)
        if r["status"] not in READY_STATUSES
    ]


def _ready_names(conn: sqlite3.Connection, pipeline_id: int) -> set[str]:
    return {
        r["name"] for r in _rows(
            conn,
            "SELECT name, status FROM pipeline_tables WHERE pipeline_id = ?",
            (pipeline_id,),
        ) if r["status"] in READY_STATUSES
    }


def _locks(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    rows = _rows(
        conn,
        "SELECT id, pipeline_id, table_id, table_name, missing_upstream, reason, "
        "policy, created_at FROM order_locks WHERE pipeline_id = ? ORDER BY id",
        (pipeline_id,),
    )
    for row in rows:
        row["kind"] = "order_lock"
        row["label"] = "顺序锁"
    return rows


def _races(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    rows = _rows(
        conn,
        "SELECT id, pipeline_id, table_id, table_name, won_table_id, won_table_name, "
        "reason, created_at FROM table_races WHERE pipeline_id = ? ORDER BY id",
        (pipeline_id,),
    )
    for row in rows:
        row["kind"] = "order_race_lost"
        row["label"] = "表序争抢失败"
    return rows


def _active_lock(conn: sqlite3.Connection, pipeline_id: int, table_id: int,
                 ready: set[str]) -> dict | None:
    """仍被顺序锁卡住时返回最近的锁记录（缺的上游都已齐则视为已解除）。"""
    for lock in reversed(_locks(conn, pipeline_id)):
        if lock["table_id"] != table_id:
            continue
        missing = [
            n for n in lock["missing_upstream"].split("、")
            if n and n not in ready
        ]
        if missing:
            return {**lock, "missing_now": missing}
    return None


def table_view(conn: sqlite3.Connection, pipeline_id: int, row: dict,
               ready: set[str] | None = None) -> dict:
    ready = ready if ready is not None else _ready_names(conn, pipeline_id)
    missing = _missing_upstream(conn, pipeline_id, row["seq"])
    lock = _active_lock(conn, pipeline_id, row["id"], ready)
    view = dict(row)
    view["status_tag"] = TABLE_TAG.get(row["status"], "")
    view["upstream"] = [r["name"] for r in _upstream(conn, pipeline_id, row["seq"])]
    view["missing_upstream"] = missing
    view["align_pending"] = row["status"] == STATUS_ALIGN
    view["align_label"] = "待对齐" if row["status"] == STATUS_ALIGN else ""
    view["order_locked"] = lock is not None
    view["order_lock_label"] = "顺序锁" if lock is not None else ""
    view["order_lock"] = lock
    view["extractable"] = row["status"] in EXTRACTABLE_STATUSES and lock is None
    view["loadable"] = row["status"] == STATUS_EXTRACTED
    return view


def tables_view(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    ready = _ready_names(conn, pipeline_id)
    return [
        table_view(conn, pipeline_id, row, ready)
        for row in list_tables(conn, pipeline_id)
    ]


POLICY_OPTIONS = [
    {
        "value": POLICY_BLOCK,
        "title": POLICY_TITLES[POLICY_BLOCK],
        "label": POLICY_LABELS[POLICY_BLOCK],
        "note": POLICY_DESCRIPTIONS[POLICY_BLOCK],
    },
    {
        "value": POLICY_ALLOW,
        "title": POLICY_TITLES[POLICY_ALLOW],
        "label": POLICY_LABELS[POLICY_ALLOW],
        "note": POLICY_DESCRIPTIONS[POLICY_ALLOW],
    },
]


def policy_mismatch(policy: str, note: str) -> bool:
    """说明与所选当日口径不一致，即「跳表策略偏了」。"""
    keywords = POLICY_KEYWORDS.get(policy, ())
    if not keywords:
        return True
    return not all(word in (note or "") for word in keywords)


def policy_payload(pipeline: dict) -> dict:
    policy = pipeline.get("skip_policy") or POLICY_BLOCK
    note = pipeline.get("policy_note") or ""
    return {
        "skip_policy": policy,
        "policy_title": POLICY_TITLES.get(policy, policy),
        "policy_label": POLICY_LABELS.get(policy, ""),
        "policy_note": note,
        "policy_day": pipeline.get("policy_date") or _DAY,
        "today": _DAY,
        "policy_options": POLICY_OPTIONS,
        "policy_mismatch": policy_mismatch(policy, note),
    }


def _occupancy(pipeline_id: int) -> dict | None:
    with _RUNTIME_LOCK:
        active = _ACTIVE.get(pipeline_id)
        if not active:
            return None
        return {
            "table_id": active["table_id"],
            "table_name": active["table_name"],
            "busy": True,
        }


def board(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    pipeline = _require_pipeline(conn, pipeline_id)
    rows = list_tables(conn, pipeline_id)
    tables = tables_view(conn, pipeline_id)
    return {
        "pipeline": pipeline,
        "policy": policy_payload(pipeline),
        "tables": tables,
        "order_locks": _locks(conn, pipeline_id),
        "races": _races(conn, pipeline_id),
        "occupancy": _occupancy(pipeline_id),
        "summary": {
            "table_count": len(rows),
            "pending": sum(1 for r in rows if r["status"] == STATUS_PENDING),
            "extracting": sum(1 for r in rows if r["status"] == STATUS_EXTRACTING),
            "extracted": sum(1 for r in rows if r["status"] == STATUS_EXTRACTED),
            "aligned": sum(1 for r in rows if r["status"] == STATUS_ALIGN),
            "loaded": sum(1 for r in rows if r["status"] == STATUS_LOADED),
            "failed": sum(1 for r in rows if r["status"] == STATUS_FAILED),
            "order_locked": sum(1 for t in tables if t["order_locked"]),
            "in_flow": any(r["status"] in IN_FLOW_STATUSES for r in rows),
        },
        "runtime": {
            "coalesce_seconds": COALESCE_SECONDS,
            "hold_seconds": HOLD_SECONDS,
        },
    }


def detail(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    payload = board(conn, pipeline_id)
    return {
        **payload["pipeline"],
        "policy": payload["policy"],
        "tables": payload["tables"],
        "order_locks": payload["order_locks"],
        "races": payload["races"],
        "occupancy": payload["occupancy"],
        "summary": payload["summary"],
    }


def overview(conn: sqlite3.Connection) -> list[dict]:
    items = []
    for pipeline in _rows(
        conn,
        "SELECT id, title, code, status, source_uri, extracted, loaded, created_at, "
        "skip_policy, policy_note, policy_date FROM pipelines ORDER BY id",
    ):
        tables = tables_view(conn, pipeline["id"])
        items.append({
            **pipeline,
            "policy": policy_payload(pipeline),
            "tables": tables,
            "summary": {
                "table_count": len(tables),
                "aligned": sum(1 for t in tables if t["align_pending"]),
                "order_locked": sum(1 for t in tables if t["order_locked"]),
            },
        })
    return items


# ---------------------------------------------------------------------------
# 跳表设置 / 表序调整 / 表清单维护
# ---------------------------------------------------------------------------

def set_policy(conn: sqlite3.Connection, pipeline_id: int, policy: str,
               note: str | None = None) -> dict:
    pipeline = _require_pipeline(conn, pipeline_id)
    if policy not in (POLICY_BLOCK, POLICY_ALLOW):
        raise LabError(400, "跳表策略不在可选范围", "invalid_policy",
                       {"policy": policy})
    note = (note or "").strip() or POLICY_DESCRIPTIONS[policy]
    conn.execute(
        "UPDATE pipelines SET skip_policy = ?, policy_note = ?, policy_date = ? "
        "WHERE id = ?",
        (policy, note, _DAY, pipeline_id),
    )
    updated = get_pipeline(conn, pipeline_id) or pipeline
    return {"pipeline": updated, "policy": policy_payload(updated)}


def reorder(conn: sqlite3.Connection, pipeline_id: int, ordered=None) -> dict:
    _require_pipeline(conn, pipeline_id)
    rows = list_tables(conn, pipeline_id)
    in_flow = [r["name"] for r in rows if r["status"] in IN_FLOW_STATUSES]
    if in_flow:
        raise LabError(
            409,
            "表仍在抽取流程里，禁止改表顺序",
            "order_locked_in_flow",
            {"tables": in_flow, "tables_in_flow": in_flow},
        )
    if not ordered:
        raise LabError(400, "缺少表顺序", "invalid_table_order")
    ids = []
    for ref in ordered:
        row = find_table(conn, pipeline_id, ref)
        if not row:
            raise LabError(400, "表清单与实际表不一致", "invalid_table_order",
                           {"ref": ref})
        ids.append(row["id"])
    if len(set(ids)) != len(ids) or sorted(ids) != sorted(r["id"] for r in rows):
        raise LabError(400, "表清单与实际表不一致", "invalid_table_order")
    for index, table_id in enumerate(ids):
        conn.execute(
            "UPDATE pipeline_tables SET seq = ?, updated_at = ? WHERE id = ?",
            (index + 1, _now(), table_id),
        )
    _rollup(conn, pipeline_id)
    return board(conn, pipeline_id)


def add_table(conn: sqlite3.Connection, pipeline_id: int, name) -> dict:
    _require_pipeline(conn, pipeline_id)
    cleaned = str(name or "").strip()
    if not cleaned:
        raise LabError(400, "空表名禁止入清单", "empty_table_name")
    duplicate = conn.execute(
        "SELECT id FROM pipeline_tables WHERE pipeline_id = ? AND name = ?",
        (pipeline_id, cleaned),
    ).fetchone()
    if duplicate:
        raise LabError(400, "表名已在清单中", "duplicate_table_name",
                       {"table": cleaned})
    max_seq = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS m FROM pipeline_tables WHERE pipeline_id = ?",
        (pipeline_id,),
    ).fetchone()["m"]
    conn.execute(
        "INSERT INTO pipeline_tables (pipeline_id, name, seq, status, updated_at) "
        "VALUES (?,?,?,?,?)",
        (pipeline_id, cleaned, int(max_seq) + 1, STATUS_PENDING, _now()),
    )
    _rollup(conn, pipeline_id)
    return board(conn, pipeline_id)


# ---------------------------------------------------------------------------
# 汇总与状态流转
# ---------------------------------------------------------------------------

def _rollup(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    rows = list_tables(conn, pipeline_id)
    extracted = sum(r["extracted"] for r in rows)
    loaded = sum(r["loaded"] for r in rows)
    if rows and all(r["status"] == STATUS_LOADED for r in rows):
        status = STATUS_LOADED
    elif any(r["status"] == STATUS_EXTRACTING for r in rows):
        status = STATUS_EXTRACTING
    elif any(r["status"] == STATUS_ALIGN for r in rows):
        status = STATUS_ALIGN
    elif any(r["status"] == STATUS_EXTRACTED for r in rows):
        status = STATUS_EXTRACTED
    elif any(r["status"] == STATUS_FAILED for r in rows):
        status = STATUS_FAILED
    else:
        status = STATUS_PENDING
    conn.execute(
        "UPDATE pipelines SET status = ?, extracted = ?, loaded = ? WHERE id = ?",
        (status, extracted, loaded, pipeline_id),
    )
    return get_pipeline(conn, pipeline_id) or {}


def _set_status(conn: sqlite3.Connection, row: dict, status: str,
                extracted: int | None = None, pending_since: str | None = None,
                clear_pending: bool = False) -> None:
    sets = ["status = ?", "updated_at = ?"]
    params = [status, _now()]
    if extracted is not None:
        sets += ["extracted = ?", "loaded = 0"]
        params.append(extracted)
    if pending_since is not None:
        sets.append("pending_since = ?")
        params.append(pending_since)
    if clear_pending:
        sets.append("pending_since = NULL")
    if status == STATUS_LOADED:
        sets.append("loaded = extracted")
    if status in (STATUS_EXTRACTED, STATUS_ALIGN):
        sets.append("extracted_at = ?")
        params.append(_now())
    params.append(row["id"])
    conn.execute("UPDATE pipeline_tables SET " + ", ".join(sets) + " WHERE id = ?",
                 params)


def _release_aligned(conn: sqlite3.Connection, pipeline_id: int) -> list[str]:
    """上游抽取完成后，把不再缺上游的待对齐表转为已抽取待加载。"""
    released = []
    for row in list_tables(conn, pipeline_id):
        if row["status"] != STATUS_ALIGN:
            continue
        if _missing_upstream(conn, pipeline_id, row["seq"]):
            continue
        conn.execute(
            "UPDATE pipeline_tables SET status = ?, pending_since = NULL, "
            "updated_at = ? WHERE id = ?",
            (STATUS_EXTRACTED, _now(), row["id"]),
        )
        released.append(row["name"])
    return released


# ---------------------------------------------------------------------------
# 同刻窗口裁定
# ---------------------------------------------------------------------------

def _race_result(winner: dict | None) -> dict:
    return {
        "won": False,
        "winner_table_id": winner["table_id"] if winner else None,
        "winner_table_name": winner["table_name"] if winner else "",
        "kind": "race",
        "reason": (f"同刻表序争抢失败：按声明顺序 {winner['table_name']} 在前"
                   if winner else "同刻表序争抢失败"),
    }


def _elect(pipeline_id: int, table: dict) -> dict:
    """把同刻到达的请求合并进一批，按声明顺序裁定唯一胜者。

    裁定完立刻登记占用表；落败者最多等到批主处理完（或短暂超时），
    以此保证同刻只有序位最前的一张表能成功。
    """
    with _RUNTIME_LOCK:
        active = _ACTIVE.get(pipeline_id)
        if active:
            if active["table_id"] == table["id"]:
                return {"won": False, "kind": "busy",
                        "winner_table_id": active["table_id"],
                        "winner_table_name": active["table_name"],
                        "reason": "表仍在抽取流程中"}
            return _race_result(active)
        window = _WINDOWS.get(pipeline_id)
        now = time.monotonic()
        if window is None or now >= window.deadline:
            window = _Window(now + COALESCE_SECONDS)
            _WINDOWS[pipeline_id] = window
        # 同批内同表重复点击只算一次参与，避免自己把自己判成争抢。
        intent = next(
            (i for i in window.intents if i.table_id == table["id"]), None
        )
        if intent is None:
            _INTENT_COUNTER[0] += 1
            intent = _Intent(table["id"], table["name"], table["seq"],
                             _INTENT_COUNTER[0])
            window.intents.append(intent)

    remaining = window.deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

    with _RUNTIME_LOCK:
        if window.decision is None:
            winner = sorted(window.intents, key=lambda i: (i.seq, i.arrival))[0]
            window.decision = winner
            window.winner_intent = intent_for_table(window, winner.table_id)
            _ACTIVE[pipeline_id] = {
                "table_id": winner.table_id,
                "table_name": winner.table_name,
            }
            if _WINDOWS.get(pipeline_id) is window:
                _WINDOWS.pop(pipeline_id, None)
        winner = window.decision
        if winner.table_id != intent.table_id:
            return _race_result({
                "table_id": winner.table_id,
                "table_name": winner.table_name,
            })
        # 同一张表同批双击：只有第一个请求取得占用，其余按表占用中处理。
        if intent is not window.winner_intent:
            return {"won": False, "kind": "busy",
                    "winner_table_id": winner.table_id,
                    "winner_table_name": winner.table_name,
                    "reason": "表仍在抽取流程中"}
        return {"won": True, "winner_table_id": winner.table_id,
                "winner_table_name": winner.table_name, "kind": "won",
                "reason": ""}


def _await_winner(pipeline_id: int, winner_id: int | None) -> None:
    """落败者短暂等待批主完成，避免它在占用窗口内又抢到写事务。"""
    if winner_id is None:
        return
    deadline = time.monotonic() + LOST_WAIT_SECONDS
    while time.monotonic() < deadline:
        with _RUNTIME_LOCK:
            active = _ACTIVE.get(pipeline_id)
        if not active or active["table_id"] != winner_id:
            return
        time.sleep(0.02)


def intent_for_table(window: _Window, table_id: int) -> _Intent | None:
    for i in window.intents:
        if i.table_id == table_id:
            return i
    return None


def _release(pipeline_id: int, table_id: int | None = None) -> None:
    with _RUNTIME_LOCK:
        active = _ACTIVE.get(pipeline_id)
        if active and (table_id is None or active["table_id"] == table_id):
            _ACTIVE.pop(pipeline_id, None)


# ---------------------------------------------------------------------------
# 跳表策略守卫与单表抽取
# ---------------------------------------------------------------------------

def _policy_guard(pipeline: dict) -> None:
    if policy_mismatch(pipeline["skip_policy"], pipeline["policy_note"]):
        raise LabError(
            409,
            "跳表策略偏了：说明与当日口径不一致",
            "policy_drifted",
            {
                "skip_policy": pipeline["skip_policy"],
                "policy_note": pipeline["policy_note"],
                "policy_day": pipeline.get("policy_date") or _DAY,
            },
        )


def _write_lock(conn: sqlite3.Connection, pipeline: dict, table: dict,
                missing: list[str]) -> dict:
    reason = (
        f"上游表 {'、'.join(missing)} 未抽取完成，按跳表策略禁止抽取 "
        f"{table['name']} 并写顺序锁"
    )
    # 同一张表、同一批缺上游只保留一条顺序锁记录。
    existing = conn.execute(
        "SELECT id FROM order_locks WHERE pipeline_id = ? AND table_id = ? "
        "AND missing_upstream = ? ORDER BY id DESC LIMIT 1",
        (pipeline["id"], table["id"], "、".join(missing)),
    ).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO order_locks (pipeline_id, table_id, table_name, "
            "missing_upstream, reason, policy, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (pipeline["id"], table["id"], table["name"], "、".join(missing),
             reason, POLICY_BLOCK, _now()),
        )
    return _locks(conn, pipeline["id"])[-1]


def _skip_blocked_payload(conn: sqlite3.Connection, pipeline: dict,
                          table: dict, missing: list[str]) -> dict:
    """block 策略：写顺序锁，并返回待抛出的事务外错误。"""
    lock = _write_lock(conn, pipeline, table, missing)
    _rollup(conn, pipeline["id"])
    return {
        "status_code": 409,
        "detail": "上游表未齐，按跳表策略禁止抽取，已写顺序锁",
        "code": "order_lock",
        "data": {
            "table": table["name"],
            "policy": POLICY_BLOCK,
            "missing_upstream": missing,
            "order_lock": lock,
            "order_locks": _locks(conn, pipeline["id"]),
        },
    }


def _skip_pending_payload(conn: sqlite3.Connection, pipeline: dict,
                          table: dict, missing: list[str]) -> dict:
    """allow_pending 策略：允许抽下游，但标待对齐，禁止落库。"""
    extracted = 8 + (table["id"] % 4)
    _set_status(conn, table, STATUS_ALIGN, extracted=extracted,
                pending_since=_now())
    _rollup(conn, pipeline["id"])
    refreshed = find_table(conn, pipeline["id"], table["id"]) or table
    return {
        "ok": True,
        "table": table_view(conn, pipeline["id"], refreshed),
        "aligned": False,
        "align_pending": True,
        "missing_upstream": missing,
        "policy": policy_payload(pipeline),
        "note": "上游表未齐，已标待对齐，禁止落库",
    }


def _complete_payload(conn: sqlite3.Connection, pipeline: dict,
                      table: dict) -> dict:
    extracted = 10 + (table["id"] % 5)
    _set_status(conn, table, STATUS_EXTRACTED, extracted=extracted)
    released = _release_aligned(conn, pipeline["id"])
    _rollup(conn, pipeline["id"])
    refreshed = find_table(conn, pipeline["id"], table["id"]) or table
    return {
        "ok": True,
        "table": table_view(conn, pipeline["id"], refreshed),
        "aligned": True,
        "align_pending": False,
        "missing_upstream": [],
        "released": released,
        "policy": policy_payload(pipeline),
        "note": "抽取完成，可继续落库",
    }


def _raise_error(error: dict) -> None:
    raise LabError(
        error["status_code"],
        error["detail"],
        error["code"],
        error.get("data") or {},
    )


def extract_table(pipeline_id: int, ref, user: str = "admin") -> dict:
    """单表抽取。数据库事务只包住短读写，同刻等待不占写锁。"""
    # 1) 短事务：读取并校验当前表、占用表与跳表策略。
    race_error: dict | None = None
    with get_db() as conn:
        pipeline = _require_pipeline(conn, pipeline_id)
        _policy_guard(pipeline)
        table = find_table(conn, pipeline_id, ref)
        if not table:
            raise LabError(404, "表不存在", "table_not_found",
                           {"table": str(ref)})
        if table["status"] == STATUS_EXTRACTING:
            raise LabError(409, "表仍在抽取流程中", "table_busy",
                           {"table": table["name"]})
        if table["status"] == STATUS_ALIGN:
            raise LabError(
                409,
                "表待对齐，上游未齐前禁止重复抽取",
                "align_pending",
                {"table": table["name"],
                 "missing_upstream": _missing_upstream(
                     conn, pipeline_id, table["seq"])},
            )
        if table["status"] not in EXTRACTABLE_STATUSES:
            raise LabError(
                409,
                "表状态不允许抽取",
                "table_not_extractable",
                {"table": table["name"], "status": table["status"]},
            )
        target = dict(table)
        running = next(
            (r for r in list_tables(conn, pipeline_id)
             if r["status"] == STATUS_EXTRACTING), None
        )
        if running:
            reason = f"同刻表序争抢失败：按声明顺序 {running['name']} 在前"
            races = _record_race(conn, pipeline_id, target, running, reason)
            race_error = {
                "status_code": 409,
                "detail": f"同刻表序争抢失败：表序最前的 {running['name']} 取得本次抽取",
                "code": "order_race_lost",
                "data": {
                    "table": target["name"],
                    "won_table_name": running["name"],
                    "races": races,
                },
            }
    if race_error:
        _raise_error(race_error)

    # 2) 同刻窗口裁定：声明序最前的一张胜出。
    decision = _elect(pipeline_id, target)
    if not decision["won"]:
        if decision["kind"] == "busy":
            raise LabError(409, "表仍在抽取流程中", "table_busy",
                           {"table": target["name"]})
        _await_winner(pipeline_id, decision.get("winner_table_id"))
        with get_db() as conn:
            winner = None
            if decision.get("winner_table_id") is not None:
                winner = find_table(conn, pipeline_id,
                                    decision["winner_table_id"])
            current = find_table(conn, pipeline_id, target["id"]) or target
            if winner and current["status"] in EXTRACTABLE_STATUSES:
                _record_race(conn, pipeline_id, current, winner,
                             decision["reason"])
            races = _races(conn, pipeline_id)
        raise LabError(
            409,
            f"同刻表序争抢失败：表序最前的 "
            f"{decision['winner_table_name']} 取得本次抽取",
            "order_race_lost",
            {
                "table": target["name"],
                "won_table_name": decision["winner_table_name"],
                "races": races,
            },
        )

    try:
        # 3) 短事务：跳表策略判定，或占用抽取流程。
        skip_error: dict | None = None
        pending_result: dict | None = None
        with get_db() as conn:
            pipeline = _require_pipeline(conn, pipeline_id)
            _policy_guard(pipeline)
            table = find_table(conn, pipeline_id, target["id"])
            if not table or table["status"] not in EXTRACTABLE_STATUSES:
                raise LabError(
                    409,
                    "表状态不允许抽取",
                    "table_not_extractable",
                    {"table": target["name"]},
                )
            missing = _missing_upstream(conn, pipeline_id, table["seq"])
            if missing and pipeline["skip_policy"] == POLICY_BLOCK:
                skip_error = _skip_blocked_payload(conn, pipeline, table, missing)
            elif missing:
                pending_result = _skip_pending_payload(
                    conn, pipeline, table, missing)
            else:
                _set_status(conn, table, STATUS_EXTRACTING)
                conn.execute("UPDATE pipelines SET status = ? WHERE id = ?",
                             (STATUS_EXTRACTING, pipeline_id))
        if skip_error:
            _raise_error(skip_error)
        if pending_result:
            return pending_result

        time.sleep(HOLD_SECONDS)

        # 4) 短事务：完成抽取。
        with get_db() as conn:
            pipeline = _require_pipeline(conn, pipeline_id)
            table = find_table(conn, pipeline_id, target["id"])
            if not table:
                raise LabError(404, "表不存在", "table_not_found")
            if table["status"] != STATUS_EXTRACTING:
                raise LabError(409, "表仍在抽取流程中", "table_busy",
                               {"table": target["name"]})
            if _missing_upstream(conn, pipeline_id, table["seq"]):
                # 理论上不会发生：上游状态只会变好。
                raise LabError(409, "上游表未齐，按跳表策略禁止抽取",
                               "order_lock", {"table": table["name"]})
            return _complete_payload(conn, pipeline, table)
    finally:
        _release(pipeline_id, target["id"])


def load_table(pipeline_id: int, ref, user: str = "admin") -> dict:
    with get_db() as conn:
        _require_pipeline(conn, pipeline_id)
        table = find_table(conn, pipeline_id, ref)
        if not table:
            raise LabError(404, "表不存在", "table_not_found",
                           {"table": str(ref)})
        if table["status"] == STATUS_ALIGN:
            raise LabError(
                409,
                "表待对齐，上游未齐前禁止落库",
                "align_load_blocked",
                {"table": table["name"],
                 "missing_upstream": _missing_upstream(
                     conn, pipeline_id, table["seq"])},
            )
        if table["status"] != STATUS_EXTRACTED:
            raise LabError(
                409,
                "表状态不允许落库",
                "table_not_loadable",
                {"table": table["name"], "status": table["status"]},
            )
        _set_status(conn, table, STATUS_LOADED)
        released = _release_aligned(conn, pipeline_id)
        _rollup(conn, pipeline_id)
        refreshed = find_table(conn, pipeline_id, table["id"]) or table
        result = {
            "ok": True,
            "table": table_view(conn, pipeline_id, refreshed),
            "released": released,
            "pipeline": get_pipeline(conn, pipeline_id),
        }
    return result


# ---------------------------------------------------------------------------
# 整轮操作：按声明顺序流表（兼容基线端点）
# ---------------------------------------------------------------------------

def bulk_extract(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    pipeline = _require_pipeline(conn, pipeline_id)
    _policy_guard(pipeline)
    running = next(
        (r for r in list_tables(conn, pipeline_id)
         if r["status"] == STATUS_EXTRACTING), None
    )
    if running:
        raise LabError(
            409,
            f"同刻表序争抢失败：表序最前的 {running['name']} 正在抽取",
            "order_race_lost",
            {"won_table_name": running["name"],
             "races": _races(conn, pipeline_id)},
        )
    for table in list_tables(conn, pipeline_id):
        if table["status"] not in EXTRACTABLE_STATUSES:
            continue
        missing = _missing_upstream(conn, pipeline_id, table["seq"])
        if missing:
            if pipeline["skip_policy"] == POLICY_BLOCK:
                lock = _write_lock(conn, pipeline, table, missing)
                _rollup(conn, pipeline_id)
                raise LabError(
                    409,
                    "上游表未齐，按跳表策略禁止抽取，已写顺序锁",
                    "order_lock",
                    {
                        "table": table["name"],
                        "policy": POLICY_BLOCK,
                        "missing_upstream": missing,
                        "order_lock": lock,
                    },
                )
            extracted = 8 + (table["id"] % 4)
            _set_status(conn, table, STATUS_ALIGN, extracted=extracted,
                        pending_since=_now())
            continue
        extracted = 10 + (table["id"] % 5)
        _set_status(conn, table, STATUS_EXTRACTED, extracted=extracted)
        _release_aligned(conn, pipeline_id)
    return _rollup(conn, pipeline_id)


def bulk_load(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    _require_pipeline(conn, pipeline_id)
    for table in list_tables(conn, pipeline_id):
        if table["status"] == STATUS_EXTRACTED:
            _set_status(conn, table, STATUS_LOADED)
            _release_aligned(conn, pipeline_id)
    return _rollup(conn, pipeline_id)


def _record_race(conn: sqlite3.Connection, pipeline_id: int, target: dict,
                 winner: dict, reason: str) -> list[dict]:
    existing = conn.execute(
        "SELECT id FROM table_races WHERE pipeline_id = ? AND table_id = ? "
        "AND won_table_id = ? AND reason = ?",
        (pipeline_id, target["id"], winner["id"], reason),
    ).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO table_races (pipeline_id, table_id, table_name, "
            "won_table_id, won_table_name, reason, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (pipeline_id, target["id"], target["name"], winner["id"],
             winner["name"], reason, _now()),
        )
    return _races(conn, pipeline_id)
