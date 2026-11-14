import sqlite3
from contextlib import contextmanager
from pathlib import Path

from app.config import Config

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]

# 同源多表：同一管道内的表按声明顺序抽取，序号由服务端裁定。
SEED_TABLES = {
    "orders_daily": ["ods_orders_raw", "dwd_orders_detail", "dws_orders_day"],
    "user_profile": ["ods_users_raw", "dwd_users_profile", "dws_users_tag"],
    "inventory_snap": ["ods_stock_raw", "dwd_stock_snap"],
    "pay_clean": ["ods_pay_raw", "dwd_pay_clean", "dws_pay_flow"],
}

STATUS_PENDING = "待抽取"
STATUS_EXTRACTING = "抽取中"
STATUS_EXTRACTED = "已抽取待加载"
STATUS_ALIGN = "待对齐"
STATUS_LOADED = "已落库"
STATUS_FAILED = "抽取失败"

# 「上游齐」指上游表已完成抽取（已抽取待加载 / 已落库）；待抽取/待对齐不算齐。
READY_STATUSES = (STATUS_EXTRACTED, STATUS_LOADED)
# 仍在抽取流程里：抽取中、已抽取待加载、待对齐。此时禁止改表顺序。
IN_FLOW_STATUSES = (STATUS_EXTRACTING, STATUS_EXTRACTED, STATUS_ALIGN)
EXTRACTABLE_STATUSES = (STATUS_PENDING, STATUS_FAILED)

POLICY_BLOCK = "block"
POLICY_ALLOW = "allow_pending"

POLICY_TITLES = {
    POLICY_BLOCK: "禁止跳表 · 顺序锁",
    POLICY_ALLOW: "允许跳表 · 待对齐",
}
POLICY_LABELS = {
    POLICY_BLOCK: "上游表未齐时禁止抽下游，并写顺序锁",
    POLICY_ALLOW: "上游表未齐时允许抽下游，标待对齐且禁止落库",
}
# 服务端写入说明的当日口径，也是判定「说明与策略不一致」的基准。
POLICY_DESCRIPTIONS = {
    POLICY_BLOCK: "上游表未齐时禁止抽取下游表，并写顺序锁；待上游表全部抽取完成后才可继续。",
    POLICY_ALLOW: "上游表未齐时允许抽取下游表，但标记待对齐并禁止落库；待上游表全部抽取完成后对齐。",
}

# 说明里必须出现的口径关键词；缺任一即「跳表策略偏了」。
POLICY_KEYWORDS = {
    POLICY_BLOCK: ("禁止", "顺序锁"),
    POLICY_ALLOW: ("允许", "待对齐"),
}


class LabError(Exception):
    """业务错误：统一映射为带 code 的 JSON 响应。"""

    def __init__(self, status_code: int, detail: str, code: str = "error",
                 payload: dict | None = None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.code = code
        self.payload = payload or {}


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=15,
                           isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 15000")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    """每一段数据库操作都是短事务：BEGIN IMMEDIATE 保证写串行。"""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        if conn.in_transaction:
            conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise
    finally:
        conn.close()


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS pipelines (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      code TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL,
      source_uri TEXT NOT NULL,
      extracted INTEGER NOT NULL DEFAULT 0,
      loaded INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      skip_policy TEXT NOT NULL DEFAULT 'block',
      policy_note TEXT NOT NULL DEFAULT '',
      policy_date TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_tables (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pipeline_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      seq INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT '待抽取',
      extracted INTEGER NOT NULL DEFAULT 0,
      loaded INTEGER NOT NULL DEFAULT 0,
      pending_since TEXT,
      extracted_at TEXT,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
      UNIQUE (pipeline_id, name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_locks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pipeline_id INTEGER NOT NULL,
      table_id INTEGER NOT NULL,
      table_name TEXT NOT NULL,
      missing_upstream TEXT NOT NULL,
      reason TEXT NOT NULL,
      policy TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS table_races (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pipeline_id INTEGER NOT NULL,
      table_id INTEGER NOT NULL,
      table_name TEXT NOT NULL,
      won_table_id INTEGER NOT NULL,
      won_table_name TEXT NOT NULL,
      reason TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
]


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _ensure_pipeline_columns(conn: sqlite3.Connection) -> None:
    """兼容旧库：补上跳表策略相关列。"""
    additions = {
        "skip_policy": "TEXT NOT NULL DEFAULT 'block'",
        "policy_note": "TEXT NOT NULL DEFAULT ''",
        "policy_date": "TEXT",
    }
    cols = _column_names(conn, "pipelines")
    for name, ddl in additions.items():
        if name not in cols:
            conn.execute(f"ALTER TABLE pipelines ADD COLUMN {name} {ddl}")


def _seed_tables(conn: sqlite3.Connection, pipeline_id: int, code: str) -> None:
    names = SEED_TABLES.get(code, [f"{code}_main"])
    conn.executemany(
        "INSERT OR IGNORE INTO pipeline_tables (pipeline_id, name, seq, status) "
        "VALUES (?,?,?,?)",
        [(pipeline_id, name, idx + 1, STATUS_PENDING)
         for idx, name in enumerate(names)],
    )


def init_db(db_path: str | None = None) -> None:
    with get_db(db_path) as conn:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        _ensure_pipeline_columns(conn)
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('lab_name', ?)",
            (Config.LAB_NAME,),
        )
        n = conn.execute("SELECT COUNT(*) AS c FROM pipelines").fetchone()["c"]
        if n == 0:
            for title, code, status, source_uri in SEED_PIPELINES:
                cur = conn.execute(
                    "INSERT INTO pipelines (title, code, status, source_uri, "
                    "skip_policy, policy_note) VALUES (?,?,?,?,?,?)",
                    (title, code, status, source_uri, POLICY_BLOCK,
                     POLICY_DESCRIPTIONS[POLICY_BLOCK]),
                )
                _seed_tables(conn, int(cur.lastrowid), code)
        else:
            for row in conn.execute("SELECT id, code FROM pipelines").fetchall():
                has = conn.execute(
                    "SELECT COUNT(*) AS c FROM pipeline_tables WHERE pipeline_id = ?",
                    (row["id"],),
                ).fetchone()["c"]
                if has == 0:
                    _seed_tables(conn, row["id"], row["code"])
            for row in conn.execute(
                "SELECT id FROM pipelines WHERE policy_note IS NULL OR policy_note = ''"
            ).fetchall():
                conn.execute(
                    "UPDATE pipelines SET policy_note = ? WHERE id = ?",
                    (POLICY_DESCRIPTIONS[POLICY_BLOCK], row["id"]),
                )


PIPELINE_COLUMNS = (
    "id, title, code, status, source_uri, extracted, loaded, created_at, "
    "skip_policy, policy_note, policy_date"
)


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        f"SELECT {PIPELINE_COLUMNS} FROM pipelines ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        f"SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE id = ?",
        (pipeline_id,),
    ).fetchone()
    return dict(row) if row else None


def get_pipeline_by_code(conn: sqlite3.Connection, code: str) -> dict | None:
    row = conn.execute(
        f"SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE code = ?",
        (code,),
    ).fetchone()
    return dict(row) if row else None
