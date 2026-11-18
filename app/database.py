"""SQLite 持久化层。

- cells：导入的调查格值（导入后不可更换，无更新/删除接口）。
- publications：已获准公开的汇总记录（不可撤销，只插入，无更新/删除接口）。

跨进程一致性：
- 数据库以 WAL 模式运行，读不阻塞写；
- 发布确认使用 ``BEGIN IMMEDIATE`` 显式写事务：先取得数据库级写锁，
  再重读最新公开记录；只有在该事务内重核通过才插入并提交。
  因此多个服务进程（各自独立连接）不会同时通过"各自安全、合并后不安全"
  的检查——后取得写锁的进程一定能看到先提交的记录。
- 任一步骤失败都显式 ROLLBACK，不会留下半次发布；进程崩溃时 SQLite
  也会回滚未提交事务。

连接按操作创建，autocommit（isolation_level=None），事务边界全部显式控制。
"""

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "app.db"
DB_PATH = os.environ.get("DB_PATH", str(_DEFAULT_DB))

BUSY_TIMEOUT_MS = 30_000

_SCHEMA_STATEMENTS = (
    """\
CREATE TABLE IF NOT EXISTS cells (
    district   TEXT NOT NULL,
    age_group  TEXT NOT NULL,
    population INTEGER NOT NULL CHECK (population >= 5),
    detected   INTEGER NOT NULL CHECK (detected >= 0 AND detected <= population),
    PRIMARY KEY (district, age_group)
)""",
    """\
CREATE TABLE IF NOT EXISTS publications (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    districts  TEXT NOT NULL,          -- JSON 数组
    age_groups TEXT NOT NULL,          -- JSON 数组
    total      INTEGER NOT NULL CHECK (total >= 0),
    created_at TEXT NOT NULL           -- ISO8601 UTC
)""",
)


class AlreadyImported(Exception):
    """调查数据已存在（跨进程并发导入时由后到者抛出）。"""


class DatabaseBusy(Exception):
    """等待数据库写锁超时（另一进程长时间持有写事务）。调用方可提示重试。"""


def connect() -> sqlite3.Connection:
    path = DB_PATH
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None：关闭 sqlite3 模块的隐式事务，由本模块显式 BEGIN/COMMIT。
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    return conn


def _begin_immediate(conn: sqlite3.Connection) -> None:
    """取得数据库级写锁；其他进程/连接的写事务会等待（受 busy_timeout 限制）。

    等待超时（极端长时间持锁）时抛 DatabaseBusy，便于接口层给出 503/409
    而非被误当作"保存失败"。
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as e:
        if "locked" in str(e).lower() or "busy" in str(e).lower():
            raise DatabaseBusy(str(e)) from e
        raise


def _rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def init_db() -> None:
    """建表（幂等）。两台实例可能同时启动：以 BEGIN IMMEDIATE 串行化，
    避免并发 DDL 互相竞争。"""
    conn = connect()
    try:
        try:
            _begin_immediate(conn)
            for stmt in _SCHEMA_STATEMENTS:
                conn.execute(stmt)
            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise
    finally:
        conn.close()
    # WAL 是库级持久设置；单独设置以避免与事务语义混淆。
    with closing(connect()) as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")


# ---------------- 调查格值 ----------------


def get_cells() -> list[dict]:
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT district, age_group, population, detected FROM cells "
            "ORDER BY district, age_group"
        ).fetchall()
    return [dict(r) for r in rows]


def insert_cells_once(rows: list[dict]) -> None:
    """单事务写入全部格值；已有数据则抛 AlreadyImported，不留部分写入。

    检查与写入在同一个 ``BEGIN IMMEDIATE`` 事务内完成，两个进程同时导入时
    也只有一个能成功。
    """
    conn = connect()
    try:
        _begin_immediate(conn)
        try:
            if conn.execute("SELECT 1 FROM cells LIMIT 1").fetchone() is not None:
                raise AlreadyImported()
            conn.executemany(
                "INSERT INTO cells (district, age_group, population, detected) "
                "VALUES (:district, :age_group, :population, :detected)",
                rows,
            )
            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise
    finally:
        conn.close()


# ---------------- 公开记录 ----------------


def _read_publications(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, districts, age_groups, total, created_at "
        "FROM publications ORDER BY id"
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["districts"] = json.loads(d["districts"])
        d["age_groups"] = json.loads(d["age_groups"])
        out.append(d)
    return out


def get_publications() -> list[dict]:
    with closing(connect()) as conn:
        return _read_publications(conn)


def get_public_snapshot() -> tuple[list[dict], list[dict]]:
    """在同一个只读事务中读取人数与公开记录，得到一致快照。

    WAL 模式下，即使另一进程正持有写事务，这里也不会被阻塞。
    """
    conn = connect()
    try:
        conn.execute("BEGIN")
        try:
            cells = [
                {k: r[k] for k in ("district", "age_group", "population")}
                for r in conn.execute(
                    "SELECT district, age_group, population FROM cells "
                    "ORDER BY district, age_group"
                ).fetchall()
            ]
            pubs = _read_publications(conn)
            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise
    finally:
        conn.close()
    return cells, pubs


def _insert_publication(
    conn: sqlite3.Connection, districts: list[str], age_groups: list[str], total: int
) -> dict:
    created = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur = conn.execute(
        "INSERT INTO publications (districts, age_groups, total, created_at) "
        "VALUES (?, ?, ?, ?)",
        (
            json.dumps(list(districts), ensure_ascii=False),
            json.dumps(list(age_groups), ensure_ascii=False),
            int(total),
            created,
        ),
    )
    return {
        "id": cur.lastrowid,
        "districts": list(districts),
        "age_groups": list(age_groups),
        "total": int(total),
        "created_at": created,
    }


def insert_publication(districts: list[str], age_groups: list[str], total: int) -> dict:
    """单事务插入一条公开记录；失败回滚，不会留下半次发布。"""
    conn = connect()
    try:
        _begin_immediate(conn)
        try:
            pub = _insert_publication(conn, districts, age_groups, total)
            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise
    finally:
        conn.close()
    return pub


def publish_guarded(
    districts: list[str],
    age_groups: list[str],
    total: int,
    evaluate_fn,
) -> dict:
    """跨进程原子发布：写锁内重读最新公开记录、重核、写入。

    ``evaluate_fn(publications)`` 返回 solver 的评估结果
    ``{"safe": bool, "affected": [...]}``。

    返回 ``{"status": "published", "publication": {...}}`` 或
    ``{"status": "unsafe", "evaluation": {...}}``；异常向上抛出（事务已回滚）。
    """
    conn = connect()
    try:
        _begin_immediate(conn)  # 立即取写锁：写入临界区跨进程串行
        try:
            latest = _read_publications(conn)
            evaluation = evaluate_fn(latest)
            if not evaluation.get("safe"):
                # 未通过：只在内存中判定，不写入任何内容。
                _rollback(conn)
                return {"status": "unsafe", "evaluation": evaluation}
            pub = _insert_publication(conn, districts, age_groups, total)
            conn.execute("COMMIT")
            return {"status": "published", "publication": pub}
        except BaseException:
            _rollback(conn)
            raise
    finally:
        conn.close()
