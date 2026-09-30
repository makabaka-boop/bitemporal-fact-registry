"""Bitemporal fact service.

Facts carry a valid-time half-open interval [valid_from, valid_to) and live on
a second, database-assigned revision axis (known time). Corrections split the
current timeline inside one transaction; historical rows are immutable, so the
state visible at any past ``knownAtRevision`` is always reconstructible.
"""
from __future__ import annotations

import os
import re
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://bitemporal@localhost:55432/bitemporal?host=/tmp/pgsock",
)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")

_pool: psycopg2.pool.SimpleConnectionPool | None = None

# SQLSTATEs raised by schema.sql -> HTTP statuses
_REVISION_CONFLICT = "99001"
_BAD_INTERVAL = "99002"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_pool() -> psycopg2.pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=20, dsn=DSN
        )
    return _pool


@contextmanager
def db_conn():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def _split_sql(sql: str) -> list[str]:
    """Split a DDL script on top-level semicolons.

    psycopg2's simple-query protocol mis-splits several dollar-quoted
    CREATE FUNCTION bodies sent in one execute(). Split ourselves with a small
    state machine that skips comments and respects single-quoted strings and
    dollar-quoted bodies ($$ ... $$ or $tag$ ... $tag$).
    """
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)

    def emit() -> None:
        stmt = "".join(buf).strip()
        # Drop purely-comment / whitespace fragments.
        if stmt and not all(
            not line.strip() or line.strip().startswith("--")
            for line in stmt.splitlines()
        ):
            statements.append(stmt)

    while i < n:
        ch = sql[i]
        # line comment: consume the whole line, do not interpret quotes in it
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j])
            i = j
            continue
        # block comment
        if sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth += 1
                    j += 2
                elif sql.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            buf.append(sql[i:j])
            i = j
            continue
        # single-quoted string with '' escapes
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            buf.append(sql[i:j])
            i = j
            continue
        # dollar quote: $tag$ ... $tag$
        if ch == "$":
            m = re.match(r"\$[A-Za-z_0-9]*\$", sql[i:])
            if m:
                tag = m.group(0)
                start = i
                i += len(tag)
                end = sql.find(tag, i)
                if end == -1:
                    i = n
                else:
                    i = end + len(tag)
                buf.append(sql[start:i])
                continue
        if ch == ";":
            emit()
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    emit()
    return statements


def init_db() -> None:
    with open(SCHEMA_PATH, encoding="utf-8") as fh:
        ddl = fh.read()
    with db_conn() as conn:
        try:
            with conn.cursor() as cur:
                for stmt in _split_sql(ddl):
                    cur.execute(stmt)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def reset_pool() -> None:
    """Close all pooled connections (used after tests recreate the schema)."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------


class Correction(BaseModel):
    object_id: str = Field(..., min_length=1)
    valid_from: datetime
    valid_to: datetime
    value: Any
    expected_revision: int = Field(
        ..., ge=0,
        description="Head revision the caller believes it is correcting",
    )

    @field_validator("valid_from", "valid_to")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        return v


class CorrectionResult(BaseModel):
    object_id: str
    revision: int


class FactView(BaseModel):
    object_id: str
    valid_at: datetime
    known_at_revision: int
    found: bool
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    value: Optional[Any] = None


class TimelineRow(BaseModel):
    valid_from: datetime
    valid_to: datetime
    value: Any
    assert_from: int
    assert_to: Optional[int] = None


class Head(BaseModel):
    object_id: str
    head_revision: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield
    reset_pool()


app = FastAPI(title="Bitemporal facts", version="1.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.post("/objects/{object_id}", response_model=Head, status_code=201)
def create_object(object_id: str):
    with db_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO objects(object_id) VALUES (%s) "
                    "ON CONFLICT (object_id) "
                    "DO UPDATE SET object_id = EXCLUDED.object_id "
                    "RETURNING object_id, head_revision",
                    (object_id,),
                )
                row = cur.fetchone()
            conn.commit()
        except psycopg2.Error:
            conn.rollback()
            raise
    return Head(object_id=row[0], head_revision=row[1])


@app.get("/objects/{object_id}", response_model=Head)
def get_head(object_id: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT object_id, head_revision FROM objects WHERE object_id = %s",
            (object_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="unknown object")
    return Head(object_id=row[0], head_revision=row[1])


@app.post("/corrections", response_model=CorrectionResult, status_code=201)
def correct(c: Correction):
    if not c.valid_from < c.valid_to:
        raise HTTPException(
            status_code=422,
            detail="valid interval must be non-empty half-open [valid_from, valid_to)",
        )
    with db_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT correct_fact(%s, %s, %s, %s::jsonb, %s)",
                    (
                        c.object_id,
                        c.valid_from,
                        c.valid_to,
                        psycopg2.extras.Json(c.value),
                        c.expected_revision,
                    ),
                )
                new_rev = cur.fetchone()[0]
            conn.commit()
        except psycopg2.Error as exc:
            conn.rollback()
            if exc.pgcode == _REVISION_CONFLICT:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if exc.pgcode == _BAD_INTERVAL:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return CorrectionResult(object_id=c.object_id, revision=new_rev)


@app.get("/objects/{object_id}/fact", response_model=FactView)
def fact_at(
    object_id: str,
    validAt: datetime,
    knownAtRevision: int,
):
    if validAt.tzinfo is None:
        raise HTTPException(status_code=422, detail="validAt must be timezone-aware")
    if knownAtRevision < 0:
        raise HTTPException(status_code=422, detail="knownAtRevision must be >= 0")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT head_revision FROM objects WHERE object_id = %s",
                    (object_id,))
        head_row = cur.fetchone()
        if head_row is None:
            raise HTTPException(status_code=404, detail="unknown object")
        head = head_row[0]
        if knownAtRevision > head:
            raise HTTPException(
                status_code=422,
                detail=f"knownAtRevision {knownAtRevision} is in the future "
                       f"(head is {head})",
            )

        cur.execute(
            """
            SELECT valid_from, valid_to, value, assert_from, assert_to
              FROM fact_versions
             WHERE object_id = %s
               AND assert_from <= %s
               AND (assert_to IS NULL OR %s < assert_to)
               AND %s >= valid_from
               AND %s <  valid_to
            """,
            (object_id, knownAtRevision, knownAtRevision, validAt, validAt),
        )
        rows = cur.fetchall()

    # Uniqueness is guaranteed structurally (half-open + exclusion invariant),
    # but defend explicitly so a bug can never silently return two truths.
    if len(rows) > 1:
        raise HTTPException(
            status_code=500,
            detail=f"invariant violated: {len(rows)} facts at one position",
        )
    if not rows:
        return FactView(
            object_id=object_id,
            valid_at=validAt,
            known_at_revision=knownAtRevision,
            found=False,
        )
    vf, vt, value, af, at = rows[0]
    return FactView(
        object_id=object_id,
        valid_at=validAt,
        known_at_revision=knownAtRevision,
        found=True,
        valid_from=vf,
        valid_to=vt,
        value=value,
    )


@app.get("/objects/{object_id}/timeline", response_model=list[TimelineRow])
def timeline(object_id: str, knownAtRevision: int):
    """Full historical view as of one revision (naive-snapshot oracle aid)."""
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT head_revision FROM objects WHERE object_id = %s",
                    (object_id,))
        head_row = cur.fetchone()
        if head_row is None:
            raise HTTPException(status_code=404, detail="unknown object")
        if knownAtRevision < 0 or knownAtRevision > head_row[0]:
            raise HTTPException(status_code=422, detail="revision out of range")
        cur.execute(
            """
            SELECT valid_from, valid_to, value, assert_from, assert_to
              FROM fact_versions
             WHERE object_id = %s
               AND assert_from <= %s
               AND (assert_to IS NULL OR %s < assert_to)
             ORDER BY valid_from
            """,
            (object_id, knownAtRevision, knownAtRevision),
        )
        rows = cur.fetchall()
    return [
        TimelineRow(
            valid_from=r[0], valid_to=r[1], value=r[2],
            assert_from=r[3], assert_to=r[4],
        )
        for r in rows
    ]


@app.get("/health")
def health():
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    return {"status": "ok", "time": _utcnow()}
