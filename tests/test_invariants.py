"""Structural guarantees:

1. No overlapping valid intervals inside ANY historical view.
2. History rows are immutable (no delete, no column rewrite); a correction
   today cannot mutate what was visible last week.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import pytest

from tests.test_bitemporal import apply, db_view, oracle_view  # noqa: F401
from tests.naive_oracle import NaiveBitemporalObject

UTC = timezone.utc
T0 = datetime(2026, 9, 21, tzinfo=UTC)


def hour(n):
    return T0 + timedelta(hours=n)


@pytest.fixture()
def conn(clean_db):
    c = psycopg2.connect(clean_db)
    yield c
    c.close()


def test_no_overlap_in_every_historical_view(conn):
    obj = "inv"
    o = NaiveBitemporalObject()
    # Chaotic mix: nested, adjacent, spanning and back-to-back corrections.
    ops = [
        (0, 24, 1), (6, 18, 2), (9, 10, 3), (0, 24, 4),
        (3, 6, 5), (6, 9, 6), (18, 20, 7), (2, 22, 8),
        (0, 1, 9), (23, 24, 10), (0, 24, 11),
    ]
    for i, (f, t, v) in enumerate(ops):
        apply(conn, o, obj, hour(f), hour(t), {"v": v}, i)

    head = len(ops)
    for rev in range(head + 1):
        with conn.cursor() as cur:
            # GiST can only index the live set, so check every past view by
            # asking the database itself to detect overlaps.
            cur.execute(
                """
                WITH view_at AS (
                    SELECT tstzrange(valid_from, valid_to, '[)') AS r
                      FROM fact_versions
                     WHERE object_id=%s
                       AND assert_from <= %s
                       AND (assert_to IS NULL OR %s < assert_to)
                )
                SELECT count(*) FROM view_at a JOIN view_at b
                  ON a.r && b.r AND a.r <> b.r
                """,
                (obj, rev, rev),
            )
            assert cur.fetchone()[0] == 0, f"overlap in view at rev {rev}"


def test_rows_cannot_be_deleted(conn):
    obj = "del"
    o = NaiveBitemporalObject()
    apply(conn, o, obj, hour(0), hour(24), {"v": 1}, 0)
    apply(conn, o, obj, hour(6), hour(9), {"v": 2}, 1)
    with pytest.raises(psycopg2.Error) as ei:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fact_versions WHERE object_id=%s", (obj,))
    conn.rollback()
    assert ei.value.pgcode == "99010"
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fact_versions WHERE object_id=%s",
                    (obj,))
        # rev 1 inserted 1 row; rev 2 closed that row and inserted left/new/
        # right = 3 rows -> 4 total, none deleted.
        assert cur.fetchone()[0] == 4


def test_columns_other_than_assert_to_cannot_change(conn):
    obj = "upd"
    o = NaiveBitemporalObject()
    apply(conn, o, obj, hour(0), hour(24), {"v": 1}, 0)
    apply(conn, o, obj, hour(6), hour(9), {"v": 2}, 1)

    attempts = [
        "UPDATE fact_versions SET value='{\"v\":999}' WHERE object_id=%s",
        "UPDATE fact_versions SET valid_from=valid_from WHERE object_id=%s",
        "UPDATE fact_versions SET assert_from=assert_from+1 WHERE object_id=%s",
    ]
    for sql in attempts:
        with pytest.raises(psycopg2.Error) as ei:
            with conn.cursor() as cur:
                cur.execute(sql, (obj,))
        conn.rollback()
        assert ei.value.pgcode == "99010", sql

    # Double-closing an already closed row is also forbidden.
    with pytest.raises(psycopg2.Error) as ei:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE fact_versions SET assert_to=99 WHERE assert_to IS NOT NULL"
            )
    conn.rollback()
    assert ei.value.pgcode == "99010"


def test_history_is_append_only_even_after_later_corrections(conn):
    """Bytes of the rev-1 view remain identical after rev-2..N corrections."""
    obj = "freeze"
    o = NaiveBitemporalObject()
    apply(conn, o, obj, hour(0), hour(24), {"v": "first"}, 0)

    snap_rev1 = db_view(conn, obj, 1)
    apply(conn, o, obj, hour(6), hour(9), {"v": "second"}, 1)
    apply(conn, o, obj, hour(0), hour(24), {"v": "third"}, 2)
    apply(conn, o, obj, hour(20), hour(21), {"v": "fourth"}, 3)
    assert db_view(conn, obj, 1) == snap_rev1
    assert db_view(conn, obj, 1) == oracle_view(o, 1)
