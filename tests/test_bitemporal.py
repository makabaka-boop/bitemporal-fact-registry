"""Sequential corrections + retroactive queries, checked against the naive
history-snapshot oracle point by point."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import pytest

from tests.naive_oracle import NaiveBitemporalObject

UTC = timezone.utc
T0 = datetime(2026, 9, 21, 0, 0, 0, tzinfo=UTC)  # a Monday ("last week")


def hour(n: int) -> datetime:
    return T0 + timedelta(hours=n)


@pytest.fixture()
def conn(clean_db):
    c = psycopg2.connect(clean_db)
    yield c
    c.close()


def apply(conn, o, obj, vf, vt, value, expected):
    """Run a correction in both the DB and the oracle."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT correct_fact(%s,%s,%s,%s::jsonb,%s)",
            (obj, vf, vt, psycopg2.extras.Json(value), expected),
        )
        db_rev = cur.fetchone()[0]
    conn.commit()
    oracle_rev = o.correct(vf, vt, value, expected)
    assert db_rev == oracle_rev
    return db_rev


def db_view(conn, obj, rev):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT valid_from, valid_to, value
              FROM fact_versions
             WHERE object_id = %s
               AND assert_from <= %s
               AND (assert_to IS NULL OR %s < assert_to)
             ORDER BY valid_from
            """,
            (obj, rev, rev),
        )
        return [(vf, vt, val) for vf, vt, val in cur.fetchall()]


def oracle_view(o, rev):
    import json

    return [(vf, vt, value)
            for vf, vt, value in o.known_view(rev)]


def assert_views_match(conn, obj, o):
    for rev in range(o.head + 1):
        assert db_view(conn, obj, rev) == oracle_view(o, rev), (
            f"view mismatch at rev {rev}"
        )


def test_first_insert_then_split_and_overwrite(conn):
    obj = "lab-result-A"
    o = NaiveBitemporalObject()

    # Initial broad fact known at revision 1: value 10 over hours [0,24).
    apply(conn, o, obj, hour(0), hour(24), {"result": 10, "unit": "mg"}, 0)
    assert_views_match(conn, obj, o)

    # Correction today back-dated into hours [6,9): value 12. This splits the
    # original broad segment into [0,6) and [9,24) remnants.
    apply(conn, o, obj, hour(6), hour(9), {"result": 12, "unit": "mg"}, 1)
    assert_views_match(conn, obj, o)

    # Overwrite part of the corrected interval [7,8): 12 -> 15.
    apply(conn, o, obj, hour(7), hour(8), {"result": 15, "unit": "mg"}, 2)
    assert_views_match(conn, obj, o)

    # Cover the whole known world [0,24) with one new fact.
    apply(conn, o, obj, hour(0), hour(24), {"result": 99, "unit": "mg"}, 3)
    assert_views_match(conn, obj, o)

    # Extend into previously-unknown future [24,30).
    apply(conn, o, obj, hour(24), hour(30), {"result": 5, "unit": "mg"}, 4)
    assert_views_match(conn, obj, o)


def test_retroactive_queries_never_change(conn):
    """What the system KNEW at each revision stays frozen forever."""
    obj = "lab-result-B"
    o = NaiveBitemporalObject()

    apply(conn, o, obj, hour(0), hour(24), {"result": 10}, 0)
    apply(conn, o, obj, hour(10), hour(12), {"result": 20}, 1)
    apply(conn, o, obj, hour(11), hour(12), {"result": 30}, 2)
    apply(conn, o, obj, hour(0), hour(24), {"result": 40}, 3)
    assert_views_match(conn, obj, o)

    # Point-by-point comparison over a dense grid and every revision.
    probes = [hour(0) + timedelta(minutes=15 * k) for k in range(24 * 4 + 4)]
    for rev in range(o.head + 1):
        for at in probes:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT value FROM fact_versions
                     WHERE object_id=%s
                       AND assert_from <= %s AND (assert_to IS NULL OR %s < assert_to)
                       AND %s >= valid_from AND %s < valid_to
                    """,
                    (obj, rev, rev, at, at),
                )
                rows = cur.fetchall()
            assert len(rows) <= 1
            db_val = rows[0][0] if rows else None
            ora_seg = o.fact_at(at, rev)
            ora_val = ora_seg[2] if ora_seg else None
            assert db_val == ora_val, (rev, at, db_val, ora_val)

    # Explicit historical narrative at valid time 11:30:
    # rev 1: broad [0,24)=10; rev 2: correction [10,12)=20;
    # rev 3: [11,12) refined to 30; rev 4: whole [0,24) overwritten -> 40.
    # None of the later corrections erased what earlier revisions knew.
    probe = hour(11) + timedelta(minutes=30)

    def known(at, rev):
        seg = o.fact_at(at, rev)
        return seg[2]["result"] if seg else None

    assert known(probe, 1) == 10
    assert known(probe, 2) == 20
    assert known(probe, 3) == 30
    assert known(probe, 4) == 40
    # And at valid time 10:30: rev1 10, rev2 20, rev3 still 20, rev4 40.
    probe2 = hour(10) + timedelta(minutes=30)
    assert known(probe2, 1) == 10
    assert known(probe2, 2) == 20
    assert known(probe2, 3) == 20
    assert known(probe2, 4) == 40
    # Before anything was known there is an explicit emptiness.
    assert o.fact_at(hour(11), 0) is None


def test_explicit_gap_is_emptiness(conn):
    obj = "lab-result-C"
    o = NaiveBitemporalObject()
    apply(conn, o, obj, hour(0), hour(4), {"result": 1}, 0)
    apply(conn, o, obj, hour(8), hour(12), {"result": 2}, 1)
    assert_views_match(conn, obj, o)
    assert o.fact_at(hour(6), o.head) is None

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM fact_versions
             WHERE object_id=%s AND assert_to IS NULL
               AND %s >= valid_from AND %s < valid_to
            """,
            (obj, hour(6), hour(6)),
        )
        assert cur.fetchone()[0] == 0


def test_stale_expected_revision_rejected(conn):
    obj = "lab-result-D"
    o = NaiveBitemporalObject()
    apply(conn, o, obj, hour(0), hour(24), {"result": 1}, 0)
    with pytest.raises(psycopg2.Error) as ei:
        apply(conn, o, obj, hour(0), hour(1), {"result": 2}, 0)  # stale!
    conn.rollback()
    assert ei.value.pgcode == "99001"


def test_empty_and_reversed_intervals_rejected(conn):
    obj = "lab-result-E"
    o = NaiveBitemporalObject()
    with pytest.raises(psycopg2.Error) as ei:
        apply(conn, o, obj, hour(5), hour(5), {"result": 1}, 0)
    conn.rollback()
    assert ei.value.pgcode == "99002"
    with pytest.raises(psycopg2.Error) as ei:
        apply(conn, o, obj, hour(9), hour(3), {"result": 1}, 0)
    conn.rollback()
    assert ei.value.pgcode == "99002"
    # Both attempts aborted their transaction: nothing was created — no
    # half-split, not even an object row.
    with conn.cursor() as cur:
        cur.execute("SELECT head_revision FROM objects WHERE object_id=%s", (obj,))
        assert cur.fetchone() is None
        cur.execute("SELECT count(*) FROM fact_versions WHERE object_id=%s", (obj,))
        assert cur.fetchone()[0] == 0
