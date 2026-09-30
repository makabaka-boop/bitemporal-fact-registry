"""HTTP-level tests against a real uvicorn server, including racy commits."""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import uvicorn

from tests.naive_oracle import NaiveBitemporalObject, RevisionConflict

UTC = timezone.utc
T0 = datetime(2026, 9, 21, tzinfo=UTC)


def hour(n):
    return T0 + timedelta(hours=n)


@pytest.fixture(scope="module")
def server(clean_db):
    import app.main as main

    config = uvicorn.Config(main.app, host="127.0.0.1", port=55801,
                            log_level="error")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    for _ in range(100):
        try:
            r = httpx.get("http://127.0.0.1:55801/health", timeout=1)
            if r.status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)
    else:
        raise RuntimeError("server never came up")
    yield "http://127.0.0.1:55801"
    srv.should_exit = True
    thread.join(timeout=5)


@pytest.fixture()
def client(server):
    with httpx.Client(base_url=server, timeout=10) as c:
        yield c


def corr(client, obj, f, t, value, expected):
    return client.post("/corrections", json={
        "object_id": obj,
        "valid_from": hour(f).isoformat(),
        "valid_to": hour(t).isoformat(),
        "value": value,
        "expected_revision": expected,
    })


def test_correction_and_bitemporal_query_api(client):
    obj = "api-1"
    assert client.post(f"/objects/{obj}").status_code == 201

    r = corr(client, obj, 0, 24, {"result": 10}, 0)
    assert r.status_code == 201, r.text
    assert r.json()["revision"] == 1

    r = corr(client, obj, 6, 9, {"result": 12}, 1)
    assert r.json()["revision"] == 2

    def q(at, rev):
        return client.get(f"/objects/{obj}/fact",
                          params={"validAt": hour(at).isoformat(),
                                  "knownAtRevision": rev}).json()

    # Unique fact at the current bi-temporal position.
    cur = q(7, 2)
    assert cur["found"] is True and cur["value"] == {"result": 12}

    # Same validAt, but as the system knew it at rev 1 -> old value intact.
    past = q(7, 1)
    assert past["found"] is True and past["value"] == {"result": 10}

    # Explicit emptiness: valid gap and pre-knowledge revision 0.
    gap = corr(client, obj, 10, 11, {"result": 50}, 2)
    assert gap.status_code == 201
    assert q(7, 0)["found"] is False          # nothing known yet
    assert q(11, 0)["found"] is False

    # Unknown revision is a 422, unknown object is a 404.
    bad = client.get(f"/objects/{obj}/fact",
                     params={"validAt": hour(7).isoformat(),
                             "knownAtRevision": 99})
    assert bad.status_code == 422
    assert client.get("/objects/nope/fact",
                      params={"validAt": hour(7).isoformat(),
                              "knownAtRevision": 0}).status_code == 404


def test_concurrent_corrections_one_wins_rest_conflict(client):
    """N clients race with expected_revision=0; exactly one commits."""
    obj = "race-1"
    client.post(f"/objects/{obj}")

    def attempt(i):
        return corr(client, obj, i, i + 1, {"who": i}, 0)

    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(attempt, range(8)))
    statuses = sorted(r.status_code for r in results)
    assert statuses.count(201) == 1
    assert statuses.count(409) == 7
    # Head advanced exactly once.
    assert client.get(f"/objects/{obj}").json()["head_revision"] == 1

    # No half-split: current timeline holds exactly one unit segment.
    tl = client.get(f"/objects/{obj}/timeline",
                    params={"knownAtRevision": 1}).json()
    assert len(tl) == 1


def test_concurrent_corrections_serialize_with_retry_and_match_oracle(client):
    """All corrections eventually commit (retry 409 with fresh head), and the
    final result equals the naive oracle fed the same successful sequence."""
    obj = "race-2"
    client.post(f"/objects/{obj}")
    intervals = [(i, i + 1, i) for i in range(12)]
    oracle = NaiveBitemporalObject()
    committed: list[tuple[int, int, int]] = []
    lock = threading.Lock()

    def worker(item):
        f, t, v = item
        for _ in range(50):
            head = client.get(f"/objects/{obj}").json()["head_revision"]
            r = corr(client, obj, f, t, {"v": v}, head)
            if r.status_code == 201:
                with lock:
                    committed.append((f, t, v))
                return r.json()["revision"]
            if r.status_code != 409:
                raise AssertionError(r.text)
            time.sleep(0.005)
        raise RuntimeError("did not commit")

    with ThreadPoolExecutor(max_workers=6) as ex:
        revs = list(ex.map(worker, intervals))
    assert sorted(revs) == list(range(1, 13))  # dense, database-assigned

    # Feed the oracle the actual commit order (unknown beforehand because of
    # the race) and compare every revision view with the database.
    for f, t, v in committed:
        oracle.correct(hour(f), hour(t), v, oracle.head)

    for rev in range(0, 13):
        tl = client.get(f"/objects/{obj}/timeline",
                        params={"knownAtRevision": rev}).json()
        ora = [
            {"valid_from": f.isoformat(), "valid_to": t.isoformat(),
             "value": {"v": v}}
            for f, t, v in oracle.known_view(rev)
        ]
        got = [{"valid_from": r["valid_from"], "valid_to": r["valid_to"],
                "value": r["value"]} for r in tl]
        # DB timestamps serialize UTC; compare reparsed triples.
        def norm(rows):
            return sorted(
                ((datetime.fromisoformat(x["valid_from"]),
                  datetime.fromisoformat(x["valid_to"]),
                  x["value"]) for x in rows),
                key=lambda z: z[0])
        assert norm(got) == norm(ora), rev


def test_concurrent_same_interval_exclusive_and_no_corruption(client):
    """Competing writers must never both split the same row (no overlap /
    duplicate even under repeated contention)."""
    obj = "race-3"
    client.post(f"/objects/{obj}")
    corr(client, obj, 0, 24, {"v": 0}, 0).raise_for_status()

    barrier = threading.Barrier(6)

    def writer(i):
        barrier.wait()  # maximize collision
        for _ in range(50):
            head = client.get(f"/objects/{obj}").json()["head_revision"]
            r = corr(client, obj, 10, 12, {"v": i}, head)
            if r.status_code == 201:
                return
            assert r.status_code == 409
            time.sleep(0.003)
        raise RuntimeError("writer starved")

    # Six writers hammer the SAME interval with retry; the final timeline must
    # still be a clean partition, last writer's value winning on [10,12).
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(writer, range(6)))

    tl = client.get(f"/objects/{obj}/timeline",
                    params={"knownAtRevision": 7}).json()
    bounds = [(r["valid_from"], r["valid_to"]) for r in tl]
    parsed = [(datetime.fromisoformat(a), datetime.fromisoformat(b))
              for a, b in bounds]
    for (_, at), (bf, _) in zip(parsed, parsed[1:]):
        assert at <= bf  # no overlaps
    # exactly three segments: [0,10),[10,12),[12,24)
    assert len(parsed) == 3
