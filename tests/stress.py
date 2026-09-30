"""Ad-hoc high-contention stress against a live server (not part of pytest)."""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import uvicorn

import app.main as main

T0 = datetime(2026, 9, 21, tzinfo=timezone.utc)


def h(n):
    return (T0 + timedelta(hours=n)).isoformat()


def run():
    main.reset_pool()
    config = uvicorn.Config(main.app, host="127.0.0.1", port=55809,
                            log_level="error")
    srv = uvicorn.Server(config)
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    with httpx.Client(base_url="http://127.0.0.1:55809", timeout=10) as c:
        for _ in range(100):
            try:
                if c.get("/health").status_code == 200:
                    break
            except httpx.TransportError:
                time.sleep(0.05)

        obj = "stress"
        c.post(f"/objects/{obj}")
        c.post("/corrections", json={
            "object_id": obj, "valid_from": h(0), "valid_to": h(24),
            "value": {"v": 0}, "expected_revision": 0}).raise_for_status()

        N = 40
        barrier = threading.Barrier(N)
        committed = []
        lock = threading.Lock()

        def writer(i):
            barrier.wait()
            for _ in range(300):
                head = c.get(f"/objects/{obj}").json()["head_revision"]
                r = c.post("/corrections", json={
                    "object_id": obj, "valid_from": h(10), "valid_to": h(12),
                    "value": {"v": i}, "expected_revision": head})
                if r.status_code == 201:
                    with lock:
                        committed.append(i)
                    return
                assert r.status_code == 409, r.text
                time.sleep(0.002)
            raise RuntimeError("starved")

        with ThreadPoolExecutor(max_workers=N) as ex:
            list(ex.map(writer, range(N)))

        head = c.get(f"/objects/{obj}").json()["head_revision"]
        assert head == N + 1, head

        # Every revision view is a clean partition (no overlaps).
        for rev in range(1, head + 1):
            rows = c.get(f"/objects/{obj}/timeline",
                         params={"knownAtRevision": rev}).json()
            bounds = sorted((r["valid_from"], r["valid_to"]) for r in rows)
            for (_, a), (b, _) in zip(bounds, bounds[1:]):
                assert a <= b, (rev, a, b)

        tl = c.get(f"/objects/{obj}/timeline",
                   params={"knownAtRevision": head}).json()
        assert len(tl) == 3, [(r["valid_from"], r["valid_to"]) for r in tl]
        print(f"STRESS OK: {N} racing writers -> head {head}; "
              f"winner v={committed[-1]}; clean partition at every revision")

    srv.should_exit = True


if __name__ == "__main__":
    run()
