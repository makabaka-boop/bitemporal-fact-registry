import os
import sys
import time
import socket
import subprocess
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Local unprivileged cluster started for this task on port 55432.
DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://bitemporal@localhost:55432/bitemporal?host=/tmp/pgsock",
)
PSQL = "/workspace/.local/pg/usr/lib/postgresql/15/bin/psql"
PGCTL = "/workspace/.local/pg/usr/lib/postgresql/15/bin/pg_ctl"
PGDATA = "/workspace/.local/pgdata"


def _port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def _ensure_postgres():
    if _port_open("localhost", 55432):
        return
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = "/workspace/.local/pg/usr/lib/aarch64-linux-gnu"
    subprocess.run([PGCTL, "-D", PGDATA, "-l", "/workspace/.local/pg.log",
                    "-w", "start"], check=True, env=env, capture_output=True)
    for _ in range(50):
        if _port_open("localhost", 55432):
            return
        time.sleep(0.1)
    raise RuntimeError("postgres did not come up")


@pytest.fixture(scope="session", autouse=True)
def postgres():
    _ensure_postgres()
    yield


@pytest.fixture(scope="session")
def clean_db(postgres):
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS fact_versions CASCADE")
        cur.execute("DROP TABLE IF EXISTS objects CASCADE")
        cur.execute("DROP FUNCTION IF EXISTS correct_fact CASCADE")
        cur.execute("DROP FUNCTION IF EXISTS fact_versions_immutable CASCADE")
    conn.commit()
    # re-create schema fresh, then discard connections that predate the DROP
    import app.main as main

    main.reset_pool()
    main._pool = None
    main.init_db()
    return DSN
