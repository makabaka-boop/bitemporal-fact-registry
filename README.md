# Bitemporal facts (FastAPI + PostgreSQL)

A correction can arrive **today** but claim validity **last week**, and
overwriting the old row would make *“what the system knew at the time”*
unrecoverable. This service stores facts on two independent axes:

* **Valid time** — when the fact was true in the real world — as a half-open
  interval `[valid_from, valid_to)`.
* **Known/assertion time** — when the database committed the fact — as a
  **database-assigned, per-object revision number**.

A lab result corrected on Tuesday for Monday therefore appears in Tuesday's
current view while Monday's view (and every view that had already seen it)
remains exactly as it was.

## Model (`app/schema.sql`)

Classic assertion-interval table `fact_versions`:

| column                  | meaning                                                 |
|-------------------------|---------------------------------------------------------|
| `valid_from`,`valid_to` | half-open valid interval `[from,to)`                    |
| `assert_from`           | revision in which the row was first asserted            |
| `assert_to`             | `NULL` while live; else the revision that superseded it |
| `value`                 | JSONB fact payload                                      |

Historical view as of revision **R**:

```sql
assert_from <= R AND (assert_to IS NULL OR R < assert_to)
```

### Why history cannot be rewritten

* Rows are **never deleted**, and no column other than `assert_to` is ever
  updated. A row trigger (`fact_versions_immutable`, SQLSTATE `99010`) forbids
  deletes outright and only permits closing `assert_to` once, with a strictly
  greater revision.
* Closing a row with `assert_to = r` cannot change any earlier view: for every
  `R < r` the view predicate behaves exactly as before.

### Why valid intervals never overlap in any view

```sql
CREATE INDEX fact_versions_no_overlap
  ON fact_versions USING GIST (object_id, tstzrange(valid_from, valid_to,'[)'))
  WHERE assert_to IS NULL;
```

The partial GiST index enforces that an object's currently-asserted timeline
is a non-overlapping partition. Because past rows are immutable, every
historical revision view is a frozen snapshot of an equally consistent state.

### Transactional correction (`correct_fact`)

Everything runs in **one transaction**:

1. `SELECT … FOR UPDATE` on the object row serializes concurrent corrections;
   the lock is held until commit/rollback.
2. The caller's `expected_revision` is compared to the database head. A stale
   expectation raises SQLSTATE `99001` (HTTP **409**) **before any write**.
3. Each live row overlapping `[valid_from, valid_to)` is closed at revision
   `head+1`, and non-empty **left/right remnants** are re-inserted — this is
   the timeline split.
4. The corrected row is inserted and `objects.head_revision` advances by one.

All closes/inserts commit together or the transaction aborts, so a conflict
can never leave a half-split timeline (the exclusion index would also reject
such a commit). Revisions are database-assigned and dense per object.

## API

| method & path                                                | purpose                                                        |
|--------------------------------------------------------------|----------------------------------------------------------------|
| `POST /objects/{id}`                                         | create object (head revision 0)                                |
| `GET  /objects/{id}`                                         | current head revision                                          |
| `POST /corrections`                                          | split + insert (body below)                                    |
| `GET  /objects/{id}/fact?validAt=&knownAtRevision=`          | unique fact at the bi-temporal point, or explicit `found:false`|
| `GET  /objects/{id}/timeline?knownAtRevision=`               | full frozen view as of one revision                            |
| `GET  /health`                                               | liveness                                                       |

Correction body:

```json
{
  "object_id": "patient-42",
  "valid_from": "2026-09-21T06:00:00Z",
  "valid_to":   "2026-09-21T18:00:00Z",
  "value": {"mg_dL": 107},
  "expected_revision": 2
}
```

A point query returns the **single** fact at `(validAt, knownAtRevision)` or
`{"found": false}` for an explicit gap. Future revision → `422`, unknown
object → `404`, stale `expected_revision` → `409`.

## Tests and the naive history-snapshot oracle

`tests/naive_oracle.py` is a deliberately simple, independent reference model:
each revision stores a **frozen Python list** of non-overlapping segments; a
correction replaces the head with a brand-new split list, and old lists are
never touched. The tests drive database and oracle with the same corrections
and compare **every revision view point-by-point** — two implementations that
share no code agreeing is the cross-check requested.

* `tests/test_bitemporal.py` — consecutive corrections (insert, middle split,
  nested overwrite, full overwrite, extension into unknown time), retroactive
  point queries over a dense grid, explicit gaps, stale revision, and
  empty/reversed intervals.
* `tests/test_invariants.py` — no overlap in **every** historical view; rows
  can be neither deleted nor column-mutated; rev-1 content stays byte-identical
  after later corrections.
* `tests/test_api_concurrency.py` — real uvicorn server: 8 racers sharing one
  expectation produce exactly one `201` and seven `409` with head advanced
  once and no half-split; 12 retrying writers all commit densely and every
  resulting view matches the oracle in the actual commit order; 6 writers
  hammering the same interval still leave a clean partition.
* `tests/stress.py` — a 40-thread high-contention variant.

## Run

```bash
# Requires PostgreSQL with btree_gist:  CREATE EXTENSION btree_gist;
export DATABASE_URL="postgresql://bitemporal@localhost:55432/bitemporal?host=/tmp/pgsock"
uvicorn app.main:app --port 8000
python -m pytest tests/ -q
```
