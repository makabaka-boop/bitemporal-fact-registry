-- Bitemporal store.
--
-- Two independent axes per object fact:
--   * VALID time  — when the fact was true in the real world.
--                   Half-open interval [valid_from, valid_to).
--   * KNOWN time  — when the database committed that fact.
--                   Encoded as database-assigned per-object revision numbers.
--
-- Classic assertion-interval table:
--   assert_from : revision in which the row first existed
--   assert_to   : NULL while live; otherwise the revision that superseded it
--
-- Historical view as of revision R:
--   assert_from <= R AND (assert_to IS NULL OR R < assert_to)
--
-- Rows are NEVER deleted and no column other than assert_to is ever updated
-- (enforced by fact_versions_immutable below). Closing assert_to with a
-- strictly greater revision cannot change any earlier view: for every
-- R < new assert_to the view predicate behaves exactly as before. A lab
-- result corrected today can therefore be back-dated into last week while
-- "what the system knew at revision R" stays reconstructible forever.

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS objects (
    object_id     TEXT PRIMARY KEY,
    head_revision INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fact_versions (
    id           BIGSERIAL PRIMARY KEY,
    object_id    TEXT NOT NULL
                   REFERENCES objects(object_id),
    valid_from   TIMESTAMPTZ NOT NULL,
    valid_to     TIMESTAMPTZ NOT NULL,
    assert_from  INTEGER NOT NULL,
    assert_to    INTEGER,
    value        JSONB NOT NULL,
    CONSTRAINT fact_versions_half_open_chk CHECK (valid_from < valid_to),
    CONSTRAINT fact_versions_assert_chk   CHECK (assert_from < assert_to)
);

CREATE INDEX IF NOT EXISTS fact_versions_object_idx
    ON fact_versions (object_id, assert_from, assert_to);

-- Core invariant for the CURRENT view: live (assert_to IS NULL) rows of one
-- object never overlap in valid time. Every past view is a frozen snapshot of
-- an equally consistent state because rows are immutable, so the invariant
-- holds inside every historical view too.
CREATE INDEX IF NOT EXISTS fact_versions_no_overlap
    ON fact_versions USING GIST (object_id, tstzrange(valid_from, valid_to, '[)'))
    WHERE assert_to IS NULL;

-- ---------------------------------------------------------------------------
-- History immutability
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fact_versions_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            MESSAGE = 'fact history is append-only; deletes are forbidden',
            ERRCODE  = '99010';
    END IF;

    -- UPDATE: the only tolerated mutation is closing the live interval once,
    -- assert_to: NULL -> r where r > assert_from. Anything else would rewrite
    -- a historical view and is rejected.
    IF OLD.assert_to IS NOT NULL
       OR NEW.assert_to IS NULL
       OR NEW.assert_to <= OLD.assert_from
       OR NEW.id <> OLD.id
       OR NEW.object_id <> OLD.object_id
       OR NEW.valid_from <> OLD.valid_from
       OR NEW.valid_to <> OLD.valid_to
       OR NEW.assert_from <> OLD.assert_from
       OR NEW.value IS DISTINCT FROM OLD.value
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'fact rows are immutable; only assert_to may be closed once',
            ERRCODE  = '99010';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS fact_versions_immutable_trg ON fact_versions;
CREATE TRIGGER fact_versions_immutable_trg
    BEFORE UPDATE OR DELETE ON fact_versions
    FOR EACH ROW EXECUTE FUNCTION fact_versions_immutable();

-- ---------------------------------------------------------------------------
-- Transactional correction: split the currently-asserted timeline inside one
-- transaction. Either every close/insert succeeds or the transaction aborts
-- (the exclusion constraint also guarantees no half-split state can commit).
--
-- p_expected_revision implements optimistic concurrency: it must equal the
-- object's current head. Conflicting concurrent corrections retry against the
-- same row lock (SELECT ... FOR UPDATE serializes them); a stale expectation
-- raises SQLSTATE 99001 and nothing is changed.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION correct_fact(
    p_object_id          TEXT,
    p_valid_from         TIMESTAMPTZ,
    p_valid_to           TIMESTAMPTZ,
    p_value              JSONB,
    p_expected_revision  INTEGER
) RETURNS INTEGER AS $$
DECLARE
    v_head     INTEGER;
    v_new_rev  INTEGER;
    v_row      RECORD;
BEGIN
    IF p_valid_from >= p_valid_to THEN
        RAISE EXCEPTION USING
            MESSAGE = 'valid interval must be non-empty half-open [from, to)',
            ERRCODE  = '99002';
    END IF;

    -- Lock the object row so concurrent corrections serialize. The lock is
    -- held until commit/rollback: no interleaving, no half-split timeline.
    INSERT INTO objects(object_id) VALUES (p_object_id)
        ON CONFLICT (object_id) DO NOTHING;

    SELECT head_revision INTO v_head
      FROM objects
     WHERE object_id = p_object_id
     FOR UPDATE;

    IF v_head <> p_expected_revision THEN
        RAISE EXCEPTION USING
            MESSAGE = format('expected revision %s but head is %s',
                             p_expected_revision, v_head),
            ERRCODE  = '99001';
    END IF;

    v_new_rev := v_head + 1;

    -- Close every LIVE row whose valid interval overlaps the correction.
    -- Collect them first; the trigger enforces that only assert_to changes.
    FOR v_row IN
        SELECT * FROM fact_versions
         WHERE object_id = p_object_id
           AND assert_to IS NULL
           AND valid_from < p_valid_to
           AND valid_to   > p_valid_from
         ORDER BY valid_from
        FOR UPDATE
    LOOP
        UPDATE fact_versions
           SET assert_to = v_new_rev
         WHERE id = v_row.id;

        -- Left remnant (adjacent only, never empty).
        IF v_row.valid_from < p_valid_from THEN
            INSERT INTO fact_versions
                (object_id, valid_from, valid_to, assert_from, assert_to, value)
            VALUES
                (p_object_id, v_row.valid_from, p_valid_from,
                 v_new_rev, NULL, v_row.value);
        END IF;

        -- Right remnant.
        IF p_valid_to < v_row.valid_to THEN
            INSERT INTO fact_versions
                (object_id, valid_from, valid_to, assert_from, assert_to, value)
            VALUES
                (p_object_id, p_valid_to, v_row.valid_to,
                 v_new_rev, NULL, v_row.value);
        END IF;
    END LOOP;

    -- The corrected fact for the requested interval.
    INSERT INTO fact_versions
        (object_id, valid_from, valid_to, assert_from, assert_to, value)
    VALUES
        (p_object_id, p_valid_from, p_valid_to, v_new_rev, NULL, p_value);

    UPDATE objects
       SET head_revision = v_new_rev
     WHERE object_id = p_object_id;

    RETURN v_new_rev;
END;
$$ LANGUAGE plpgsql;
