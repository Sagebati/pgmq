-- Used by install::*::init(). Combines the advisory lock + CREATE EXTENSION into one
-- multi-statement batch (single roundtrip via the simple-query protocol).
--
-- The advisory lock key (-9223372036854771659 = i64::MIN + 4149) is a randomly-chosen large
-- negative bigint, picked to minimize collision with application-level advisory locks. See
-- src/install/mod.rs::ADVISORY_LOCK_KEY.
SELECT pg_advisory_xact_lock({LOCK_KEY});
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;
