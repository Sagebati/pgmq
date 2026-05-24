-- Used by install::*::create_migrations_table(). Combines the advisory lock, the schema and
-- table creation, and the access-exclusive lock into one multi-statement batch (single
-- roundtrip via the simple-query protocol).
--
-- The advisory lock key (-9223372036854771659 = i64::MIN + 4149) is the same key used by
-- init.sql — see src/install/mod.rs::ADVISORY_LOCK_KEY.
SELECT pg_advisory_xact_lock({LOCK_KEY});
CREATE SCHEMA IF NOT EXISTS pgmq;
CREATE TABLE IF NOT EXISTS pgmq.__pgmq_migrations (
    name TEXT PRIMARY KEY NOT NULL,
    version TEXT NOT NULL,
    run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
);
LOCK TABLE pgmq.__pgmq_migrations IN ACCESS EXCLUSIVE MODE;
