//! SQL for the `pgmq.__pgmq_migrations` bookkeeping table, shared by every driver's install path.

pub(crate) const CREATE_SCHEMA: &str = "CREATE SCHEMA IF NOT EXISTS pgmq;";

pub(crate) const CREATE_MIGRATIONS_TABLE: &str = "CREATE TABLE IF NOT EXISTS pgmq.__pgmq_migrations ( name TEXT PRIMARY KEY NOT NULL, version TEXT NOT NULL, run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp());";

pub(crate) const LOCK_MIGRATIONS_TABLE: &str =
    "LOCK TABLE pgmq.__pgmq_migrations IN ACCESS EXCLUSIVE MODE;";

pub(crate) const SELECT_APPLIED_MIGRATIONS: &str =
    "SELECT name, version FROM pgmq.__pgmq_migrations";

pub(crate) const INSERT_APPLIED_MIGRATION: &str =
    "INSERT INTO pgmq.__pgmq_migrations ( name, version ) VALUES ( $1, $2 );";
