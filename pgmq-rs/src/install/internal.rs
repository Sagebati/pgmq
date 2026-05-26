//! Implementation details shared by the per-driver install modules. Not part of the public API.
//!
//! Items here are `pub` so sibling modules in `crate::install` can use them via `super::internal::*`.
//! The `internal` module itself is declared `mod internal` (private) in `install/mod.rs`, so
//! external callers cannot reach `pgmq::install::internal::*`.

use crate::install::script::MigrationScript;
use crate::install::version::Version;
#[cfg(feature = "install-sql-github")]
use crate::PgmqError;

/// A row from `pgmq.__pgmq_migrations`.
#[derive(Debug)]
pub struct AppliedMigration {
    pub name: String,
    pub version: Version,
}

/// Given the list of available migration scripts plus the list of already-applied migrations,
/// return the subset that still needs to be applied, ordered for execution.
pub fn filter_unapplied_scripts(
    available: Vec<MigrationScript>,
    applied: &[AppliedMigration],
) -> Vec<MigrationScript> {
    use itertools::Itertools;
    available
        .into_iter()
        .filter(|script| !applied.iter().any(|mig| mig.name == script.name.original))
        .sorted()
        .collect()
}

/// Find the maximum version among already-applied migrations.
pub fn max_applied_version(applied: &[AppliedMigration]) -> Option<&Version> {
    applied.iter().map(|mig| &mig.version).max()
}

#[cfg(feature = "install-sql-embedded")]
pub fn embedded_fetcher() -> crate::install::embedded::EmbeddedScriptFetcher {
    crate::install::embedded::EmbeddedScriptFetcher
}

#[cfg(feature = "install-sql-github")]
pub async fn github_fetcher(
    version: Option<&str>,
) -> Result<crate::install::github::GitHubScriptFetcher, PgmqError> {
    crate::install::github::GitHubScriptFetcher::new(version).await
}

// SQL constants used by the per-driver install modules. Inlined from `src/sql/install/*.sql` —
// kept internal because they're an implementation detail of the install path.

/// Advisory lock key used by `init` and `setup_migrations_table` to serialize concurrent
/// installs/upgrades. A randomly-chosen large negative bigint (`i64::MIN + 4149`) picked to
/// minimize collision with application-level advisory locks.
///
/// Substituted into the install SQL at runtime in place of the `{LOCK_KEY}` placeholder.
/// (The simple-query protocol used by `batch_execute` doesn't support parameters, so we
/// inline the value as text.)
pub const ADVISORY_LOCK_KEY: i64 = i64::MIN + 4149;

const INIT_SQL_TEMPLATE: &str = include_str!("../sql/install/init.sql");
const SETUP_MIGRATIONS_TABLE_SQL_TEMPLATE: &str =
    include_str!("../sql/install/setup_migrations_table.sql");
pub const SELECT_APPLIED_MIGRATIONS_SQL: &str =
    include_str!("../sql/install/select_applied_migrations.sql");
pub const INSERT_APPLIED_MIGRATION_SQL: &str =
    include_str!("../sql/install/insert_applied_migration.sql");

/// Substitutes the `{LOCK_KEY}` placeholder in [`INIT_SQL_TEMPLATE`] with [`ADVISORY_LOCK_KEY`].
pub fn init_sql() -> String {
    INIT_SQL_TEMPLATE.replace("{LOCK_KEY}", &ADVISORY_LOCK_KEY.to_string())
}

/// Substitutes the `{LOCK_KEY}` placeholder in [`SETUP_MIGRATIONS_TABLE_SQL_TEMPLATE`] with
/// [`ADVISORY_LOCK_KEY`].
pub fn setup_migrations_table_sql() -> String {
    SETUP_MIGRATIONS_TABLE_SQL_TEMPLATE.replace("{LOCK_KEY}", &ADVISORY_LOCK_KEY.to_string())
}
