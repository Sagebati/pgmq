//! Implementation details shared by the per-driver install modules. Not part of the public API.
//!
//! Items here are `pub` so sibling modules in `crate::install` can use them via `super::internal::*`.
//! The `internal` module itself is declared `mod internal` (private) in `install/mod.rs`, so
//! external callers cannot reach `pgmq::install::internal::*`.

use crate::install::script::MigrationScript;
use crate::install::version::Version;
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
        .filter(|script| !applied.iter().any(|a| a.name == script.name.original))
        .sorted()
        .collect()
}

/// Find the maximum version among already-applied migrations.
pub fn max_applied_version(applied: &[AppliedMigration]) -> Option<&Version> {
    applied.iter().map(|a| &a.version).max()
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

pub const INIT_SQL: &str = include_str!("../sql/install/init.sql");
pub const SETUP_MIGRATIONS_TABLE_SQL: &str =
    include_str!("../sql/install/setup_migrations_table.sql");
pub const SELECT_APPLIED_MIGRATIONS_SQL: &str =
    include_str!("../sql/install/select_applied_migrations.sql");
pub const INSERT_APPLIED_MIGRATION_SQL: &str =
    include_str!("../sql/install/insert_applied_migration.sql");
