//! # Install / migrate the pgmq Postgres extension
//!
//! pgmq's runtime queries assume the extension's schema is present in your database
//! (`pgmq.create`, `pgmq.send`, etc.). Installation is a **one-time per-database** setup
//! step that adds that schema. This module provides per-driver installers that handle it.
//!
//! ## What the installer does
//!
//! It applies the pgmq extension's SQL migrations atomically inside a transaction, tracking
//! which migrations have already run in a `pgmq.__pgmq_migrations` table. This means:
//!
//! - **Idempotent.** Calling install on an already-installed database is a no-op for already-applied migrations and applies any newer ones.
//! - **Atomic.** Each migration script's SQL + the row recording it as applied are in the same transaction. A failure rolls both back.
//! - **Version-aware.** If you're upgrading pgmq (the extension), running the installer applies only the new migration scripts.
//!
//! ## Per-driver entry points
//!
//! Each driver has its own module that uses the driver's native transaction API:
//!
//! | Driver | Module | Takes |
//! |--------|--------|-------|
//! | sqlx (default) | [`sqlx`] | `&sqlx::PgPool` |
//! | tokio-postgres | [`tokio_postgres`] | `&mut tokio_postgres::Client` |
//! | diesel-async | [`diesel_async`] | `&diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>` |
//! | diesel (sync) | [`diesel_sync`] | `&mut diesel::pg::PgConnection` (synchronous) |
//!
//! Each module exposes the same four functions:
//!
//! - `init(...)` — runs `CREATE EXTENSION IF NOT EXISTS pgmq CASCADE` (use if you have the extension installed as a Postgres extension binary)
//! - `install_sql_from_embedded(...)` — SQL-only install using scripts bundled with the crate (no Postgres extension binary required, no network)
//! - `install_sql_from_github(...)` — SQL-only install fetching scripts from the pgmq GitHub repo (lets you pin a specific extension version); **not available for `diesel-sync`**
//! - `installed_version(...)` — returns the currently-applied pgmq version, or `None` if not installed
//! - `init_migrations_table(...)` — for upgrading from very old pre-versioned installs (rarely needed)
//!
//! ## Cargo features
//!
//! - `install-sql-embedded` — enables `install_sql_from_embedded` (pulls in `include_dir`)
//! - `install-sql-github` — enables `install_sql_from_github` (pulls in `reqwest` for HTTP)
//! - `install-sql` — base feature, enabled automatically by either of the above
//!
//! ## Examples
//!
//! ```ignore
//! // sqlx
//! let pool = sqlx::PgPool::connect(url).await?;
//! pgmq::install::sqlx::install_sql_from_embedded(&pool).await?;
//!
//! // tokio-postgres (bring your own pool; install takes a `&mut Client`)
//! let mut client = pool.get().await?;
//! pgmq::install::tokio_postgres::install_sql_from_embedded(&mut **client).await?;
//!
//! // diesel-async
//! let pool: diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection> = /* … */;
//! pgmq::install::diesel_async::install_sql_from_embedded(&pool).await?;
//!
//! // diesel (sync)
//! let mut conn = diesel::pg::PgConnection::establish(url)?;
//! pgmq::install::diesel_sync::install_sql_from_embedded(&mut conn)?;
//! ```
//!
//! ## CLI
//!
//! For one-time install at deploy time (outside your app), the `pgmq-cli` binary (enable the
//! `cli` feature) does the same thing from a shell:
//!
//! ```text
//! pgmq-cli install -d postgres://... install-from-embedded
//! pgmq-cli install -d postgres://... installed-version
//! ```

#[cfg(feature = "install-sql-embedded")]
mod embedded;
#[cfg(feature = "install-sql-github")]
mod github;
mod script;
mod version;

pub use version::Version;

use crate::errors::PgmqError;

#[cfg(feature = "sqlx")]
pub mod sqlx;

#[cfg(feature = "tokio-postgres")]
pub mod tokio_postgres;

#[cfg(feature = "diesel-async")]
pub mod diesel_async;

#[cfg(feature = "diesel-sync")]
pub mod diesel_sync;

/// Helper method to reduce the boilerplate required to create a [`PgmqError::InstallationError`].
fn install_err(err: impl ToString) -> PgmqError {
    PgmqError::InstallationError(err.to_string())
}

// Note: the advisory lock key is now inlined as a literal in `sql/install/{init,setup_migrations_table}.sql`
// since the simple-query protocol used by `batch_execute` doesn't support parameters. See those
// .sql files' comments for the literal value and its derivation.

/// A row from `pgmq.__pgmq_migrations`.
#[derive(Debug)]
pub(crate) struct AppliedMigration {
    pub name: String,
    pub version: Version,
}

/// Given the list of available migration scripts plus the list of already-applied migrations,
/// return the subset that still needs to be applied, ordered for execution.
pub(crate) fn filter_unapplied_scripts(
    available: Vec<script::MigrationScript>,
    applied: &[AppliedMigration],
) -> Vec<script::MigrationScript> {
    use itertools::Itertools;
    available
        .into_iter()
        .filter(|script| !applied.iter().any(|a| a.name == script.name.original))
        .sorted()
        .collect()
}

/// Find the maximum version among already-applied migrations.
pub(crate) fn max_applied_version(applied: &[AppliedMigration]) -> Option<&Version> {
    applied.iter().map(|a| &a.version).max()
}

#[cfg(feature = "install-sql-embedded")]
pub(crate) fn embedded_fetcher() -> embedded::EmbeddedScriptFetcher {
    embedded::EmbeddedScriptFetcher
}

#[cfg(feature = "install-sql-github")]
pub(crate) async fn github_fetcher(
    version: Option<&str>,
) -> Result<github::GitHubScriptFetcher, PgmqError> {
    github::GitHubScriptFetcher::new(version).await
}


#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;

    #[test]
    fn install_err() {
        let err = super::install_err("Some error");
        assert_debug_snapshot!(err);
    }
}
