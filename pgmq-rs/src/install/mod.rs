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
//!
//! Each module exposes the same set of functions:
//!
//! - `init(...)` — runs `CREATE EXTENSION IF NOT EXISTS pgmq CASCADE` (use if you have the extension installed as a Postgres extension binary)
//! - `install_sql_from_embedded(...)` — SQL-only install using scripts bundled with the crate (no Postgres extension binary required, no network)
//! - `install_sql_from_github(...)` — SQL-only install fetching scripts from the pgmq GitHub repo (lets you pin a specific extension version)
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
//! pgmq::install::tokio_postgres::install_sql_from_embedded(&mut client).await?;
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
mod internal;
mod script;
mod version;

pub use version::Version;

use crate::errors::PgmqError;

#[cfg(feature = "sqlx")]
pub mod sqlx;

#[cfg(feature = "tokio-postgres")]
pub mod tokio_postgres;

/// Helper method to reduce the boilerplate required to create a [`PgmqError::InstallationError`].
fn install_err(err: impl ToString) -> PgmqError {
    PgmqError::InstallationError(err.to_string())
}

// Note: shared install internals (`AppliedMigration`, the script-fetching helpers, the SQL
// constants) live in the private `internal` submodule above. They are intentionally not part
// of the public API; per-driver install modules access them via `super::internal::*`.

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;

    #[test]
    fn install_err() {
        let err = super::install_err("Some error");
        assert_debug_snapshot!(err);
    }
}
