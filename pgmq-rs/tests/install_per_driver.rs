//! Per-driver install tests.
//!
//! For each enabled adapter feature, verify both install paths succeed:
//! - `install_sql_from_embedded` — pure-SQL install (bundled with the crate)
//! - `init` — CREATE EXTENSION pgmq (requires the extension binary on the Postgres server)
//!
//! and that `installed_version` returns a `Some(...)` afterwards. The install paths are
//! idempotent (advisory-locked), so running these in parallel against the same database is safe.
//!
//! Run with the matching features enabled, e.g.:
//! ```
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo test --features sqlx,tokio-postgres,install-sql-embedded
//! ```
//! Each test is `#[cfg]`-gated to its own feature so unused tests are skipped cleanly.
//!
//! The `init` tests are gated on the `PGMQ_EXTENSION_INSTALLED` env var being set so they
//! only run on hosts that actually have the `pgmq` Postgres extension binary available.

#![cfg(feature = "install-sql-embedded")]

use std::env;

fn db_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned())
}

fn extension_available() -> bool {
    env::var("PGMQ_EXTENSION_INSTALLED").is_ok()
}

// ---- sqlx ----------------------------------------------------------------------

#[cfg(feature = "sqlx")]
#[tokio::test]
async fn install_sql_embedded_sqlx() {
    let pool = sqlx::PgPool::connect(&db_url()).await.expect("connect");
    pgmq::install::sqlx::install_sql_from_embedded(&pool)
        .await
        .expect("install via sqlx");
    let version = pgmq::install::sqlx::installed_version(&pool)
        .await
        .expect("installed_version via sqlx");
    assert!(
        version.is_some(),
        "expected an installed pgmq version after install_sql_from_embedded via sqlx"
    );
}

#[cfg(feature = "sqlx")]
#[tokio::test]
async fn init_create_extension_sqlx() {
    if !extension_available() {
        eprintln!("skipping init_create_extension_sqlx: set PGMQ_EXTENSION_INSTALLED=1 to run");
        return;
    }
    let pool = sqlx::PgPool::connect(&db_url()).await.expect("connect");
    pgmq::install::sqlx::init(&pool)
        .await
        .expect("init via sqlx");
}

// ---- tokio-postgres ------------------------------------------------------------

#[cfg(feature = "tokio-postgres")]
#[tokio::test]
async fn install_sql_embedded_tokio_postgres() {
    use tokio_postgres::NoTls;
    let (mut client, conn) = tokio_postgres::connect(&db_url(), NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    pgmq::install::tokio_postgres::install_sql_from_embedded(&mut client)
        .await
        .expect("install via tokio-postgres");
    let version = pgmq::install::tokio_postgres::installed_version(&mut client)
        .await
        .expect("installed_version via tokio-postgres");
    assert!(
        version.is_some(),
        "expected an installed pgmq version after install_sql_from_embedded via tokio-postgres"
    );
}

#[cfg(feature = "tokio-postgres")]
#[tokio::test]
async fn init_create_extension_tokio_postgres() {
    if !extension_available() {
        eprintln!(
            "skipping init_create_extension_tokio_postgres: set PGMQ_EXTENSION_INSTALLED=1 to run"
        );
        return;
    }
    use tokio_postgres::NoTls;
    let (mut client, conn) = tokio_postgres::connect(&db_url(), NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    pgmq::install::tokio_postgres::init(&mut client)
        .await
        .expect("init via tokio-postgres");
}
