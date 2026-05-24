//! Per-driver install tests.
//!
//! For each enabled adapter feature, verify that `install_sql_from_embedded` succeeds and
//! that `installed_version` returns a Some(...) afterwards. The install path is idempotent
//! and uses an advisory lock, so running these in parallel against the same database is safe.
//!
//! Run with the matching features enabled, e.g.:
//! ```
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo test --features sqlx,tokio-postgres,diesel-async,diesel-sync,install-sql-embedded
//! ```
//! Each test is `#[cfg]`-gated to its own feature so unused tests are skipped cleanly.

#![cfg(feature = "install-sql-embedded")]

use std::env;

fn db_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned())
}

#[cfg(feature = "sqlx")]
#[tokio::test]
async fn install_sqlx() {
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

#[cfg(feature = "tokio-postgres")]
#[tokio::test]
async fn install_tokio_postgres() {
    use tokio_postgres::NoTls;
    let (mut client, conn) = tokio_postgres::connect(&db_url(), NoTls).await.expect("connect");
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

#[cfg(feature = "diesel-async")]
#[tokio::test]
async fn install_diesel_async() {
    use diesel_async::pooled_connection::deadpool::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::AsyncPgConnection;

    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(db_url());
    let pool = Pool::builder(manager).build().expect("build pool");
    pgmq::install::diesel_async::install_sql_from_embedded(&pool)
        .await
        .expect("install via diesel-async");
    let version = pgmq::install::diesel_async::installed_version(&pool)
        .await
        .expect("installed_version via diesel-async");
    assert!(
        version.is_some(),
        "expected an installed pgmq version after install_sql_from_embedded via diesel-async"
    );
}

#[cfg(feature = "diesel-sync")]
#[test]
fn install_diesel_sync() {
    use diesel::pg::PgConnection;
    use diesel::Connection;
    let mut conn = PgConnection::establish(&db_url()).expect("connect");
    pgmq::install::diesel_sync::install_sql_from_embedded(&mut conn)
        .expect("install via diesel-sync");
    let version =
        pgmq::install::diesel_sync::installed_version(&mut conn).expect("installed_version via diesel-sync");
    assert!(
        version.is_some(),
        "expected an installed pgmq version after install_sql_from_embedded via diesel-sync"
    );
}
