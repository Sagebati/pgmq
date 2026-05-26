//! Smoke tests for the per-driver `PgmqError` variants. The previous suite (in the now-deleted
//! `tests/integration_test.rs.pre-trait-refactor`) covered these via the now-deprecated
//! `pgmq::PGMQueue` struct + a stringified `DatabaseError`. The variants are typed now, so this
//! file just asserts the variants surface as expected.

#![cfg(feature = "sqlx")]
#![allow(deprecated)]

use pgmq::PgmqError;
use std::env;

fn db_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned())
}

/// `PGMQueueExt::new` with a malformed URL should produce `PgmqError::UrlParseError`.
#[tokio::test]
async fn url_parse_error_surfaces() {
    let res = pgmq::PGMQueueExt::new("not://a valid url".to_owned(), 2).await;
    match res {
        Err(PgmqError::UrlParseError(_)) => {}
        Err(other) => panic!("expected UrlParseError, got {other:?}"),
        Ok(_) => panic!("expected UrlParseError, got Ok"),
    }
}

/// `PGMQueueExt::new` pointing at a host that won't resolve should surface as `SqlxError`
/// (the connection attempt fails inside sqlx).
#[tokio::test]
async fn unreachable_host_surfaces_as_sqlx_error() {
    let res = pgmq::PGMQueueExt::new(
        "postgres://u:p@invalid.host.example.test:5432/db".to_owned(),
        1,
    )
    .await;
    match res {
        Err(PgmqError::SqlxError(_)) => {}
        Err(other) => panic!("expected SqlxError, got {other:?}"),
        Ok(_) => panic!("expected SqlxError, got Ok"),
    }
}

/// Invalid queue name should surface as `PgmqError::InvalidQueueName`.
#[tokio::test]
async fn invalid_queue_name_validation() {
    use pgmq::Queue;
    let pool = match sqlx::PgPool::connect(&db_url()).await {
        Ok(p) => p,
        Err(_) => {
            eprintln!("skipping invalid_queue_name_validation: no DATABASE_URL reachable");
            return;
        }
    };
    let res = (&pool).create("invalid name with spaces").await;
    match res {
        Err(PgmqError::InvalidQueueName { name }) => {
            assert_eq!(name, "invalid name with spaces");
        }
        other => panic!("expected InvalidQueueName, got {other:?}"),
    }
}
