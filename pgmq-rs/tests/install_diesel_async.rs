//! Integration test for the `diesel-async` SQL-only installer. Needs a Postgres reachable at
//! `DATABASE_URL` (defaults to the same URL as the other integration tests); the test creates and
//! drops its own database.

#![cfg(all(feature = "diesel-async", feature = "install-sql-embedded"))]

use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use pgmq::install::diesel_async as install;
use pgmq::queue::Queue;
use url::Url;

async fn connect(url: &Url) -> AsyncPgConnection {
    AsyncPgConnection::establish(url.as_str())
        .await
        .expect("connect to Postgres")
}

#[tokio::test]
async fn install_sql_from_embedded_is_idempotent_and_usable() {
    let base = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let base = Url::parse(&base).unwrap();
    let db_name = format!(
        "install_diesel_async_{}",
        chrono::Utc::now().timestamp_millis()
    );
    let mut test_url = base.clone();
    test_url.set_path(&db_name);

    let mut admin = connect(&base).await;
    admin
        .batch_execute(&format!("CREATE DATABASE \"{db_name}\";"))
        .await
        .unwrap();

    {
        let mut conn = connect(&test_url).await;

        assert_eq!(install::installed_version(&mut conn).await.unwrap(), None);

        install::install_sql_from_embedded(&mut conn).await.unwrap();
        let installed = install::installed_version(&mut conn)
            .await
            .unwrap()
            .expect("pgmq is installed");

        // A second run is a no-op rather than re-running (and failing on) `pgmq.sql`.
        install::install_sql_from_embedded(&mut conn).await.unwrap();
        assert_eq!(
            install::installed_version(&mut conn).await.unwrap(),
            Some(installed)
        );

        // The installed schema is usable through the `Queue` trait on the same connection.
        (&mut conn).create("install_test").await.unwrap();
    }

    admin
        .batch_execute(&format!("DROP DATABASE \"{db_name}\";"))
        .await
        .unwrap();
}
