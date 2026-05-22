//! End-to-end smoke test for the tokio-postgres adapter and the [`pgmq::PGMQueueExt`] trait.
//!
//! Runs against a live Postgres (uses `DATABASE_URL` env var, defaults to a local instance).
//! Marked `#[ignore]` so it's only run explicitly:
//!   `cargo test --no-default-features --features tokio-postgres,install-sql-embedded \
//!     --test tokio_postgres_smoke -- --ignored`

#![cfg(all(feature = "tokio-postgres", feature = "install-sql-embedded"))]

use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use pgmq::pg_ext::VisibilityTimeoutOffset;
use pgmq::PGMQueueExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::env;
use tokio_postgres::NoTls;

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq, Clone)]
struct MyMessage {
    foo: String,
    num: u64,
}

fn pool_from_env() -> Pool {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let parsed = url::Url::parse(&url).expect("DATABASE_URL is a URL");
    let mut cfg = Config::new();
    cfg.host = parsed.host_str().map(|s| s.to_string());
    cfg.port = parsed.port();
    cfg.user = Some(parsed.username().to_string());
    cfg.password = parsed.password().map(|s| s.to_string());
    cfg.dbname = Some(parsed.path().trim_start_matches('/').to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("build pool")
}

fn unique_queue_name(prefix: &str) -> String {
    let n: u64 = rand::thread_rng().gen_range(0..1_000_000);
    format!("{prefix}_{n}")
}

async fn pool() -> Pool {
    let pool = pool_from_env();
    let mut client = pool.get().await.expect("get client");
    pgmq::install::tokio_postgres::install_sql_from_embedded(&mut **client)
        .await
        .expect("install pgmq");
    pool
}

#[ignore]
#[tokio::test]
async fn create_send_read_archive_delete() {
    let pool = pool().await;
    let q = unique_queue_name("tpg_smoke");
    let client = pool.get().await.expect("get client");
    let _ = client.drop_queue(&q).await;

    client.create(&q).await.expect("create");

    let msg = MyMessage {
        foo: "bar".into(),
        num: 42,
    };
    let id = client.send(&q, &msg).await.expect("send");
    assert!(id > 0);

    let read: pgmq::Message<MyMessage> = client
        .read(&q, VisibilityTimeoutOffset::seconds(30))
        .await
        .expect("read")
        .expect("got a message");
    assert_eq!(read.msg_id, id);
    assert_eq!(read.message, msg);

    let archived = client.archive(&q, id).await.expect("archive");
    assert!(archived);

    let empty: Option<pgmq::Message<MyMessage>> = client
        .read(&q, VisibilityTimeoutOffset::seconds(30))
        .await
        .expect("read 2");
    assert!(empty.is_none());

    client.drop_queue(&q).await.expect("drop");
}

#[ignore]
#[tokio::test]
async fn send_batch_and_metrics() {
    let pool = pool().await;
    let q = unique_queue_name("tpg_batch");
    let client = pool.get().await.expect("get client");
    let _ = client.drop_queue(&q).await;
    client.create(&q).await.expect("create");

    let messages: Vec<MyMessage> = (0..5)
        .map(|i| MyMessage {
            foo: "x".into(),
            num: i,
        })
        .collect();
    let ids = client
        .send_batch(&q, &messages)
        .await
        .expect("send_batch");
    assert_eq!(ids.len(), 5);

    let metrics = client.metrics(&q).await.expect("metrics");
    assert_eq!(metrics.queue_name, q);
    assert_eq!(metrics.queue_length, 5);

    client.drop_queue(&q).await.expect("drop");
}

#[ignore]
#[tokio::test]
async fn pgmq_inside_user_transaction() {
    let pool = pool().await;
    let q = unique_queue_name("tpg_tx");
    let client = pool.get().await.expect("get");
    client.create(&q).await.expect("create");

    let msg = MyMessage {
        foo: "tx".into(),
        num: 9,
    };
    let id: i64 = {
        let mut client = pool.get().await.expect("get client");
        let tx = client.transaction().await.expect("begin");

        tx.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _smoke_marker (note TEXT);",
            &[],
        )
        .await
        .expect("create temp");
        tx.execute("INSERT INTO _smoke_marker (note) VALUES ($1);", &[&"hello"])
            .await
            .expect("insert marker");

        // pgmq.send via the same tx — `&tx` matches the impl on `&Transaction<'_>`.
        let id = tx.send(&q, &msg).await.expect("send in tx");
        tx.commit().await.expect("commit");
        id
    };

    let client = pool.get().await.expect("get for read");
    let read: pgmq::Message<MyMessage> = client
        .read(&q, VisibilityTimeoutOffset::seconds(30))
        .await
        .expect("read")
        .expect("message present after commit");
    assert_eq!(read.msg_id, id);
    assert_eq!(read.message, msg);

    client.drop_queue(&q).await.expect("drop");
}
