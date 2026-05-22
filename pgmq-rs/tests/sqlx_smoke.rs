//! End-to-end smoke test for the sqlx adapter and the [`pgmq::PGMQueueExt`] trait.
//!
//! Runs against a live Postgres (uses `DATABASE_URL` env var, defaults to a local instance).
//! Marked `#[ignore]` so it's only run explicitly: `cargo test --features sqlx,install-sql-embedded -- --ignored`.

#![cfg(all(feature = "sqlx", feature = "install-sql-embedded"))]

use pgmq::pg_ext::VisibilityTimeoutOffset;
use pgmq::PGMQueueExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq, Clone)]
struct MyMessage {
    foo: String,
    num: u64,
}

fn db_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned())
}

fn unique_queue_name(prefix: &str) -> String {
    let n: u64 = rand::thread_rng().gen_range(0..1_000_000);
    format!("{prefix}_{n}")
}

async fn pool() -> PgPool {
    let pool = PgPool::connect(&db_url()).await.expect("connect");
    pgmq::install::sqlx::install_sql_from_embedded(&pool)
        .await
        .expect("install pgmq");
    pool
}

#[ignore]
#[tokio::test]
async fn create_send_read_archive_delete() {
    let pool = pool().await;
    let q = unique_queue_name("smoke");
    let mut conn = pool.acquire().await.expect("acquire");
    let _ = conn.drop_queue(&q).await;

    conn.create(&q).await.expect("create");

    let msg = MyMessage {
        foo: "bar".into(),
        num: 42,
    };
    let id = conn.send(&q, &msg).await.expect("send");
    assert!(id > 0);

    let read: pgmq::Message<MyMessage> = conn
        .read(&q, VisibilityTimeoutOffset::seconds(30))
        .await
        .expect("read")
        .expect("got a message");
    assert_eq!(read.msg_id, id);
    assert_eq!(read.message, msg);

    let archived = conn.archive(&q, id).await.expect("archive");
    assert!(archived);

    // Now empty.
    let empty: Option<pgmq::Message<MyMessage>> = conn
        .read(&q, VisibilityTimeoutOffset::seconds(30))
        .await
        .expect("read 2");
    assert!(empty.is_none());

    conn.drop_queue(&q).await.expect("drop");
}

#[ignore]
#[tokio::test]
async fn send_batch_and_metrics() {
    let pool = pool().await;
    let q = unique_queue_name("smoke_batch");
    let mut conn = pool.acquire().await.expect("acquire");
    let _ = conn.drop_queue(&q).await;
    conn.create(&q).await.expect("create");

    let messages: Vec<MyMessage> = (0..5)
        .map(|i| MyMessage {
            foo: "x".into(),
            num: i,
        })
        .collect();
    let ids = conn
        .send_batch(&q, &messages)
        .await
        .expect("send_batch");
    assert_eq!(ids.len(), 5);

    let metrics = conn.metrics(&q).await.expect("metrics");
    assert_eq!(metrics.queue_name, q);
    assert_eq!(metrics.queue_length, 5);
    assert_eq!(metrics.total_messages, 5);

    let read: Vec<pgmq::Message<MyMessage>> = conn
        .read_batch(&q, VisibilityTimeoutOffset::seconds(30), 10)
        .await
        .expect("read_batch");
    assert_eq!(read.len(), 5);

    let n_deleted = conn
        .delete_batch(&q, &ids)
        .await
        .expect("delete_batch");
    assert_eq!(n_deleted, 5);

    conn.drop_queue(&q).await.expect("drop");
}

#[ignore]
#[tokio::test]
async fn list_queues_includes_created() {
    let pool = pool().await;
    let q = unique_queue_name("smoke_list");
    let mut conn = pool.acquire().await.expect("acquire");
    let _ = conn.drop_queue(&q).await;
    conn.create(&q).await.expect("create");

    let queues = conn
        .list_queues()
        .await
        .expect("list_queues")
        .expect("at least one queue");
    assert!(queues.iter().any(|qm| qm.queue_name == q));

    conn.drop_queue(&q).await.expect("drop");
}

#[ignore]
#[tokio::test]
async fn pop_returns_and_removes() {
    let pool = pool().await;
    let q = unique_queue_name("smoke_pop");
    let mut conn = pool.acquire().await.expect("acquire");
    let _ = conn.drop_queue(&q).await;
    conn.create(&q).await.expect("create");

    let msg = MyMessage {
        foo: "pop".into(),
        num: 7,
    };
    let _ = conn.send(&q, &msg).await.expect("send");

    let popped: Option<pgmq::Message<MyMessage>> =
        conn.pop(&q).await.expect("pop");
    let popped = popped.expect("got a message");
    assert_eq!(popped.message, msg);

    let again: Option<pgmq::Message<MyMessage>> =
        conn.pop(&q).await.expect("pop 2");
    assert!(again.is_none());

    conn.drop_queue(&q).await.expect("drop");
}
