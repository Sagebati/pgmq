//! End-to-end smoke test for the diesel-async adapter.
//!
//! Run with:
//!   DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo test --no-default-features --features diesel-async,install-sql-embedded \
//!     --test diesel_async_smoke -- --ignored --nocapture

#![cfg(all(feature = "diesel-async", feature = "install-sql-embedded"))]

use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use pgmq::pg_ext::VisibilityTimeoutOffset;
use pgmq::PGMQueueExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq, Clone)]
struct MyMessage {
    foo: String,
    num: u64,
}

fn pool_from_env() -> Pool<AsyncPgConnection> {
    let url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    Pool::builder(manager).build().expect("build pool")
}

fn unique_queue_name(prefix: &str) -> String {
    let n: u64 = rand::thread_rng().gen_range(0..1_000_000);
    format!("{prefix}_{n}")
}

async fn pool() -> Pool<AsyncPgConnection> {
    let pool = pool_from_env();
    pgmq::install::diesel_async::install_sql_from_embedded(&pool)
        .await
        .expect("install pgmq");
    pool
}

#[ignore]
#[tokio::test]
async fn create_send_read_archive_delete() {
    let pool = pool().await;
    let q = unique_queue_name("diesel_smoke");
    let mut conn = pool.get().await.expect("get conn");
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
    let q = unique_queue_name("diesel_batch");
    let mut conn = pool.get().await.expect("get conn");
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

    conn.drop_queue(&q).await.expect("drop");
}

#[ignore]
#[tokio::test]
async fn pgmq_inside_user_transaction() {
    let pool = pool().await;
    let q = unique_queue_name("diesel_tx");
    let mut conn = pool.get().await.expect("get");
    conn.create(&q).await.expect("create");

    let msg = MyMessage {
        foo: "tx".into(),
        num: 9,
    };
    let q_clone = q.clone();
    let msg_clone = msg.clone();
    let id: i64 = conn
        .transaction::<_, pgmq::PgmqError, _>(|conn| {
            async move {
                diesel::sql_query("CREATE TEMP TABLE IF NOT EXISTS _diesel_smoke_marker (note TEXT);")
                    .execute(conn).await?;
                diesel::sql_query("INSERT INTO _diesel_smoke_marker (note) VALUES ('hello');")
                    .execute(conn).await?;
                let id = conn.send(&q_clone, &msg_clone).await?;
                Ok(id)
            }
            .scope_boxed()
        })
        .await
        .expect("transaction");

    let mut conn = pool.get().await.expect("get for read");
    let read: pgmq::Message<MyMessage> = conn
        .read(&q, VisibilityTimeoutOffset::seconds(30))
        .await
        .expect("read")
        .expect("message present after commit");
    assert_eq!(read.msg_id, id);
    assert_eq!(read.message, msg);

    conn.drop_queue(&q).await.expect("drop");
}
