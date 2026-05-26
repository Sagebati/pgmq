//! End-to-end example using the tokio-postgres adapter.
//!
//! Run with:
//!   cargo run --no-default-features --features tokio-postgres,install-sql-embedded \
//!     --example tokio_postgres_basic

use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use pgmq::Queue;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct MyMessage {
    foo: String,
    num: u64,
}

#[tokio::main]
async fn main() {
    // Build a deadpool_postgres pool — pgmq doesn't open connections for you.
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(5432);
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    cfg.dbname = Some("postgres".to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("build pool");

    // One-time install via the tokio-postgres install module — pass a borrowed client.
    {
        let mut client = pool.get().await.expect("get client for install");
        pgmq::install::tokio_postgres::install_sql_from_embedded(&mut client)
            .await
            .expect("install");
    }

    let queue = "tpg_basic_queue";
    // Acquire a client and call pgmq on it. pgmq's API is implemented on
    // `&tokio_postgres::Client` — not on the pool, so any pool implementation works.
    let client = pool.get().await.expect("get client");
    let _ = client.drop_queue(queue).await;
    client.create(queue).await.expect("create");

    let msg = MyMessage {
        foo: "bar".into(),
        num: 42,
    };
    let id = client.send(queue, &msg).await.expect("send");
    println!("sent msg_id={id}");

    let received: pgmq::Message<MyMessage> = client
        .read(queue, 30)
        .await
        .expect("read")
        .expect("got a message");
    println!("received: {received:?}");
    assert_eq!(received.message, msg);

    // Composed with a user-owned transaction.
    let mut client = pool.get().await.expect("get client");
    let tx = client.transaction().await.expect("begin");
    tx.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _example_marker (note TEXT);",
        &[],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO _example_marker (note) VALUES ($1);",
        &[&"hello"],
    )
    .await
    .unwrap();
    let id2 = tx
        .send(
            queue,
            &MyMessage {
                foo: "in_tx".into(),
                num: 1,
            },
        )
        .await
        .expect("send in tx");
    tx.commit().await.expect("commit");
    println!("sent in tx, msg_id={id2}");

    let client = pool.get().await.expect("get for drop");
    client.drop_queue(queue).await.expect("drop");
}
