//! End-to-end smoke test for the sync diesel adapter.
//!
//! The pgmq trait is async, but the diesel-sync impl runs synchronously. Sync tests use
//! `futures::executor::block_on` to drive the futures (each is ready on first poll).
//!
//! Run with:
//!   DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo test --no-default-features --features diesel-sync,install-sql-embedded \
//!     --test diesel_sync_smoke -- --ignored --nocapture

#![cfg(all(feature = "diesel-sync", feature = "install-sql-embedded"))]

use diesel::pg::PgConnection;
use diesel::Connection;
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

fn db_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned())
}

fn unique_queue_name(prefix: &str) -> String {
    let n: u64 = rand::thread_rng().gen_range(0..1_000_000);
    format!("{prefix}_{n}")
}

fn conn() -> PgConnection {
    let mut conn = PgConnection::establish(&db_url()).expect("connect");
    pgmq::install::diesel_sync::install_sql_from_embedded(&mut conn).expect("install");
    conn
}

/// Tests use tokio's runtime (already in dev-deps) to drive futures from the sync adapter.
/// In real sync code without an async runtime, see `block_on` in the adapter's module docs.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

#[ignore]
#[test]
fn create_send_read_archive_delete() {
    let mut conn = conn();
    let q = unique_queue_name("diesel_sync_smoke");
    let _ = block_on((&mut conn).drop_queue(&q));

    block_on((&mut conn).create(&q)).expect("create");

    let msg = MyMessage {
        foo: "bar".into(),
        num: 42,
    };
    let id = block_on((&mut conn).send(&q, &msg)).expect("send");
    assert!(id > 0);

    let read: pgmq::Message<MyMessage> =
        block_on((&mut conn).read(&q, VisibilityTimeoutOffset::seconds(30)))
            .expect("read")
            .expect("got a message");
    assert_eq!(read.msg_id, id);
    assert_eq!(read.message, msg);

    let archived = block_on((&mut conn).archive(&q, id)).expect("archive");
    assert!(archived);

    let empty: Option<pgmq::Message<MyMessage>> =
        block_on((&mut conn).read(&q, VisibilityTimeoutOffset::seconds(30))).expect("read 2");
    assert!(empty.is_none());

    block_on((&mut conn).drop_queue(&q)).expect("drop");
}

#[ignore]
#[test]
fn send_batch_and_metrics() {
    let mut conn = conn();
    let q = unique_queue_name("diesel_sync_batch");
    let _ = block_on((&mut conn).drop_queue(&q));
    block_on((&mut conn).create(&q)).expect("create");

    let messages: Vec<MyMessage> = (0..5)
        .map(|i| MyMessage {
            foo: "x".into(),
            num: i,
        })
        .collect();
    let ids = block_on((&mut conn).send_batch(&q, &messages)).expect("send_batch");
    assert_eq!(ids.len(), 5);

    let metrics = block_on((&mut conn).metrics(&q)).expect("metrics");
    assert_eq!(metrics.queue_name, q);
    assert_eq!(metrics.queue_length, 5);

    block_on((&mut conn).drop_queue(&q)).expect("drop");
}

#[ignore]
#[test]
fn pgmq_inside_user_transaction() {
    use diesel::{sql_query, RunQueryDsl};
    let mut conn = conn();
    let q = unique_queue_name("diesel_sync_tx");
    block_on((&mut conn).create(&q)).expect("create");

    let msg = MyMessage {
        foo: "tx".into(),
        num: 9,
    };
    let q_clone = q.clone();
    let msg_clone = msg.clone();
    let id: i64 = conn
        .transaction::<_, pgmq::PgmqError, _>(|conn| {
            sql_query("CREATE TEMP TABLE IF NOT EXISTS _diesel_sync_marker (note TEXT);")
                .execute(conn)?;
            sql_query("INSERT INTO _diesel_sync_marker (note) VALUES ('hello');").execute(conn)?;
            block_on((&mut *conn).send(&q_clone, &msg_clone))
        })
        .expect("transaction");

    let read: pgmq::Message<MyMessage> =
        block_on((&mut conn).read(&q, VisibilityTimeoutOffset::seconds(30)))
            .expect("read")
            .expect("message present after commit");
    assert_eq!(read.msg_id, id);
    assert_eq!(read.message, msg);

    block_on((&mut conn).drop_queue(&q)).expect("drop");
}
