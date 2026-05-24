//! Demonstrates composing pgmq calls with a user-owned sqlx transaction.
//!
//! Run with:
//!   cargo run --example transactions --features install-sql-embedded
//!
//! What this shows: the pattern "insert into my own table AND enqueue a message, atomically."
//! Either both happen, or neither does.

use pgmq::pg_ext::VisibilityTimeoutOffset;
use pgmq::PgMQConnExt;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq)]
struct OrderShipped {
    order_id: i64,
    total_cents: i64,
}

#[tokio::main]
async fn main() {
    let db_url = "postgres://postgres:postgres@localhost:5432/postgres";
    let pool = PgPool::connect(db_url).await.expect("connect to postgres");

    #[cfg(feature = "install-sql-embedded")]
    pgmq::install::sqlx::install_sql_from_embedded(&pool)
        .await
        .expect("install pgmq");

    let queue = "shipping_events";
    {
        let mut conn = pool.acquire().await.expect("acquire");
        let _ = conn.drop_queue(queue).await;
        conn.create(queue).await.expect("create queue");
    }

    // The user's own table.
    sqlx::query("CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY, total_cents BIGINT NOT NULL, shipped BOOL NOT NULL DEFAULT false);")
        .execute(&pool).await.expect("create orders");

    // --- Successful path: open a tx, do our own work, send to pgmq, commit. ---
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("INSERT INTO orders (id, total_cents, shipped) VALUES ($1, $2, true) ON CONFLICT (id) DO UPDATE SET shipped = true, total_cents = EXCLUDED.total_cents;")
        .bind(1001i64).bind(5000i64).execute(&mut *tx).await.expect("insert order");

    // pgmq.send inside the same transaction — the trait impls on `&mut Transaction<'_, Postgres>`.
    tx.send(
        queue,
        &OrderShipped {
            order_id: 1001,
            total_cents: 5000,
        },
    )
    .await
    .expect("send via tx");

    // Before commit: the message is invisible from a fresh connection.
    let len: i64 = sqlx::query("SELECT queue_length FROM pgmq.metrics($1)")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .expect("metrics")
        .get(0);
    println!("queue length before commit: {len}");
    assert_eq!(len, 0);

    tx.commit().await.expect("commit");

    let len: i64 = sqlx::query("SELECT queue_length FROM pgmq.metrics($1)")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .expect("metrics")
        .get(0);
    println!("queue length after commit: {len}");
    assert_eq!(len, 1);

    let mut conn = pool.acquire().await.expect("acquire");
    let msg: pgmq::Message<OrderShipped> = conn
        .read(queue, VisibilityTimeoutOffset::seconds(10))
        .await
        .expect("read")
        .expect("message present");
    println!("got message: {msg:?}");
    assert_eq!(msg.message.order_id, 1001);
    conn.delete(queue, msg.msg_id).await.expect("delete");

    // --- Failure path: if the user's work fails after pgmq.send inside a tx, the rollback
    //     prevents the message from being persisted. ---
    let mut tx = pool.begin().await.expect("begin 2");
    tx.send(
        queue,
        &OrderShipped {
            order_id: 9999,
            total_cents: 1,
        },
    )
    .await
    .expect("send via tx 2");
    tx.rollback().await.expect("rollback");

    let after_rollback_count: i64 = sqlx::query("SELECT queue_length FROM pgmq.metrics($1)")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .expect("metrics 2")
        .get(0);
    println!(
        "queue length after rollback: {after_rollback_count} (rolled-back send did not persist)"
    );

    conn.drop_queue(queue).await.expect("drop queue");
    sqlx::query("DROP TABLE orders;")
        .execute(&pool)
        .await
        .expect("drop orders");
}
