use pgmq::{errors::PgmqError, Message, Queue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), PgmqError> {
    // Initialize a connection to Postgres.
    println!("Connecting to Postgres");
    let pool = PgPool::connect("postgres://postgres:postgres@0.0.0.0:5432")
        .await
        .expect("Failed to connect to postgres");

    // One-time install of pgmq into the database.
    #[cfg(feature = "install-sql-embedded")]
    pgmq::install::sqlx::install_sql_from_embedded(&pool)
        .await
        .expect("Failed to install pgmq");

    // Acquire a connection from the pool. pgmq's queue API is implemented on
    // `&mut PgConnection`, not on `PgPool` — bring your own pool semantics.
    let mut conn = pool.acquire().await.expect("acquire");

    // Create a queue.
    println!("Creating a queue 'my_basic_queue'");
    let my_queue = "my_basic_queue";
    conn.create(my_queue).await.expect("Failed to create queue");

    // Send a message as JSON.
    let json_message = serde_json::json!({ "foo": "bar" });
    println!("Enqueueing a JSON message: {json_message}");
    let json_message_id: i64 = conn
        .send(my_queue, &json_message)
        .await
        .expect("Failed to enqueue message");

    // Messages can also be sent from structs.
    #[derive(Serialize, Debug, Deserialize)]
    struct MyMessage {
        foo: String,
    }
    let struct_message = MyMessage {
        foo: "bar".to_owned(),
    };
    println!("Enqueueing a struct message: {struct_message:?}");
    let struct_message_id: i64 = conn
        .send(my_queue, &struct_message)
        .await
        .expect("Failed to enqueue message");

    // The visibility timeout accepts any `Into<VisibilityTimeoutOffset>` — pass a plain
    // integer (seconds), an `i64`, a `std::time::Duration`, or a `chrono::Duration`.
    let received_json_message: Message<Value> = conn
        .read(my_queue, 30)
        .await
        .unwrap()
        .expect("No messages in the queue");
    println!("Received a message: {received_json_message:?}");
    assert_eq!(received_json_message.msg_id, json_message_id);

    let received_struct_message: Message<MyMessage> = conn
        .read(my_queue, 30)
        .await
        .unwrap()
        .expect("No messages in the queue");
    println!("Received a message: {received_struct_message:?}");
    assert_eq!(received_struct_message.msg_id, struct_message_id);

    let _ = conn
        .delete(my_queue, received_json_message.msg_id)
        .await
        .expect("Failed to delete message");
    let _ = conn
        .delete(my_queue, received_struct_message.msg_id)
        .await
        .expect("Failed to delete message");
    println!("Deleted the messages from the queue");

    let no_message: Option<Message<Value>> = conn.read(my_queue, 30).await.unwrap();
    assert!(no_message.is_none());

    Ok(())
}
