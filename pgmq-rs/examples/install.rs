use pgmq::pg_ext::VisibilityTimeoutOffset;
use pgmq::{Message, Queue};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq)]
struct MyMessage {
    foo: String,
    num: u64,
}

#[tokio::main]
async fn main() {
    let db_url = "postgres://postgres:postgres@localhost:5432/postgres";
    let pool = PgPool::connect(db_url)
        .await
        .expect("failed to connect to postgres");

    // Installs the specific version from GitHub.
    pgmq::install::sqlx::install_sql_from_github(&pool, Some("1.10.0"))
        .await
        .unwrap();

    // Installs the version embedded in the rust crate. This may not always be the latest released
    // extension version.
    pgmq::install::sqlx::install_sql_from_embedded(&pool)
        .await
        .unwrap();

    // Installs the latest version from GitHub.
    pgmq::install::sqlx::install_sql_from_github(&pool, None)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.expect("acquire");
    conn.create("my_queue")
        .await
        .expect("failed to create queue");

    let msg = MyMessage {
        foo: "hello".to_string(),
        num: 42,
    };
    conn.send("my_queue", &msg)
        .await
        .expect("failed to send message");
    let received: Message<MyMessage> = conn
        .read("my_queue", VisibilityTimeoutOffset::seconds(15))
        .await
        .unwrap()
        .expect("No messages in the queue");
    println!("Received a message: {received:?}");
}
