//! # The queue API — [`Queue`]
//!
//! This module declares the extension trait that adds queue methods to your Postgres
//! connection. Each driver adapter in [`crate::adapters`] implements every method natively
//! against the driver's typed query API.
//!
//! Bring the trait into scope and call methods directly on a connection or transaction:
//!
//! ```ignore
//! use pgmq::Queue;
//! conn.create("my_queue").await?;
//! let id = conn.send("my_queue", &payload).await?;
//! ```
//!
//! ## Method categories
//!
//! - **Queue lifecycle:** `create`, `create_unlogged`, `create_partitioned`, `drop_queue`,
//!   `purge_queue`, `list_queues`, `create_fifo_index`, `convert_archive_partitioned`
//! - **Enqueue:** `send`, `send_delay`, `send_delay_with_headers`, `send_batch`,
//!   `send_batch_with_delay`, `send_batch_with_delay_with_headers`
//! - **Dequeue:** `read`, `read_batch`, `read_with_poll`, `read_batch_with_poll`,
//!   `read_grouped*`, `pop`, `set_vt`
//! - **Acknowledge:** `archive`, `archive_batch`, `delete`, `delete_batch`
//! - **Topic-based routing:** `bind_topic`, `unbind_topic`, `list_topic_bindings`,
//!   `list_topic_bindings_all`, `send_topic`, `send_batch_topic`
//! - **Notification triggers:** `enable_notify_insert`, `disable_notify_insert`,
//!   `update_notify_insert`, `list_notify_insert_throttles`
//! - **Observability:** `metrics`, `metrics_all`
//!
//! ## Visibility timeout
//!
//! Most read/dequeue methods accept any `impl Into<VisibilityTimeoutOffset>` — how long a
//! message stays invisible to other consumers after being read. Pass a plain integer
//! (seconds), an `i64`, a `std::time::Duration`, a `chrono::Duration`, or
//! `VisibilityTimeoutOffset::seconds(i32)` if you want to be explicit.
//!
//! ```ignore
//! conn.read("q", 30).await?;
//! ```
//!
//! ## Composing with transactions
//!
//! Every adapter implements `Queue` on its driver's transaction type, so enqueue/dequeue
//! can be atomic with your own SQL. The exact incantation varies per driver — see each
//! adapter's module documentation for the pattern:
//!
//! - [`crate::adapters::sqlx`] — `tx.send(...)`
//! - [`crate::adapters::tokio_postgres`] — `tx.send(...)`
//! - [`crate::adapters::diesel`] (async) — `conn.send(...)` inside `conn.transaction(|conn| ...)`
//! - [`crate::adapters::diesel::sync`] — `conn.send(...)` inside `conn.transaction(|conn| ...)`
//!
//! ## See also
//!
//! - [`crate::install`] for installing the pgmq extension into your database
//! - `examples/transactions.rs` for a runnable sqlx-transaction example
//! - `examples/tokio_postgres_basic.rs` for tokio-postgres

#[cfg(feature = "sqlx")]
mod deprecated;
mod visibility_timeout_offest;

use crate::errors::PgmqError;
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
#[cfg(feature = "sqlx")]
#[allow(deprecated)]
pub use deprecated::PGMQueueExt;
use serde::{Deserialize, Serialize};
pub use visibility_timeout_offest::VisibilityTimeoutOffset;

/// Queue API for the `pgmq` Postgres extension. Implemented natively by each driver adapter.
/// Bring this trait into scope to call queue methods directly on your pool or transaction.
#[async_trait]
pub trait Queue {
    /// Create a queue. Idempotent.
    async fn create(self, queue_name: &str) -> Result<(), PgmqError>;

    /// Create an unlogged queue (faster but loses data on crash).
    async fn create_unlogged(self, queue_name: &str) -> Result<(), PgmqError>;

    /// Create a partitioned queue. Returns `Ok(false)` if it already exists.
    async fn create_partitioned(self, queue_name: &str) -> Result<bool, PgmqError>;

    /// Convert an archive table to a partitioned table.
    async fn convert_archive_partitioned(
        self,
        table_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
    ) -> Result<(), PgmqError>;

    /// Drop an existing queue.
    async fn drop_queue(self, queue_name: &str) -> Result<(), PgmqError>;

    /// Purge all messages from a queue. Returns the count purged.
    async fn purge_queue(self, queue_name: &str) -> Result<i64, PgmqError>;

    /// List all queues. Returns `None` if there are no queues.
    async fn list_queues(self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError>;

    /// Set the visibility timeout on an existing message.
    async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        msg_id: i64,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Message<T>, PgmqError>;

    /// Send a message. Returns its msg_id.
    async fn send<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, PgmqError>;

    /// Send a message with a delay (in seconds) before it becomes visible.
    async fn send_delay<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError>;

    /// Send a message with optional headers and a delay.
    async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError>;

    /// Send a batch of messages.
    async fn send_batch<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        messages: &[T],
    ) -> Result<Vec<i64>, PgmqError>;

    async fn send_batch_with_delay<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        messages: &[T],
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError>;

    async fn send_batch_with_delay_with_headers<
        T: Serialize + Send + Sync,
        H: Serialize + Send + Sync,
    >(
        self,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError>;

    async fn read<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Option<Message<T>>, PgmqError>;

    async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError>;

    async fn read_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Message<T>>, PgmqError>;

    async fn read_batch_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError>;

    async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError>;

    async fn read_grouped_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError>;

    async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError>;

    async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError>;

    async fn read_grouped_rr_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError>;

    async fn archive(self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError>;

    async fn archive_batch(self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError>;

    async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError>;

    async fn delete(self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError>;

    async fn delete_batch(self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError>;

    async fn create_fifo_index(self, queue_name: &str) -> Result<(), PgmqError>;
    async fn create_fifo_indexes_all(self) -> Result<(), PgmqError>;

    async fn bind_topic(self, pattern: &str, queue_name: &str) -> Result<(), PgmqError>;
    async fn unbind_topic(self, pattern: &str, queue_name: &str) -> Result<(), PgmqError>;
    async fn list_topic_bindings(
        self,
        queue_name: &str,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError>;
    async fn list_topic_bindings_all(self) -> Result<Vec<ListTopicBindingsRow>, PgmqError>;

    async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        self,
        routing_key: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i32, PgmqError>;

    async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        self,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError>;

    async fn enable_notify_insert(
        self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError>;
    async fn disable_notify_insert(self, queue_name: &str) -> Result<(), PgmqError>;
    async fn update_notify_insert(
        self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError>;
    async fn list_notify_insert_throttles(
        self,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError>;

    async fn metrics(self, queue_name: &str) -> Result<QueueMetrics, PgmqError>;
    async fn metrics_all(self) -> Result<Vec<QueueMetrics>, PgmqError>;
}

// Adapter-internal helpers (poll_*_to_*, serialize_*_list, check_input, DEFAULT_POLL_*) live
// in `crate::adapters::helpers` (private module).

/// Translate the given queue name into the name of the Postgres notification channel that will
/// be triggered when using [`Queue::enable_notify_insert`]. Listen on this channel to
/// receive notifications when an item is inserted into the queue.
///
/// # Examples
/// ```
/// # use pgmq::pg_ext::queue_name_to_insert_notification_channel_name;
/// let channel_name = queue_name_to_insert_notification_channel_name("test");
/// assert_eq!("pgmq.q_test.INSERT", channel_name);
/// ```
pub fn queue_name_to_insert_notification_channel_name(queue_name: &str) -> String {
    format!("pgmq.q_{queue_name}.INSERT")
}

/// Deprecated alias — the sqlx-specific listener helpers now live in
/// [`crate::adapters::sqlx::listener`]. Re-exported here for backward compatibility.
#[cfg(feature = "sqlx")]
#[deprecated(
    since = "0.34.0",
    note = "use `pgmq::adapters::sqlx::listener` (free functions or the `QueueListener` trait)"
)]
pub mod sqlx_listener {
    pub use crate::adapters::sqlx::listener::{queue_insert_listener, queue_insert_listener_all};
}
