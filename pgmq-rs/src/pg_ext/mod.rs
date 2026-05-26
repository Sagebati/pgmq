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

mod visibility_timeout_offest;

use crate::errors::PgmqError;
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
#[cfg(feature = "sqlx")]
use sqlx::postgres::PgPoolOptions;
#[cfg(feature = "sqlx")]
use sqlx::{Pool, Postgres};
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
    ///
    /// Convenience over [`Self::send_delay_with_headers`] with a zero delay and no headers;
    /// every driver gets the same default body. Adapters can override for a faster path,
    /// but none currently do.
    async fn send<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, PgmqError>
    where
        Self: Sized,
    {
        self.send_delay(queue_name, message, VisibilityTimeoutOffset::seconds(0))
            .await
    }

    /// Send a message with a delay (in seconds) before it becomes visible.
    ///
    /// Convenience over [`Self::send_delay_with_headers`] without headers.
    async fn send_delay<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError>
    where
        Self: Sized,
    {
        self.send_delay_with_headers(queue_name, message, Option::<&()>::None, delay)
            .await
    }

    /// Send a message with optional headers and a delay.
    async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError>;

    /// Send a batch of messages.
    ///
    /// Convenience over [`Self::send_batch_with_delay_with_headers`] with zero delay and no
    /// headers.
    async fn send_batch<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        messages: &[T],
    ) -> Result<Vec<i64>, PgmqError>
    where
        Self: Sized,
    {
        self.send_batch_with_delay(queue_name, messages, VisibilityTimeoutOffset::seconds(0))
            .await
    }

    /// Send a batch of messages with a delay.
    ///
    /// Convenience over [`Self::send_batch_with_delay_with_headers`] without headers.
    async fn send_batch_with_delay<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        messages: &[T],
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError>
    where
        Self: Sized,
    {
        self.send_batch_with_delay_with_headers(queue_name, messages, Option::<&[()]>::None, delay)
            .await
    }

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

    /// Read one message — convenience over [`Self::read_batch`] with `qty = 1`.
    async fn read<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Option<Message<T>>, PgmqError>
    where
        Self: Sized,
    {
        Ok(self
            .read_batch::<T>(queue_name, visibility_timeout, 1)
            .await?
            .into_iter()
            .next())
    }

    async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError>;

    /// Read one message, blocking on the server until one is available or `poll_timeout`
    /// elapses. Convenience over [`Self::read_batch_with_poll`] with `max_batch_size = 1`.
    async fn read_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Message<T>>, PgmqError>
    where
        Self: Sized,
    {
        Ok(self
            .read_batch_with_poll::<T>(
                queue_name,
                visibility_timeout,
                1,
                poll_timeout,
                poll_interval,
            )
            .await?
            .and_then(|rows| rows.into_iter().next()))
    }

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

// ---------------------------------------------------------------------------------------------
// Deprecated `PGMQueueExt` struct — backward-compat shim for the pre-0.34 sqlx-only API.
//
// New code should use the [`Queue`] trait directly on a sqlx pool, connection, or transaction.
// This struct exists so existing callers compile against `0.34.0-alpha` without changes; all
// methods forward to the trait implementation on the inner [`sqlx::PgPool`].
//
// The `_with_cxn` family from prior releases is removed — call the [`Queue`] trait methods on
// your own executor instead (the trait is implemented on `&PgPool`, `&mut PgConnection`, and
// `&mut Transaction<'_, Postgres>`).
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "sqlx")]
#[deprecated(
    since = "0.34.0",
    note = "use the `pgmq::Queue` trait directly on a sqlx pool, connection, or transaction. PGMQueueExt is a backward-compat shim and will be removed in a future release."
)]
#[derive(Clone, Debug)]
pub struct PGMQueueExt {
    pub url: String,
    pub connection: Pool<Postgres>,
}

#[cfg(feature = "sqlx")]
#[allow(deprecated)]
impl PGMQueueExt {
    /// Initialize a connection to PGMQ/Postgres. Builds an internal `PgPool` with the given
    /// max-connection cap.
    #[deprecated(
        since = "0.34.0",
        note = "build your own `sqlx::PgPool` and call the `Queue` trait on it"
    )]
    pub async fn new(url: String, max_connections: u32) -> Result<Self, PgmqError> {
        use std::str::FromStr;
        let opts = sqlx::postgres::PgConnectOptions::from_str(&url)
            .map_err(|e| PgmqError::UrlParseError(e.to_string()))?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(opts)
            .await?;
        Ok(Self {
            url,
            connection: pool,
        })
    }

    /// Bring-your-own-pool variant — no connection establishment, just wraps an existing pool.
    #[deprecated(
        since = "0.34.0",
        note = "the `Queue` trait is implemented directly on `&sqlx::PgPool` — no wrapper needed"
    )]
    pub async fn new_with_pool(pool: Pool<Postgres>) -> Self {
        Self {
            url: String::new(),
            connection: pool,
        }
    }

    /// Install pgmq via `CREATE EXTENSION` (advisory-lock-serialized).
    #[cfg(feature = "install-sql")]
    #[deprecated = "use `pgmq::install::sqlx::init`"]
    pub async fn init(&self) -> Result<bool, PgmqError> {
        crate::install::sqlx::init(&self.connection).await?;
        Ok(true)
    }

    #[cfg(feature = "install-sql")]
    #[deprecated = "use `pgmq::install::sqlx::init_migrations_table`"]
    pub async fn init_migrations_table(&self, version: &str) -> Result<(), PgmqError> {
        use std::str::FromStr;
        crate::install::sqlx::init_migrations_table(
            &self.connection,
            crate::install::Version::from_str(version)?,
        )
        .await
    }

    #[cfg(feature = "install-sql")]
    #[deprecated = "use `pgmq::install::sqlx::installed_version`"]
    pub async fn installed_version(&self) -> Result<Option<crate::install::Version>, PgmqError> {
        crate::install::sqlx::installed_version(&self.connection).await
    }

    #[cfg(feature = "install-sql-github")]
    #[deprecated = "use `pgmq::install::sqlx::install_sql_from_github`"]
    pub async fn install_sql_from_github(&self, version: Option<&str>) -> Result<(), PgmqError> {
        crate::install::sqlx::install_sql_from_github(&self.connection, version).await
    }

    #[cfg(feature = "install-sql-embedded")]
    #[deprecated = "use `pgmq::install::sqlx::install_sql_from_embedded`"]
    pub async fn install_sql_from_embedded(&self) -> Result<(), PgmqError> {
        crate::install::sqlx::install_sql_from_embedded(&self.connection).await
    }

    /// Acquire a transaction-level advisory lock specific to the provided queue. Begins a new
    /// transaction; the caller is responsible for committing it.
    #[deprecated = "begin your own transaction with `pool.begin()` and run `SELECT pgmq.acquire_queue_lock(...)` against it"]
    pub async fn acquire_queue_lock<'c>(
        &self,
        queue_name: &str,
    ) -> Result<sqlx::Transaction<'c, Postgres>, PgmqError> {
        let mut txn = self.connection.begin().await?;
        sqlx::query("SELECT pgmq.acquire_queue_lock(queue_name=>$1::text);")
            .bind(queue_name)
            .execute(&mut *txn)
            .await?;
        Ok(txn)
    }

    /// Create a queue. Returns `Ok(true)` (the underlying SQL is idempotent — the old return
    /// value was nominal).
    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn create(&self, queue_name: &str) -> Result<bool, PgmqError> {
        (&self.connection).create(queue_name).await?;
        Ok(true)
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn create_unlogged(&self, queue_name: &str) -> Result<bool, PgmqError> {
        (&self.connection).create_unlogged(queue_name).await?;
        Ok(true)
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn create_partitioned(&self, queue_name: &str) -> Result<bool, PgmqError> {
        (&self.connection).create_partitioned(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn convert_archive_partitioned(
        &self,
        table_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
    ) -> Result<(), PgmqError> {
        (&self.connection)
            .convert_archive_partitioned(table_name, partition_interval, retention_interval)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn drop_queue(&self, queue_name: &str) -> Result<(), PgmqError> {
        (&self.connection).drop_queue(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn purge_queue(&self, queue_name: &str) -> Result<i64, PgmqError> {
        (&self.connection).purge_queue(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn list_queues(&self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        (&self.connection).list_queues().await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        msg_id: i64,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Message<T>, PgmqError> {
        (&self.connection)
            .set_vt(queue_name, msg_id, visibility_timeout)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send<T: Serialize + Send + Sync>(
        &self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, PgmqError> {
        (&self.connection).send(queue_name, message).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send_delay<T: Serialize + Send + Sync>(
        &self,
        queue_name: &str,
        message: &T,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError> {
        (&self.connection)
            .send_delay(queue_name, message, delay)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        &self,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError> {
        (&self.connection)
            .send_delay_with_headers(queue_name, message, headers, delay)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send_batch<T: Serialize + Send + Sync>(
        &self,
        queue_name: &str,
        messages: &[T],
    ) -> Result<Vec<i64>, PgmqError> {
        (&self.connection).send_batch(queue_name, messages).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send_batch_with_delay<T: Serialize + Send + Sync>(
        &self,
        queue_name: &str,
        messages: &[T],
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError> {
        (&self.connection)
            .send_batch_with_delay(queue_name, messages, delay)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send_batch_with_delay_with_headers<
        T: Serialize + Send + Sync,
        H: Serialize + Send + Sync,
    >(
        &self,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError> {
        (&self.connection)
            .send_batch_with_delay_with_headers(queue_name, messages, headers, delay)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Option<Message<T>>, PgmqError> {
        (&self.connection)
            .read(queue_name, visibility_timeout)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        (&self.connection)
            .read_batch(queue_name, visibility_timeout, qty)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Message<T>>, PgmqError> {
        (&self.connection)
            .read_with_poll(queue_name, visibility_timeout, poll_timeout, poll_interval)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_batch_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        (&self.connection)
            .read_batch_with_poll(
                queue_name,
                visibility_timeout,
                max_batch_size,
                poll_timeout,
                poll_interval,
            )
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        (&self.connection)
            .read_grouped(queue_name, visibility_timeout, qty)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_grouped_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        (&self.connection)
            .read_grouped_with_poll(
                queue_name,
                visibility_timeout,
                qty,
                poll_timeout,
                poll_interval,
            )
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        (&self.connection)
            .read_grouped_head(queue_name, visibility_timeout, qty)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        (&self.connection)
            .read_grouped_rr(queue_name, visibility_timeout, qty)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn read_grouped_rr_with_poll<
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    >(
        &self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        (&self.connection)
            .read_grouped_rr_with_poll(
                queue_name,
                visibility_timeout,
                qty,
                poll_timeout,
                poll_interval,
            )
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn archive(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        (&self.connection).archive(queue_name, msg_id).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn archive_batch(
        &self,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<usize, PgmqError> {
        (&self.connection).archive_batch(queue_name, msg_ids).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        (&self.connection).pop(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn delete(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        (&self.connection).delete(queue_name, msg_id).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn delete_batch(
        &self,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<usize, PgmqError> {
        (&self.connection).delete_batch(queue_name, msg_ids).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn create_fifo_index(&self, queue_name: &str) -> Result<(), PgmqError> {
        (&self.connection).create_fifo_index(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn create_fifo_indexes_all(&self) -> Result<(), PgmqError> {
        (&self.connection).create_fifo_indexes_all().await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn bind_topic(&self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        (&self.connection).bind_topic(pattern, queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn unbind_topic(&self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        (&self.connection).unbind_topic(pattern, queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn list_topic_bindings(
        &self,
        queue_name: &str,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        (&self.connection).list_topic_bindings(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn list_topic_bindings_all(&self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        (&self.connection).list_topic_bindings_all().await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        &self,
        routing_key: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i32, PgmqError> {
        (&self.connection)
            .send_topic(routing_key, message, headers, delay)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        &self,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        (&self.connection)
            .send_batch_topic(routing_key, messages, headers, delay)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn enable_notify_insert(
        &self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        (&self.connection)
            .enable_notify_insert(queue_name, throttle_interval)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn disable_notify_insert(&self, queue_name: &str) -> Result<(), PgmqError> {
        (&self.connection).disable_notify_insert(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn update_notify_insert(
        &self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        (&self.connection)
            .update_notify_insert(queue_name, throttle_interval)
            .await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn list_notify_insert_throttles(
        &self,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        (&self.connection).list_notify_insert_throttles().await
    }

    #[deprecated = "use `pgmq::pg_ext::sqlx_listener::queue_insert_listener`"]
    pub async fn queue_insert_listener(
        &self,
        queue_name: &str,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        crate::pg_ext::sqlx_listener::queue_insert_listener(&self.connection, queue_name).await
    }

    #[deprecated = "use `pgmq::pg_ext::sqlx_listener::queue_insert_listener_all`"]
    pub async fn queue_insert_listener_all<'a>(
        &self,
        queue_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        crate::pg_ext::sqlx_listener::queue_insert_listener_all(&self.connection, queue_names).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn metrics(&self, queue_name: &str) -> Result<QueueMetrics, PgmqError> {
        (&self.connection).metrics(queue_name).await
    }

    #[deprecated = "use the `pgmq::Queue` trait on your sqlx pool/connection/transaction directly"]
    pub async fn metrics_all(&self) -> Result<Vec<QueueMetrics>, PgmqError> {
        (&self.connection).metrics_all().await
    }
}
