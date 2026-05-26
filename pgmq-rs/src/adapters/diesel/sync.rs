//! # diesel (sync) adapter
//!
//! Implements [`crate::Queue`] for [`&mut diesel::pg::PgConnection`](diesel::pg::PgConnection).
//!
//! The trait signature is `async fn`, but diesel's sync `PgConnection` runs everything
//! synchronously. Method bodies execute diesel I/O on the calling thread; the returned future
//! is **ready on first poll**.
//!
//! **This blocks the calling thread.** In async code (e.g. tokio), wrap calls in
//! [`tokio::task::spawn_blocking`] so you don't stall the executor. In sync code, drive the
//! future with any block-on helper (tokio runtime, [pollster][pollster], or a hand-rolled
//! noop-waker poll).
//!
//! [pollster]: https://docs.rs/pollster
//!
//! ## Cargo features
//!
//! ```toml
//! [dependencies]
//! pgmq = { version = "0.34", default-features = false, features = ["diesel-sync"] }
//! diesel = { version = "2.2", features = ["postgres", "chrono", "serde_json"] }
//! ```
//!
//! Add `install-sql-embedded` for the one-time installer. `install-sql-github` is **not
//! supported** for sync diesel (it would need an async HTTP path); if you need it, use the
//! `diesel-async` adapter inside a small tokio runtime instead.
//!
//! ## Install
//!
//! Synchronous — no async runtime needed:
//!
//! ```ignore
//! use diesel::Connection;
//! let mut conn = diesel::pg::PgConnection::establish("postgres://...")?;
//! pgmq::install::diesel_sync::install_sql_from_embedded(&mut conn)?;
//! ```
//!
//! ## Normal use (no async runtime, with `block_on` helper)
//!
//! ```ignore
//! use pgmq::Queue;
//! use pgmq::pg_ext::VisibilityTimeoutOffset;
//! use diesel::Connection;
//!
//! // Sync code can drive ready-on-first-poll futures with a minimal block-on. Example using
//! // a noop waker (no runtime dependency):
//! fn block_on<F: std::future::Future>(f: F) -> F::Output {
//!     use std::future::Future;
//!     use std::pin::Pin;
//!     use std::task::{Context, Poll, Waker};
//!     let waker = Waker::noop();
//!     let mut cx = Context::from_waker(waker);
//!     let mut f = Box::pin(f);
//!     match Pin::new(&mut f).as_mut().poll(&mut cx) {
//!         Poll::Ready(v) => v,
//!         Poll::Pending => panic!("sync diesel future should be ready on first poll"),
//!     }
//! }
//!
//! let mut conn = diesel::pg::PgConnection::establish(url)?;
//! block_on((&mut conn).create("orders"))?;
//! let id = block_on((&mut conn).send("orders", &my_order))?;
//! let msg: Option<pgmq::Message<MyOrder>> =
//!     block_on((&mut conn).read("orders", 30))?;
//! block_on((&mut conn).archive("orders", id))?;
//! ```
//!
//! Or use [pollster][pollster]'s `block_on`, or tokio's runtime if you already have one.
//!
//! ## With a pool (r2d2, deadpool-diesel, etc.)
//!
//! Acquire a connection from your pool, then call methods on `&mut *conn`:
//!
//! ```ignore
//! use diesel::r2d2::{ConnectionManager, Pool};
//! let manager = ConnectionManager::<diesel::pg::PgConnection>::new(url);
//! let pool = Pool::builder().build(manager)?;
//!
//! let mut conn = pool.get()?;                // r2d2::PooledConnection
//! block_on(conn.create("q"))?;
//! ```
//!
//! ## With a user-managed transaction
//!
//! diesel's sync `Connection::transaction(|conn| { ... })` callback hands you
//! `&mut PgConnection`. Call pgmq methods on it directly inside the closure:
//!
//! ```ignore
//! use pgmq::Queue;
//! use diesel::{Connection, RunQueryDsl};
//!
//! conn.transaction::<_, pgmq::PgmqError, _>(|conn| {
//!     diesel::sql_query("INSERT INTO orders (id, total) VALUES ($1, $2)")
//!         .bind::<diesel::sql_types::BigInt, _>(order_id)
//!         .bind::<diesel::sql_types::BigInt, _>(total)
//!         .execute(conn)?;
//!     block_on(conn.send("orders_q", &order))?;
//!     Ok(())
//! })?;
//! ```
//!
//! ## In async code (tokio): wrap with `spawn_blocking`
//!
//! Calling sync diesel inside an async fn blocks the executor thread. The correct pattern is
//! to move the connection into a blocking task:
//!
//! ```ignore
//! let mut conn = pool.get()?;
//! let result = tokio::task::spawn_blocking(move || {
//!     // sync code here — uses the block_on helper above or any other
//!     block_on(async move { (&mut conn).create("q").await })
//! }).await??;
//! ```
//!
//! For most use cases where you're already in async land, prefer the [diesel-async][async]
//! adapter (the top-level [`crate::adapters::diesel`] module).
//!
//! [async]: crate::adapters::diesel

use super::{
    ArchiveCol, DeleteCol, ExistsCol, MessageRowJson, PurgeQueueCol, SendBatchCol, SendCol,
    SendTopicCol,
};
use crate::adapters::helpers::check_input;
use crate::adapters::helpers::{
    poll_interval_ms, poll_timeout_secs, serialize_list, serialize_optional_list,
};
use crate::adapters::query;
use crate::errors::PgmqError;
use crate::pg_ext::{Queue, VisibilityTimeoutOffset};
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use diesel::pg::{Pg, PgConnection};
use diesel::{sql_query, sql_types, OptionalExtension, RunQueryDsl};
use serde::{Deserialize, Serialize};

#[async_trait]
impl Queue for &mut PgConnection {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::CREATE)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_unlogged(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::CREATE_UNLOGGED)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_partitioned(self, queue_name: &str) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = format!("pgmq.q_{queue_name}");
        let row: ExistsCol = sql_query(query::CREATE_PARTITIONED_EXISTS_CHECK)
            .bind::<sql_types::Text, _>(queue_table)
            .get_result(&mut *self)?;
        if row.exists {
            return Ok(false);
        }
        sql_query(query::CREATE_PARTITIONED)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(true)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn convert_archive_partitioned(
        self,
        table_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
    ) -> Result<(), PgmqError> {
        let sql = query::convert_archive_partitioned_sql(
            partition_interval.is_some(),
            retention_interval.is_some(),
        );
        let mut q = sql_query(sql)
            .into_boxed::<Pg>()
            .bind::<sql_types::Text, _>(table_name);
        if let Some(p) = partition_interval {
            q = q.bind::<sql_types::Text, _>(p);
        }
        if let Some(r) = retention_interval {
            q = q.bind::<sql_types::Text, _>(r);
        }
        q.execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn drop_queue(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::DROP_QUEUE)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn purge_queue(self, queue_name: &str) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let row: PurgeQueueCol = sql_query(query::PURGE_QUEUE)
            .bind::<sql_types::Text, _>(queue_name)
            .get_result(self)?;
        Ok(row.purge_queue)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_queues(self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let rows: Vec<PGMQueueMeta> = sql_query(query::LIST_QUEUES).load(self)?;
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        msg_id: i64,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        let row: MessageRowJson = sql_query(query::SET_VT)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::BigInt, _>(msg_id)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .get_result(self)?;
        row.into_message()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let message = serde_json::to_value(message)?;
        let headers = match headers {
            Some(h) => Some(serde_json::to_value(h)?),
            None => None,
        };
        let row: SendCol = sql_query(query::SEND)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Jsonb, _>(message)
            .bind::<sql_types::Nullable<sql_types::Jsonb>, _>(headers)
            .bind::<sql_types::Integer, _>(delay.into().as_seconds())
            .get_result(self)?;
        Ok(row.send)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn send_batch_with_delay_with_headers<
        T: Serialize + Send + Sync,
        H: Serialize + Send + Sync,
    >(
        self,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError> {
        check_input(queue_name)?;
        let messages = serialize_list(messages)?;
        let headers = serialize_optional_list(headers)?;
        let rows: Vec<SendBatchCol> = sql_query(query::SEND_BATCH)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Array<sql_types::Jsonb>, _>(messages)
            .bind::<sql_types::Nullable<sql_types::Array<sql_types::Jsonb>>, _>(headers)
            .bind::<sql_types::Integer, _>(delay.into().as_seconds())
            .load(self)?;
        Ok(rows.into_iter().map(|r| r.send_batch).collect())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(self)?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_batch_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        check_input(queue_name)?;
        let sql = query::read_with_poll_sql(poll_timeout.is_some(), poll_interval.is_some());
        let mut q = sql_query(sql)
            .into_boxed::<Pg>()
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .bind::<sql_types::Integer, _>(max_batch_size);
        if let Some(t) = poll_timeout {
            q = q.bind::<sql_types::Integer, _>(poll_timeout_secs(t));
        }
        if let Some(i) = poll_interval {
            q = q.bind::<sql_types::Integer, _>(poll_interval_ms(i));
        }
        let rows: Vec<MessageRowJson> = q.load(self)?;
        Ok(Some(
            rows.into_iter()
                .map(MessageRowJson::into_message)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(self)?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let sql =
            query::read_grouped_with_poll_sql(poll_timeout.is_some(), poll_interval.is_some());
        let mut q = sql_query(sql)
            .into_boxed::<Pg>()
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .bind::<sql_types::Integer, _>(qty);
        if let Some(t) = poll_timeout {
            q = q.bind::<sql_types::Integer, _>(poll_timeout_secs(t));
        }
        if let Some(i) = poll_interval {
            q = q.bind::<sql_types::Integer, _>(poll_interval_ms(i));
        }
        let rows: Vec<MessageRowJson> = q.load(self)?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED_HEAD)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(self)?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED_RR)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(self)?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped_rr_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let sql =
            query::read_grouped_rr_with_poll_sql(poll_timeout.is_some(), poll_interval.is_some());
        let mut q = sql_query(sql)
            .into_boxed::<Pg>()
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(visibility_timeout.into().as_seconds())
            .bind::<sql_types::Integer, _>(qty);
        if let Some(t) = poll_timeout {
            q = q.bind::<sql_types::Integer, _>(poll_timeout_secs(t));
        }
        if let Some(i) = poll_interval {
            q = q.bind::<sql_types::Integer, _>(poll_interval_ms(i));
        }
        let rows: Vec<MessageRowJson> = q.load(self)?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn archive(self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let row: ArchiveCol = sql_query(query::ARCHIVE)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::BigInt, _>(msg_id)
            .get_result(self)?;
        Ok(row.archive)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn archive_batch(self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<ArchiveCol> = sql_query(query::ARCHIVE_BATCH)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Array<sql_types::BigInt>, _>(msg_ids)
            .load(self)?;
        Ok(rows.len())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row: Option<MessageRowJson> = sql_query(query::POP)
            .bind::<sql_types::Text, _>(queue_name)
            .get_result(self)
            .optional()?;
        match row {
            Some(r) => Ok(Some(r.into_message()?)),
            None => Ok(None),
        }
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn delete(self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let row: DeleteCol = sql_query(query::DELETE)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::BigInt, _>(msg_id)
            .get_result(self)?;
        Ok(row.was_deleted)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn delete_batch(self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<DeleteCol> = sql_query(query::DELETE_BATCH)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Array<sql_types::BigInt>, _>(msg_ids)
            .load(self)?;
        Ok(rows.len())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_fifo_index(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::CREATE_FIFO_INDEX)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_fifo_indexes_all(self) -> Result<(), PgmqError> {
        sql_query(query::CREATE_FIFO_INDEXES_ALL).execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn bind_topic(self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::BIND_TOPIC)
            .bind::<sql_types::Text, _>(pattern)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn unbind_topic(self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::UNBIND_TOPIC)
            .bind::<sql_types::Text, _>(pattern)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_topic_bindings(
        self,
        queue_name: &str,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        Ok(sql_query(query::LIST_TOPIC_BINDINGS)
            .bind::<sql_types::Text, _>(queue_name)
            .load(self)?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_topic_bindings_all(self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        Ok(sql_query(query::LIST_TOPIC_BINDINGS_ALL).load(self)?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        self,
        routing_key: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i32, PgmqError> {
        let message = serde_json::to_value(message)?;
        let headers = match headers {
            Some(h) => Some(serde_json::to_value(h)?),
            None => None,
        };
        let row: SendTopicCol = sql_query(query::SEND_TOPIC)
            .bind::<sql_types::Text, _>(routing_key)
            .bind::<sql_types::Jsonb, _>(message)
            .bind::<sql_types::Nullable<sql_types::Jsonb>, _>(headers)
            .bind::<sql_types::Integer, _>(delay.into().as_seconds())
            .get_result(self)?;
        Ok(row.send_topic)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        self,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        let messages = serialize_list(messages)?;
        let headers = serialize_optional_list(headers)?;
        Ok(sql_query(query::SEND_BATCH_TOPIC)
            .bind::<sql_types::Text, _>(routing_key)
            .bind::<sql_types::Array<sql_types::Jsonb>, _>(messages)
            .bind::<sql_types::Nullable<sql_types::Array<sql_types::Jsonb>>, _>(headers)
            .bind::<sql_types::Integer, _>(delay.into().as_seconds())
            .load(self)?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn enable_notify_insert(
        self,
        queue_name: &str,
        throttle: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle.as_millis()).unwrap_or(i32::MAX);
        sql_query(query::ENABLE_NOTIFY_INSERT)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(ms)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn disable_notify_insert(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::DISABLE_NOTIFY_INSERT)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn update_notify_insert(
        self,
        queue_name: &str,
        throttle: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle.as_millis()).unwrap_or(i32::MAX);
        sql_query(query::UPDATE_NOTIFY_INSERT)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(ms)
            .execute(self)?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_notify_insert_throttles(
        self,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        Ok(sql_query(query::LIST_NOTIFY_INSERT_THROTTLES).load(self)?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn metrics(self, queue_name: &str) -> Result<QueueMetrics, PgmqError> {
        check_input(queue_name)?;
        Ok(sql_query(query::METRICS)
            .bind::<sql_types::Text, _>(queue_name)
            .get_result(self)?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn metrics_all(self) -> Result<Vec<QueueMetrics>, PgmqError> {
        Ok(sql_query(query::METRICS_ALL).load(self)?)
    }
}
