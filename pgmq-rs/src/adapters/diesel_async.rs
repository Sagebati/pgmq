//! # diesel-async adapter
//!
//! Implements [`crate::PGMQueueExt`] for
//! [`&mut diesel_async::AsyncPgConnection`](diesel_async::AsyncPgConnection).
//!
//! Works with any diesel-async pool — bring your own (`deadpool`, `bb8`, etc.), acquire a
//! connection, and call pgmq methods on it.
//!
//! Methods bind params with diesel's typed `bind::<sql_types::T, _>` API and decode rows via
//! `#[derive(QueryableByName)]` on the DTOs.
//!
//! ## Cargo features
//!
//! ```toml
//! [dependencies]
//! pgmq = { version = "0.34", default-features = false, features = ["diesel-async"] }
//! diesel = { version = "2.2", default-features = false, features = ["postgres", "chrono", "serde_json"] }
//! diesel-async = { version = "0.5", features = ["postgres", "deadpool"] }
//! ```
//!
//! Add `install-sql-embedded` for the one-time installer.
//!
//! ## Install
//!
//! ```ignore
//! use diesel_async::pooled_connection::deadpool::Pool;
//! use diesel_async::pooled_connection::AsyncDieselConnectionManager;
//! use diesel_async::AsyncPgConnection;
//!
//! let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
//! let pool = Pool::builder(manager).build()?;
//!
//! pgmq::install::diesel_async::install_sql_from_embedded(&pool).await?;
//! ```
//!
//! Or `install_sql_from_github` for a specific extension version.
//!
//! ## Normal use (with a pool)
//!
//! ```ignore
//! use pgmq::PGMQueueExt;
//! use pgmq::pg_ext::VisibilityTimeoutOffset;
//!
//! let mut conn = pool.get().await?;        // Object<Manager>, derefs to &mut AsyncPgConnection
//! conn.create("orders").await?;
//!
//! let id = conn.send("orders", &my_order).await?;
//!
//! let msg: Option<pgmq::Message<MyOrder>> =
//!     conn.read("orders", VisibilityTimeoutOffset::seconds(30)).await?;
//!
//! conn.archive("orders", id).await?;
//! ```
//!
//! `&mut *conn` is a reborrow of `&mut AsyncPgConnection` for each call. The underlying
//! `conn` binding stays alive between calls.
//!
//! ## With a user-managed transaction
//!
//! diesel-async's `AsyncConnection::transaction` callback hands you `&mut AsyncPgConnection`.
//! Call pgmq methods on it directly — the trait is implemented for that exact type:
//!
//! ```ignore
//! use pgmq::PGMQueueExt;
//! use diesel_async::AsyncConnection;
//! use diesel_async::scoped_futures::ScopedFutureExt;
//! use diesel_async::RunQueryDsl;
//!
//! let mut conn = pool.get().await?;
//! conn.transaction::<_, pgmq::PgmqError, _>(|conn| async move {
//!     // Your own diesel work in the tx:
//!     diesel::sql_query("INSERT INTO orders (id, total) VALUES ($1, $2)")
//!         .bind::<diesel::sql_types::BigInt, _>(order_id)
//!         .bind::<diesel::sql_types::BigInt, _>(total)
//!         .execute(conn).await?;
//!
//!     // pgmq in the same tx — direct on conn:
//!     conn.send("orders_q", &order).await?;
//!     Ok(())
//! }.scope_boxed()).await?;
//! ```
//!
//! ## One-connection workflow (no pool)
//!
//! ```ignore
//! use diesel_async::{AsyncConnection, AsyncPgConnection};
//! let mut conn = AsyncPgConnection::establish(url).await?;
//! (&mut conn).create("q").await?;
//! (&mut conn).send("q", &payload).await?;
//! ```

use super::helpers::check_input;
use super::helpers::{
    poll_interval_to_ms, poll_timeout_to_secs, serialize_list, serialize_optional_list,
};
use super::query;
use crate::errors::PgmqError;
use crate::pg_ext::{PGMQueueExt, VisibilityTimeoutOffset};
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use diesel::pg::Pg;
use diesel::sql_query;
use diesel::sql_types;
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};

impl From<diesel_async::pooled_connection::deadpool::PoolError> for PgmqError {
    fn from(err: diesel_async::pooled_connection::deadpool::PoolError) -> Self {
        PgmqError::PoolError(err.to_string())
    }
}

// ---------------------------------------------------------------------------------------------
// Scalar return-column structs — one per shape that diesel needs to decode.
// ---------------------------------------------------------------------------------------------

#[derive(QueryableByName)]
struct SendCol {
    #[diesel(sql_type = sql_types::BigInt)]
    send: i64,
}

#[derive(QueryableByName)]
struct SendBatchCol {
    #[diesel(sql_type = sql_types::BigInt)]
    send_batch: i64,
}

#[derive(QueryableByName)]
struct SendTopicCol {
    #[diesel(sql_type = sql_types::Integer)]
    send_topic: i32,
}

#[derive(QueryableByName)]
struct ArchiveCol {
    #[diesel(sql_type = sql_types::Bool)]
    archive: bool,
}

#[derive(QueryableByName)]
struct DeleteCol {
    #[diesel(sql_type = sql_types::Bool)]
    delete: bool,
}

#[derive(QueryableByName)]
struct PurgeQueueCol {
    #[diesel(sql_type = sql_types::BigInt)]
    purge_queue: i64,
}

#[derive(QueryableByName)]
struct ExistsCol {
    #[diesel(sql_type = sql_types::Bool)]
    exists: bool,
}

// ---------------------------------------------------------------------------------------------
// Shared body that takes any `&mut AsyncPgConnection`. Used by both the pool impl (after
// `pool.get()`) and the Tx wrapper (after locking).
// ---------------------------------------------------------------------------------------------

mod imp {
    use super::*;

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::CREATE)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_unlogged(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::CREATE_UNLOGGED)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_partitioned(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = format!("pgmq.q_{queue_name}");
        let row: ExistsCol = sql_query(query::CREATE_PARTITIONED_EXISTS_CHECK)
            .bind::<sql_types::Text, _>(queue_table)
            .get_result(conn)
            .await?;
        if row.exists {
            return Ok(false);
        }
        sql_query(query::CREATE_PARTITIONED)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(true)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn convert_archive_partitioned(
        conn: &mut AsyncPgConnection,
        table_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
    ) -> Result<(), PgmqError> {
        let sql = query::convert_archive_partitioned_sql(
            partition_interval.is_some(),
            retention_interval.is_some(),
        );
        // Use into_boxed so we can chain optional binds dynamically.
        let mut q = sql_query(sql)
            .into_boxed::<Pg>()
            .bind::<sql_types::Text, _>(table_name);
        if let Some(p) = partition_interval {
            q = q.bind::<sql_types::Text, _>(p);
        }
        if let Some(r) = retention_interval {
            q = q.bind::<sql_types::Text, _>(r);
        }
        q.execute(conn).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn drop_queue(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::DROP_QUEUE)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn purge_queue(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let row: PurgeQueueCol = sql_query(query::PURGE_QUEUE)
            .bind::<sql_types::Text, _>(queue_name)
            .get_result(conn)
            .await?;
        Ok(row.purge_queue)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_queues(
        conn: &mut AsyncPgConnection,
    ) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let rows: Vec<PGMQueueMeta> = sql_query(query::LIST_QUEUES).load(conn).await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        msg_id: i64,
        vt: VisibilityTimeoutOffset,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        let row: MessageRowJson = sql_query(query::SET_VT)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::BigInt, _>(msg_id)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .get_result(conn)
            .await?;
        row.into_message()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_delay_with_headers<
        T: Serialize + Send + Sync,
        H: Serialize + Send + Sync,
    >(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: VisibilityTimeoutOffset,
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
            .bind::<sql_types::Integer, _>(delay.as_seconds())
            .get_result(conn)
            .await?;
        Ok(row.send)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_batch_with_delay_with_headers<
        T: Serialize + Send + Sync,
        H: Serialize + Send + Sync,
    >(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: VisibilityTimeoutOffset,
    ) -> Result<Vec<i64>, PgmqError> {
        check_input(queue_name)?;
        let messages = serialize_list(messages)?;
        let headers = serialize_optional_list(headers)?;
        let rows: Vec<SendBatchCol> = sql_query(query::SEND_BATCH)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Array<sql_types::Jsonb>, _>(messages)
            .bind::<sql_types::Nullable<sql_types::Array<sql_types::Jsonb>>, _>(headers)
            .bind::<sql_types::Integer, _>(delay.as_seconds())
            .load(conn)
            .await?;
        Ok(rows.into_iter().map(|r| r.send_batch).collect())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(conn)
            .await?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_batch_with_poll<
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    >(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let pt = poll_timeout_to_secs(poll_timeout);
        let pi = poll_interval_to_ms(poll_interval);
        let rows: Vec<MessageRowJson> = sql_query(query::READ_WITH_POLL)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .bind::<sql_types::Integer, _>(max_batch_size)
            .bind::<sql_types::Integer, _>(pt)
            .bind::<sql_types::Integer, _>(pi)
            .load(conn)
            .await?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(conn)
            .await?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_with_poll<
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    >(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let pt = poll_timeout_to_secs(poll_timeout);
        let pi = poll_interval_to_ms(poll_interval);
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED_WITH_POLL)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .bind::<sql_types::Integer, _>(pt)
            .bind::<sql_types::Integer, _>(pi)
            .load(conn)
            .await?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED_HEAD)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(conn)
            .await?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED_RR)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .load(conn)
            .await?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_rr_with_poll<
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    >(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let pt = poll_timeout_to_secs(poll_timeout);
        let pi = poll_interval_to_ms(poll_interval);
        let rows: Vec<MessageRowJson> = sql_query(query::READ_GROUPED_RR_WITH_POLL)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(vt.as_seconds())
            .bind::<sql_types::Integer, _>(qty)
            .bind::<sql_types::Integer, _>(pt)
            .bind::<sql_types::Integer, _>(pi)
            .load(conn)
            .await?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn archive(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        msg_id: i64,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let row: ArchiveCol = sql_query(query::ARCHIVE)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::BigInt, _>(msg_id)
            .get_result(conn)
            .await?;
        Ok(row.archive)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn archive_batch(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<ArchiveCol> = sql_query(query::ARCHIVE_BATCH)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Array<sql_types::BigInt>, _>(msg_ids)
            .load(conn)
            .await?;
        Ok(rows.len())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let rows: Vec<MessageRowJson> = sql_query(query::POP)
            .bind::<sql_types::Text, _>(queue_name)
            .load(conn)
            .await?;
        match rows.into_iter().next() {
            Some(r) => Ok(Some(r.into_message()?)),
            None => Ok(None),
        }
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn delete(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        msg_id: i64,
    ) -> Result<bool, PgmqError> {
        let row: DeleteCol = sql_query(query::DELETE)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::BigInt, _>(msg_id)
            .get_result(conn)
            .await?;
        Ok(row.delete)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn delete_batch(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<usize, PgmqError> {
        let rows: Vec<DeleteCol> = sql_query(query::DELETE_BATCH)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Array<sql_types::BigInt>, _>(msg_ids)
            .load(conn)
            .await?;
        Ok(rows.len())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_fifo_index(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<(), PgmqError> {
        sql_query(query::CREATE_FIFO_INDEX)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_fifo_indexes_all(
        conn: &mut AsyncPgConnection,
    ) -> Result<(), PgmqError> {
        sql_query(query::CREATE_FIFO_INDEXES_ALL)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn bind_topic(
        conn: &mut AsyncPgConnection,
        pattern: &str,
        queue_name: &str,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::BIND_TOPIC)
            .bind::<sql_types::Text, _>(pattern)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn unbind_topic(
        conn: &mut AsyncPgConnection,
        pattern: &str,
        queue_name: &str,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::UNBIND_TOPIC)
            .bind::<sql_types::Text, _>(pattern)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_topic_bindings(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        Ok(sql_query(query::LIST_TOPIC_BINDINGS)
            .bind::<sql_types::Text, _>(queue_name)
            .load(conn)
            .await?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_topic_bindings_all(
        conn: &mut AsyncPgConnection,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        Ok(sql_query(query::LIST_TOPIC_BINDINGS_ALL).load(conn).await?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        conn: &mut AsyncPgConnection,
        routing_key: &str,
        message: &T,
        headers: Option<&H>,
        delay: VisibilityTimeoutOffset,
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
            .bind::<sql_types::Integer, _>(delay.as_seconds())
            .get_result(conn)
            .await?;
        Ok(row.send_topic)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        conn: &mut AsyncPgConnection,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: VisibilityTimeoutOffset,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        let messages = serialize_list(messages)?;
        let headers = serialize_optional_list(headers)?;
        Ok(sql_query(query::SEND_BATCH_TOPIC)
            .bind::<sql_types::Text, _>(routing_key)
            .bind::<sql_types::Array<sql_types::Jsonb>, _>(messages)
            .bind::<sql_types::Nullable<sql_types::Array<sql_types::Jsonb>>, _>(headers)
            .bind::<sql_types::Integer, _>(delay.as_seconds())
            .load(conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn enable_notify_insert(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        throttle: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle.as_millis()).unwrap_or(i32::MAX);
        sql_query(query::ENABLE_NOTIFY_INSERT)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(ms)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn disable_notify_insert(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sql_query(query::DISABLE_NOTIFY_INSERT)
            .bind::<sql_types::Text, _>(queue_name)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn update_notify_insert(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
        throttle: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle.as_millis()).unwrap_or(i32::MAX);
        sql_query(query::UPDATE_NOTIFY_INSERT)
            .bind::<sql_types::Text, _>(queue_name)
            .bind::<sql_types::Integer, _>(ms)
            .execute(conn)
            .await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_notify_insert_throttles(
        conn: &mut AsyncPgConnection,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        Ok(sql_query(query::LIST_NOTIFY_INSERT_THROTTLES)
            .load(conn)
            .await?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn metrics(
        conn: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Result<QueueMetrics, PgmqError> {
        check_input(queue_name)?;
        Ok(sql_query(query::METRICS)
            .bind::<sql_types::Text, _>(queue_name)
            .get_result(conn)
            .await?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn metrics_all(
        conn: &mut AsyncPgConnection,
    ) -> Result<Vec<QueueMetrics>, PgmqError> {
        Ok(sql_query(query::METRICS_ALL).load(conn).await?)
    }
}

// ---------------------------------------------------------------------------------------------
// Diesel doesn't have a Message<T> equivalent that takes a generic T, so we decode the message
// column as serde_json::Value and parse T from it in Rust. One struct, one conversion fn.
// ---------------------------------------------------------------------------------------------

#[derive(QueryableByName)]
struct MessageRowJson {
    #[diesel(sql_type = sql_types::BigInt)]
    msg_id: i64,
    #[diesel(sql_type = sql_types::Integer)]
    read_ct: i32,
    #[diesel(sql_type = sql_types::Timestamptz)]
    enqueued_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = sql_types::Timestamptz)]
    vt: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = sql_types::Jsonb)]
    message: serde_json::Value,
}

impl MessageRowJson {
    fn into_message<T: for<'de> Deserialize<'de>>(self) -> Result<Message<T>, PgmqError> {
        Ok(Message {
            msg_id: self.msg_id,
            read_ct: self.read_ct,
            enqueued_at: self.enqueued_at,
            vt: self.vt,
            message: serde_json::from_value(self.message)?,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Both impls (Pool, &mut AsyncPgConnection) delegate every method to `imp::` shared body,
// varying only in how they obtain the `&mut AsyncPgConnection` to pass in. Macro factors out the
// duplication. Methods take `self` by value; auto-(re)borrow handles the reference passing.
// ---------------------------------------------------------------------------------------------

macro_rules! impl_pgmq_for_diesel {
    ($target:ty, |$self:ident| $conn:expr) => {
        #[async_trait]
        impl PGMQueueExt for $target {
            async fn create($self, queue_name: &str) -> Result<(), PgmqError> { imp::create($conn, queue_name).await }
            async fn create_unlogged($self, queue_name: &str) -> Result<(), PgmqError> { imp::create_unlogged($conn, queue_name).await }
            async fn create_partitioned($self, queue_name: &str) -> Result<bool, PgmqError> { imp::create_partitioned($conn, queue_name).await }
            async fn convert_archive_partitioned($self, t: &str, pi: Option<&str>, ri: Option<&str>) -> Result<(), PgmqError> { imp::convert_archive_partitioned($conn, t, pi, ri).await }
            async fn drop_queue($self, queue_name: &str) -> Result<(), PgmqError> { imp::drop_queue($conn, queue_name).await }
            async fn purge_queue($self, queue_name: &str) -> Result<i64, PgmqError> { imp::purge_queue($conn, queue_name).await }
            async fn list_queues($self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> { imp::list_queues($conn).await }
            async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, msg_id: i64, vt: VisibilityTimeoutOffset) -> Result<Message<T>, PgmqError> { imp::set_vt::<T>($conn, queue_name, msg_id, vt).await }
            async fn send<T: Serialize + Send + Sync>($self, queue_name: &str, message: &T) -> Result<i64, PgmqError> { $self.send_delay(queue_name, message, VisibilityTimeoutOffset::seconds(0)).await }
            async fn send_delay<T: Serialize + Send + Sync>($self, queue_name: &str, message: &T, delay: VisibilityTimeoutOffset) -> Result<i64, PgmqError> { $self.send_delay_with_headers(queue_name, message, Option::<&()>::None, delay).await }
            async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, queue_name: &str, message: &T, headers: Option<&H>, delay: VisibilityTimeoutOffset) -> Result<i64, PgmqError> { imp::send_delay_with_headers($conn, queue_name, message, headers, delay).await }
            async fn send_batch<T: Serialize + Send + Sync>($self, queue_name: &str, messages: &[T]) -> Result<Vec<i64>, PgmqError> { $self.send_batch_with_delay(queue_name, messages, VisibilityTimeoutOffset::seconds(0)).await }
            async fn send_batch_with_delay<T: Serialize + Send + Sync>($self, queue_name: &str, messages: &[T], delay: VisibilityTimeoutOffset) -> Result<Vec<i64>, PgmqError> { $self.send_batch_with_delay_with_headers(queue_name, messages, Option::<&[()]>::None, delay).await }
            async fn send_batch_with_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, queue_name: &str, messages: &[T], headers: Option<&[H]>, delay: VisibilityTimeoutOffset) -> Result<Vec<i64>, PgmqError> { imp::send_batch_with_delay_with_headers($conn, queue_name, messages, headers, delay).await }
            async fn read<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset) -> Result<Option<Message<T>>, PgmqError> { Ok($self.read_batch::<T>(queue_name, vt, 1).await?.into_iter().next()) }
            async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_batch::<T>($conn, queue_name, vt, qty).await }
            async fn read_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Option<Message<T>>, PgmqError> { Ok($self.read_batch_with_poll::<T>(queue_name, vt, 1, pt, pi).await?.and_then(|v| v.into_iter().next())) }
            async fn read_batch_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Option<Vec<Message<T>>>, PgmqError> { Ok(Some(imp::read_batch_with_poll::<T>($conn, queue_name, vt, qty, pt, pi).await?)) }
            async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped::<T>($conn, queue_name, vt, qty).await }
            async fn read_grouped_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_with_poll::<T>($conn, queue_name, vt, qty, pt, pi).await }
            async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_head::<T>($conn, queue_name, vt, qty).await }
            async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_rr::<T>($conn, queue_name, vt, qty).await }
            async fn read_grouped_rr_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_rr_with_poll::<T>($conn, queue_name, vt, qty, pt, pi).await }
            async fn archive($self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> { imp::archive($conn, queue_name, msg_id).await }
            async fn archive_batch($self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> { imp::archive_batch($conn, queue_name, msg_ids).await }
            async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str) -> Result<Option<Message<T>>, PgmqError> { imp::pop::<T>($conn, queue_name).await }
            async fn delete($self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> { imp::delete($conn, queue_name, msg_id).await }
            async fn delete_batch($self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> { imp::delete_batch($conn, queue_name, msg_ids).await }
            async fn create_fifo_index($self, queue_name: &str) -> Result<(), PgmqError> { imp::create_fifo_index($conn, queue_name).await }
            async fn create_fifo_indexes_all($self) -> Result<(), PgmqError> { imp::create_fifo_indexes_all($conn).await }
            async fn bind_topic($self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> { imp::bind_topic($conn, pattern, queue_name).await }
            async fn unbind_topic($self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> { imp::unbind_topic($conn, pattern, queue_name).await }
            async fn list_topic_bindings($self, queue_name: &str) -> Result<Vec<ListTopicBindingsRow>, PgmqError> { imp::list_topic_bindings($conn, queue_name).await }
            async fn list_topic_bindings_all($self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> { imp::list_topic_bindings_all($conn).await }
            async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, rk: &str, m: &T, h: Option<&H>, d: VisibilityTimeoutOffset) -> Result<i32, PgmqError> { imp::send_topic($conn, rk, m, h, d).await }
            async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, rk: &str, m: &[T], h: Option<&[H]>, d: VisibilityTimeoutOffset) -> Result<Vec<SendBatchTopicRow>, PgmqError> { imp::send_batch_topic($conn, rk, m, h, d).await }
            async fn enable_notify_insert($self, queue_name: &str, t: std::time::Duration) -> Result<(), PgmqError> { imp::enable_notify_insert($conn, queue_name, t).await }
            async fn disable_notify_insert($self, queue_name: &str) -> Result<(), PgmqError> { imp::disable_notify_insert($conn, queue_name).await }
            async fn update_notify_insert($self, queue_name: &str, t: std::time::Duration) -> Result<(), PgmqError> { imp::update_notify_insert($conn, queue_name, t).await }
            async fn list_notify_insert_throttles($self) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> { imp::list_notify_insert_throttles($conn).await }
            async fn metrics($self, queue_name: &str) -> Result<QueueMetrics, PgmqError> { imp::metrics($conn, queue_name).await }
            async fn metrics_all($self) -> Result<Vec<QueueMetrics>, PgmqError> { imp::metrics_all($conn).await }
        }
    };
}

// &mut AsyncPgConnection: the only impl — works with any pool (user acquires explicitly), with
// `conn.transaction(|conn| async move { conn.send(...).await })`, or with a bare connection.
impl_pgmq_for_diesel!(&mut AsyncPgConnection, |self| self);

// Suppress unused-import warning for ScopedFutureExt; consumers use it in their own code.
#[allow(dead_code)]
fn _hint_scoped_futures<F>(_: F)
where
    F: ScopedFutureExt + Sized,
{
}
