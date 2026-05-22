//! # tokio-postgres adapter
//!
//! Implements [`crate::PGMQueueExt`] for:
//! - [`&tokio_postgres::Client`](tokio_postgres::Client) — a client (typically acquired from a pool)
//! - [`&tokio_postgres::Transaction<'_>`](tokio_postgres::Transaction) — a user-managed transaction
//!
//! Pgmq has **no opinion on which pool you use**. The impl is on the connection (`Client`), so
//! [deadpool-postgres](https://docs.rs/deadpool-postgres/), [bb8](https://docs.rs/bb8/),
//! [mobc](https://docs.rs/mobc/), a custom pool, or a one-shot client all work — bring your
//! own pool, acquire a client, and call pgmq methods on it.
//!
//! Methods bind params directly to tokio-postgres' `&[&(dyn ToSql + Sync)]` slice and decode
//! rows into typed DTOs via hand-written `from_tokio_postgres_row` constructors on each DTO.
//!
//! ## Cargo features
//!
//! ```toml
//! [dependencies]
//! pgmq = { version = "0.34", default-features = false, features = ["tokio-postgres"] }
//! tokio-postgres = "0.7"
//! deadpool-postgres = "0.14"   # or whatever pool you prefer
//! ```
//!
//! For one-time install of the pgmq extension into your database:
//!
//! ```toml
//! pgmq = { version = "0.34", default-features = false, features = ["tokio-postgres", "install-sql-embedded"] }
//! ```
//!
//! ## Install
//!
//! The install module takes `&mut tokio_postgres::Client`. Acquire a client from your pool
//! (or open one directly), then pass it in. Run once per database (idempotent):
//!
//! ```ignore
//! let mut client = pool.get().await?;
//! pgmq::install::tokio_postgres::install_sql_from_embedded(&mut **client).await?;
//! ```
//!
//! With a non-pooled connection:
//!
//! ```ignore
//! let (mut client, conn) = tokio_postgres::connect(url, NoTls).await?;
//! tokio::spawn(async move { conn.await.ok(); });
//! pgmq::install::tokio_postgres::install_sql_from_embedded(&mut client).await?;
//! ```
//!
//! Or `install_sql_from_github` to fetch a specific version.
//!
//! ## Normal use (with a pool)
//!
//! Acquire a client from your pool and call pgmq methods on it.
//!
//! ```ignore
//! use pgmq::PGMQueueExt;
//! use pgmq::pg_ext::VisibilityTimeoutOffset;
//!
//! let client = pool.get().await?;             // deadpool_postgres::Client
//! client.create("orders").await?;        // deref through Object<Manager>+ClientWrapper to &Client
//!
//! let id = client.send("orders", &my_order).await?;
//!
//! let msg: Option<pgmq::Message<MyOrder>> =
//!     client.read("orders", VisibilityTimeoutOffset::seconds(30)).await?;
//!
//! client.archive("orders", id).await?;
//! ```
//!
//! The `&**client` dance is because deadpool's `Object<Manager>` derefs through `ClientWrapper`
//! to `tokio_postgres::Client`. With bb8 or mobc the exact deref chain differs slightly. In
//! a tight loop you can bind it once: `let c: &tokio_postgres::Client = &**client;`.
//!
//! ## With a user-managed transaction
//!
//! Compose pgmq with your own SQL atomically:
//!
//! ```ignore
//! use pgmq::PGMQueueExt;
//!
//! let mut client = pool.get().await?;
//! let tx = client.transaction().await?;
//!
//! // Your own SQL:
//! tx.execute("INSERT INTO orders (id, total) VALUES ($1, $2)",
//!            &[&order_id, &total]).await?;
//!
//! // pgmq in the same tx — call it on `&tx`:
//! tx.send("orders_queue", &order).await?;
//!
//! tx.commit().await?;
//! ```
//!
//! ## One-shot client (no pool)
//!
//! tokio-postgres lets you connect without a pool. Same API.
//!
//! ```ignore
//! let (client, conn) = tokio_postgres::connect(url, NoTls).await?;
//! tokio::spawn(async move { conn.await.ok(); });
//!
//! (&client).create("q").await?;
//! (&client).send("q", &payload).await?;
//! ```

use crate::errors::PgmqError;
use crate::pg_ext::{PGMQueueExt, VisibilityTimeoutOffset};
use super::helpers::{poll_interval_to_ms, poll_timeout_to_secs, serialize_list, serialize_optional_list};
use super::query;
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use super::helpers::check_input;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::ToSql;
use tokio_postgres::GenericClient;

impl From<tokio_postgres::Error> for PgmqError {
    fn from(err: tokio_postgres::Error) -> Self {
        PgmqError::DatabaseError(err.to_string())
    }
}

// ---------------------------------------------------------------------------------------------
// Private inherent row decoders on the DTOs. Inherent methods defined in this module are not
// visible outside it — keeps `from_tokio_postgres_row` out of the public API on the DTOs.
// ---------------------------------------------------------------------------------------------

fn col<T>(
    res: Result<T, tokio_postgres::Error>,
    col_name: &str,
) -> Result<T, PgmqError> {
    res.map_err(|e| PgmqError::RowDecodeError {
        column: col_name.into(),
        reason: e.to_string(),
    })
}

impl PGMQueueMeta {
    fn from_tokio_postgres_row(row: &tokio_postgres::Row) -> Result<Self, PgmqError> {
        Ok(Self {
            queue_name: col(row.try_get("queue_name"), "queue_name")?,
            is_partitioned: col(row.try_get("is_partitioned"), "is_partitioned")?,
            is_unlogged: col(row.try_get("is_unlogged"), "is_unlogged")?,
            created_at: col(row.try_get("created_at"), "created_at")?,
        })
    }
}

impl<T: for<'de> Deserialize<'de>> Message<T> {
    fn from_tokio_postgres_row(row: &tokio_postgres::Row) -> Result<Self, PgmqError> {
        let raw_msg: serde_json::Value = col(row.try_get("message"), "message")?;
        Ok(Self {
            msg_id: col(row.try_get("msg_id"), "msg_id")?,
            vt: col(row.try_get("vt"), "vt")?,
            enqueued_at: col(row.try_get("enqueued_at"), "enqueued_at")?,
            read_ct: col(row.try_get("read_ct"), "read_ct")?,
            message: serde_json::from_value(raw_msg)?,
        })
    }
}

impl SendBatchTopicRow {
    fn from_tokio_postgres_row(row: &tokio_postgres::Row) -> Result<Self, PgmqError> {
        Ok(Self {
            queue_name: col(row.try_get("queue_name"), "queue_name")?,
            msg_id: col(row.try_get("msg_id"), "msg_id")?,
        })
    }
}

impl ListTopicBindingsRow {
    fn from_tokio_postgres_row(row: &tokio_postgres::Row) -> Result<Self, PgmqError> {
        Ok(Self {
            pattern: col(row.try_get("pattern"), "pattern")?,
            queue_name: col(row.try_get("queue_name"), "queue_name")?,
            bound_at: col(row.try_get("bound_at"), "bound_at")?,
            compiled_regex: col(row.try_get("compiled_regex"), "compiled_regex")?,
        })
    }
}

impl ListNotifyInsertThrottlesRow {
    fn from_tokio_postgres_row(row: &tokio_postgres::Row) -> Result<Self, PgmqError> {
        Ok(Self {
            queue_name: col(row.try_get("queue_name"), "queue_name")?,
            throttle_interval_ms: col(
                row.try_get("throttle_interval_ms"),
                "throttle_interval_ms",
            )?,
            last_notified_at: col(row.try_get("last_notified_at"), "last_notified_at")?,
        })
    }
}

impl QueueMetrics {
    fn from_tokio_postgres_row(row: &tokio_postgres::Row) -> Result<Self, PgmqError> {
        Ok(Self {
            queue_name: col(row.try_get("queue_name"), "queue_name")?,
            queue_length: col(row.try_get("queue_length"), "queue_length")?,
            newest_msg_age_sec: col(row.try_get("newest_msg_age_sec"), "newest_msg_age_sec")?,
            oldest_msg_age_sec: col(row.try_get("oldest_msg_age_sec"), "oldest_msg_age_sec")?,
            total_messages: col(row.try_get("total_messages"), "total_messages")?,
            scrape_time: col(row.try_get("scrape_time"), "scrape_time")?,
            queue_visible_length: col(
                row.try_get("queue_visible_length"),
                "queue_visible_length",
            )?,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Shared implementation body that works against any tokio-postgres `GenericClient`.
//
// `&tokio_postgres::Client` and `&tokio_postgres::Transaction` each get their own impl by
// calling these free functions; everything else (param binding, row decoding) is identical.
// ---------------------------------------------------------------------------------------------

mod imp {
    use super::*;

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        c.execute(query::CREATE, &[&queue_name]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_unlogged<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        c.execute(query::CREATE_UNLOGGED, &[&queue_name]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_partitioned<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = format!("pgmq.q_{queue_name}");
        let row = c
            .query_one(query::CREATE_PARTITIONED_EXISTS_CHECK, &[&queue_table])
            .await?;
        let exists: bool = row.try_get("exists")?;
        if exists { return Ok(false); }
        c.execute(query::CREATE_PARTITIONED, &[&queue_name]).await?;
        Ok(true)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn convert_archive_partitioned<C: GenericClient + Sync>(
        c: &C,
        table_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
    ) -> Result<(), PgmqError> {
        let sql = query::convert_archive_partitioned_sql(
            partition_interval.is_some(),
            retention_interval.is_some(),
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&table_name];
        if let Some(p) = &partition_interval { params.push(p); }
        if let Some(r) = &retention_interval { params.push(r); }
        c.execute(sql.as_str(), &params).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn drop_queue<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        c.execute(query::DROP_QUEUE, &[&queue_name]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn purge_queue<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let row = c.query_one(query::PURGE_QUEUE, &[&queue_name]).await?;
        Ok(row.try_get("purge_queue")?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_queues<C: GenericClient + Sync>(c: &C) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let rows = c.query(query::LIST_QUEUES, &[]).await?;
        if rows.is_empty() { return Ok(None); }
        rows.iter()
            .map(PGMQueueMeta::from_tokio_postgres_row)
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn set_vt<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        msg_id: i64,
        vt: VisibilityTimeoutOffset,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let row = c.query_one(query::SET_VT, &[&queue_name, &msg_id, &vt_secs]).await?;
        Message::<T>::from_tokio_postgres_row(&row)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_delay_with_headers<C: GenericClient + Sync, T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        c: &C,
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
        let delay_secs = delay.as_seconds();
        let row = c.query_one(query::SEND, &[&queue_name, &message, &headers, &delay_secs]).await?;
        Ok(row.try_get("send")?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_batch_with_delay_with_headers<C: GenericClient + Sync, T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        c: &C,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: VisibilityTimeoutOffset,
    ) -> Result<Vec<i64>, PgmqError> {
        check_input(queue_name)?;
        let messages = serialize_list(messages)?;
        let headers = serialize_optional_list(headers)?;
        let delay_secs = delay.as_seconds();
        let rows = c.query(query::SEND_BATCH, &[&queue_name, &messages, &headers, &delay_secs]).await?;
        rows.iter().map(|r| r.try_get("send_batch").map_err(Into::into)).collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_batch<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let rows = c.query(query::READ, &[&queue_name, &vt_secs, &qty]).await?;
        rows.iter().map(Message::<T>::from_tokio_postgres_row).collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_batch_with_poll<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let pt = poll_timeout_to_secs(poll_timeout);
        let pi = poll_interval_to_ms(poll_interval);
        let rows = c
            .query(query::READ_WITH_POLL, &[&queue_name, &vt_secs, &max_batch_size, &pt, &pi])
            .await?;
        rows.iter().map(Message::<T>::from_tokio_postgres_row).collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let rows = c.query(query::READ_GROUPED, &[&queue_name, &vt_secs, &qty]).await?;
        rows.iter().map(Message::<T>::from_tokio_postgres_row).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_with_poll<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let pt = poll_timeout_to_secs(poll_timeout);
        let pi = poll_interval_to_ms(poll_interval);
        let rows = c.query(query::READ_GROUPED_WITH_POLL, &[&queue_name, &vt_secs, &qty, &pt, &pi]).await?;
        rows.iter().map(Message::<T>::from_tokio_postgres_row).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_head<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let rows = c.query(query::READ_GROUPED_HEAD, &[&queue_name, &vt_secs, &qty]).await?;
        rows.iter().map(Message::<T>::from_tokio_postgres_row).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_rr<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let rows = c.query(query::READ_GROUPED_RR, &[&queue_name, &vt_secs, &qty]).await?;
        rows.iter().map(Message::<T>::from_tokio_postgres_row).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn read_grouped_rr_with_poll<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
        vt: VisibilityTimeoutOffset,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = vt.as_seconds();
        let pt = poll_timeout_to_secs(poll_timeout);
        let pi = poll_interval_to_ms(poll_interval);
        let rows = c
            .query(query::READ_GROUPED_RR_WITH_POLL, &[&queue_name, &vt_secs, &qty, &pt, &pi])
            .await?;
        rows.iter().map(Message::<T>::from_tokio_postgres_row).collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn archive<C: GenericClient + Sync>(c: &C, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let row = c.query_one(query::ARCHIVE, &[&queue_name, &msg_id]).await?;
        Ok(row.try_get("archive")?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn archive_batch<C: GenericClient + Sync>(c: &C, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let rows = c.query(query::ARCHIVE_BATCH, &[&queue_name, &msg_ids]).await?;
        Ok(rows.len())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn pop<C: GenericClient + Sync, T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        c: &C,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row = c.query_opt(query::POP, &[&queue_name]).await?;
        row.as_ref().map(Message::<T>::from_tokio_postgres_row).transpose()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn delete<C: GenericClient + Sync>(c: &C, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        let row = c.query_one(query::DELETE, &[&queue_name, &msg_id]).await?;
        Ok(row.try_get("delete")?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn delete_batch<C: GenericClient + Sync>(c: &C, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
        let rows = c.query(query::DELETE_BATCH, &[&queue_name, &msg_ids]).await?;
        Ok(rows.len())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_fifo_index<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<(), PgmqError> {
        c.execute(query::CREATE_FIFO_INDEX, &[&queue_name]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn create_fifo_indexes_all<C: GenericClient + Sync>(c: &C) -> Result<(), PgmqError> {
        c.execute(query::CREATE_FIFO_INDEXES_ALL, &[]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn bind_topic<C: GenericClient + Sync>(c: &C, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        c.execute(query::BIND_TOPIC, &[&pattern, &queue_name]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn unbind_topic<C: GenericClient + Sync>(c: &C, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        c.execute(query::UNBIND_TOPIC, &[&pattern, &queue_name]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_topic_bindings<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let rows = c.query(query::LIST_TOPIC_BINDINGS, &[&queue_name]).await?;
        rows.iter().map(ListTopicBindingsRow::from_tokio_postgres_row).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_topic_bindings_all<C: GenericClient + Sync>(c: &C) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let rows = c.query(query::LIST_TOPIC_BINDINGS_ALL, &[]).await?;
        rows.iter().map(ListTopicBindingsRow::from_tokio_postgres_row).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_topic<C: GenericClient + Sync, T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        c: &C,
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
        let delay_secs = delay.as_seconds();
        let row = c.query_one(query::SEND_TOPIC, &[&routing_key, &message, &headers, &delay_secs]).await?;
        Ok(row.try_get("send_topic")?)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn send_batch_topic<C: GenericClient + Sync, T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        c: &C,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: VisibilityTimeoutOffset,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        let messages = serialize_list(messages)?;
        let headers = serialize_optional_list(headers)?;
        let delay_secs = delay.as_seconds();
        let rows = c.query(query::SEND_BATCH_TOPIC, &[&routing_key, &messages, &headers, &delay_secs]).await?;
        rows.iter().map(SendBatchTopicRow::from_tokio_postgres_row).collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn enable_notify_insert<C: GenericClient + Sync>(c: &C, queue_name: &str, throttle: std::time::Duration) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle.as_millis()).unwrap_or(i32::MAX);
        c.execute(query::ENABLE_NOTIFY_INSERT, &[&queue_name, &ms]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn disable_notify_insert<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        c.execute(query::DISABLE_NOTIFY_INSERT, &[&queue_name]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn update_notify_insert<C: GenericClient + Sync>(c: &C, queue_name: &str, throttle: std::time::Duration) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle.as_millis()).unwrap_or(i32::MAX);
        c.execute(query::UPDATE_NOTIFY_INSERT, &[&queue_name, &ms]).await?;
        Ok(())
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn list_notify_insert_throttles<C: GenericClient + Sync>(c: &C) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        let rows = c.query(query::LIST_NOTIFY_INSERT_THROTTLES, &[]).await?;
        rows.iter().map(ListNotifyInsertThrottlesRow::from_tokio_postgres_row).collect()
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn metrics<C: GenericClient + Sync>(c: &C, queue_name: &str) -> Result<QueueMetrics, PgmqError> {
        check_input(queue_name)?;
        let row = c.query_one(query::METRICS, &[&queue_name]).await?;
        QueueMetrics::from_tokio_postgres_row(&row)
    }
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(super) async fn metrics_all<C: GenericClient + Sync>(c: &C) -> Result<Vec<QueueMetrics>, PgmqError> {
        let rows = c.query(query::METRICS_ALL, &[]).await?;
        rows.iter().map(QueueMetrics::from_tokio_postgres_row).collect()
    }
}

// ---------------------------------------------------------------------------------------------
// Both impls (Pool, Transaction) delegate every method to the `imp::` shared body, varying only
// in how they obtain the `GenericClient` to pass in. The macro factors out the duplication.
// ---------------------------------------------------------------------------------------------

macro_rules! impl_pgmq_for_client_source {
    ($target:ty, |$self:ident| $client:expr) => {
        #[async_trait]
        impl PGMQueueExt for $target {
            async fn create($self, queue_name: &str) -> Result<(), PgmqError> { imp::create($client, queue_name).await }
            async fn create_unlogged($self, queue_name: &str) -> Result<(), PgmqError> { imp::create_unlogged($client, queue_name).await }
            async fn create_partitioned($self, queue_name: &str) -> Result<bool, PgmqError> { imp::create_partitioned($client, queue_name).await }
            async fn convert_archive_partitioned($self, t: &str, pi: Option<&str>, ri: Option<&str>) -> Result<(), PgmqError> { imp::convert_archive_partitioned($client, t, pi, ri).await }
            async fn drop_queue($self, queue_name: &str) -> Result<(), PgmqError> { imp::drop_queue($client, queue_name).await }
            async fn purge_queue($self, queue_name: &str) -> Result<i64, PgmqError> { imp::purge_queue($client, queue_name).await }
            async fn list_queues($self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> { imp::list_queues($client).await }
            async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, msg_id: i64, vt: VisibilityTimeoutOffset) -> Result<Message<T>, PgmqError> { imp::set_vt::<_, T>($client, queue_name, msg_id, vt).await }
            async fn send<T: Serialize + Send + Sync>($self, queue_name: &str, message: &T) -> Result<i64, PgmqError> { $self.send_delay(queue_name, message, VisibilityTimeoutOffset::seconds(0)).await }
            async fn send_delay<T: Serialize + Send + Sync>($self, queue_name: &str, message: &T, delay: VisibilityTimeoutOffset) -> Result<i64, PgmqError> { $self.send_delay_with_headers(queue_name, message, Option::<&()>::None, delay).await }
            async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, queue_name: &str, message: &T, headers: Option<&H>, delay: VisibilityTimeoutOffset) -> Result<i64, PgmqError> { imp::send_delay_with_headers($client, queue_name, message, headers, delay).await }
            async fn send_batch<T: Serialize + Send + Sync>($self, queue_name: &str, messages: &[T]) -> Result<Vec<i64>, PgmqError> { $self.send_batch_with_delay(queue_name, messages, VisibilityTimeoutOffset::seconds(0)).await }
            async fn send_batch_with_delay<T: Serialize + Send + Sync>($self, queue_name: &str, messages: &[T], delay: VisibilityTimeoutOffset) -> Result<Vec<i64>, PgmqError> { $self.send_batch_with_delay_with_headers(queue_name, messages, Option::<&[()]>::None, delay).await }
            async fn send_batch_with_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, queue_name: &str, messages: &[T], headers: Option<&[H]>, delay: VisibilityTimeoutOffset) -> Result<Vec<i64>, PgmqError> { imp::send_batch_with_delay_with_headers($client, queue_name, messages, headers, delay).await }
            async fn read<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset) -> Result<Option<Message<T>>, PgmqError> { Ok($self.read_batch::<T>(queue_name, vt, 1).await?.into_iter().next()) }
            async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_batch::<_, T>($client, queue_name, vt, qty).await }
            async fn read_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Option<Message<T>>, PgmqError> { Ok($self.read_batch_with_poll::<T>(queue_name, vt, 1, pt, pi).await?.and_then(|v| v.into_iter().next())) }
            async fn read_batch_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Option<Vec<Message<T>>>, PgmqError> { Ok(Some(imp::read_batch_with_poll::<_, T>($client, queue_name, vt, qty, pt, pi).await?)) }
            async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped::<_, T>($client, queue_name, vt, qty).await }
            async fn read_grouped_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_with_poll::<_, T>($client, queue_name, vt, qty, pt, pi).await }
            async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_head::<_, T>($client, queue_name, vt, qty).await }
            async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_rr::<_, T>($client, queue_name, vt, qty).await }
            async fn read_grouped_rr_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str, vt: VisibilityTimeoutOffset, qty: i32, pt: Option<std::time::Duration>, pi: Option<std::time::Duration>) -> Result<Vec<Message<T>>, PgmqError> { imp::read_grouped_rr_with_poll::<_, T>($client, queue_name, vt, qty, pt, pi).await }
            async fn archive($self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> { imp::archive($client, queue_name, msg_id).await }
            async fn archive_batch($self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> { imp::archive_batch($client, queue_name, msg_ids).await }
            async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>($self, queue_name: &str) -> Result<Option<Message<T>>, PgmqError> { imp::pop::<_, T>($client, queue_name).await }
            async fn delete($self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> { imp::delete($client, queue_name, msg_id).await }
            async fn delete_batch($self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> { imp::delete_batch($client, queue_name, msg_ids).await }
            async fn create_fifo_index($self, queue_name: &str) -> Result<(), PgmqError> { imp::create_fifo_index($client, queue_name).await }
            async fn create_fifo_indexes_all($self) -> Result<(), PgmqError> { imp::create_fifo_indexes_all($client).await }
            async fn bind_topic($self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> { imp::bind_topic($client, pattern, queue_name).await }
            async fn unbind_topic($self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> { imp::unbind_topic($client, pattern, queue_name).await }
            async fn list_topic_bindings($self, queue_name: &str) -> Result<Vec<ListTopicBindingsRow>, PgmqError> { imp::list_topic_bindings($client, queue_name).await }
            async fn list_topic_bindings_all($self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> { imp::list_topic_bindings_all($client).await }
            async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, rk: &str, m: &T, h: Option<&H>, d: VisibilityTimeoutOffset) -> Result<i32, PgmqError> { imp::send_topic($client, rk, m, h, d).await }
            async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>($self, rk: &str, m: &[T], h: Option<&[H]>, d: VisibilityTimeoutOffset) -> Result<Vec<SendBatchTopicRow>, PgmqError> { imp::send_batch_topic($client, rk, m, h, d).await }
            async fn enable_notify_insert($self, queue_name: &str, t: std::time::Duration) -> Result<(), PgmqError> { imp::enable_notify_insert($client, queue_name, t).await }
            async fn disable_notify_insert($self, queue_name: &str) -> Result<(), PgmqError> { imp::disable_notify_insert($client, queue_name).await }
            async fn update_notify_insert($self, queue_name: &str, t: std::time::Duration) -> Result<(), PgmqError> { imp::update_notify_insert($client, queue_name, t).await }
            async fn list_notify_insert_throttles($self) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> { imp::list_notify_insert_throttles($client).await }
            async fn metrics($self, queue_name: &str) -> Result<QueueMetrics, PgmqError> { imp::metrics($client, queue_name).await }
            async fn metrics_all($self) -> Result<Vec<QueueMetrics>, PgmqError> { imp::metrics_all($client).await }
        }
    };
}

// &tokio_postgres::Client: works with any pool implementation — user acquires their own
// connection however they want (deadpool, bb8, mobc, custom, or just one-shot).
impl_pgmq_for_client_source!(&tokio_postgres::Client, |self| self);

// Transaction: already a `GenericClient`, use directly.
impl_pgmq_for_client_source!(&tokio_postgres::Transaction<'_>, |self| self);
