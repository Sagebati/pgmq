//! # tokio-postgres adapter
//!
//! Implements [`QueueConn`](super::queue_conn::QueueConn) for `&tokio_postgres::Client` and
//! `&tokio_postgres::Transaction<'_>` via a single sealed-trait blanket. The full
//! [`Queue`](crate::Queue) surface comes from the central blanket
//! `impl<C: QueueConn> Queue for C` in [`super::queue_conn`].
//!
//! Pgmq has **no opinion on which pool you use**. The impl is on the connection (`Client`),
//! so [deadpool-postgres](https://docs.rs/deadpool-postgres/), [bb8](https://docs.rs/bb8/),
//! [mobc](https://docs.rs/mobc/), a custom pool, or a one-shot client all work — bring your
//! own pool, acquire a client, and call pgmq methods on it.
//!
//! Params are converted from [`Param`] variants into `&[&(dyn ToSql + Sync)]` per call via
//! [`as_tpg_params`]; rows decode via private `from_tokio_postgres_row` inherent methods on
//! the DTOs in [`crate::types`] (defined privately here so they don't leak into the public
//! API).
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
//! ## Install
//!
//! ```ignore
//! let mut client = pool.get().await?;
//! pgmq::install::tokio_postgres::install_sql_from_embedded(&mut client).await?;
//! ```
//!
//! ## Normal use (with a pool)
//!
//! ```ignore
//! use pgmq::Queue;
//!
//! let client = pool.get().await?;        // e.g. deadpool_postgres::Client — derefs to &Client
//! client.create("orders").await?;
//! let id = client.send("orders", &my_order).await?;
//! let msg: Option<pgmq::Message<MyOrder>> = client.read("orders", 30).await?;
//! client.archive("orders", id).await?;
//! ```
//!
//! ## With a user-managed transaction
//!
//! ```ignore
//! use pgmq::Queue;
//!
//! let mut client = pool.get().await?;
//! let tx = client.transaction().await?;
//! tx.execute("INSERT INTO orders (id, total) VALUES ($1, $2)", &[&order_id, &total]).await?;
//! tx.send("orders_queue", &order).await?;
//! tx.commit().await?;
//! ```

use super::queue_conn::{Param, Params, QueueConn};
use crate::errors::PgmqError;
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use serde::Deserialize;
use tokio_postgres::types::ToSql;
use tokio_postgres::GenericClient;

// ---------------------------------------------------------------------------------------------
// Private inherent row decoders on the DTOs. Inherent methods defined in this module are not
// visible outside it — keeps `from_tokio_postgres_row` out of the public API on the DTOs.
// tokio-postgres has no `FromRow` derive equivalent, so each decoder is hand-written.
// ---------------------------------------------------------------------------------------------

fn col<T>(res: Result<T, tokio_postgres::Error>, col_name: &str) -> Result<T, PgmqError> {
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
            throttle_interval_ms: col(row.try_get("throttle_interval_ms"), "throttle_interval_ms")?,
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
            queue_visible_length: col(row.try_get("queue_visible_length"), "queue_visible_length")?,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Param → &dyn ToSql + Sync conversion.
// ---------------------------------------------------------------------------------------------

/// Coerce a typed reference into a `&dyn ToSql + Sync`. Works for every `Param` inner type
/// because each one already implements `ToSql + Sync`.
fn as_tosql<T>(v: &T) -> &(dyn ToSql + Sync)
where
    T: ToSql + Sync,
{
    v
}

/// Build the `&[&(dyn ToSql + Sync)]` slice tokio-postgres needs from our `Params`.
fn as_tpg_params<'a>(params: &'a Params<'a>) -> Vec<&'a (dyn ToSql + Sync)> {
    params
        .0
        .iter()
        .map(|p| match p {
            Param::Text(s) => as_tosql(s),
            Param::BigInt(n) => as_tosql(n),
            Param::Integer(n) => as_tosql(n),
            Param::Jsonb(v) => as_tosql(v),
            Param::NullableJsonb(v) => as_tosql(v),
            Param::BigIntArray(a) => as_tosql(a),
            Param::JsonbArray(a) => as_tosql(a),
            Param::NullableJsonbArray(a) => as_tosql(a),
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Sealed marker restricts the blanket to `Client` and `Transaction<'_>` — the only two
// `GenericClient` implementers we want to expose.
// ---------------------------------------------------------------------------------------------

mod sealed {
    pub trait TpgConn: tokio_postgres::GenericClient + Sync {}
    impl TpgConn for tokio_postgres::Client {}
    impl TpgConn for tokio_postgres::Transaction<'_> {}
}

// ---------------------------------------------------------------------------------------------
// Single blanket QueueConn impl shared by `&Client` and `&Transaction<'_>`.
// ---------------------------------------------------------------------------------------------

#[async_trait]
impl<C: sealed::TpgConn> QueueConn for &C {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute(&mut self, sql: &str, params: Params<'_>) -> Result<u64, PgmqError> {
        let p = as_tpg_params(&params);
        Ok(GenericClient::execute(*self, sql, &p).await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_one_i64(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i64, PgmqError> {
        let p = as_tpg_params(&params);
        let row = GenericClient::query_one(*self, sql, &p).await?;
        Ok(row.try_get(col)?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_one_i32(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i32, PgmqError> {
        let p = as_tpg_params(&params);
        let row = GenericClient::query_one(*self, sql, &p).await?;
        Ok(row.try_get(col)?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_one_bool(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<bool, PgmqError> {
        let p = as_tpg_params(&params);
        let row = GenericClient::query_one(*self, sql, &p).await?;
        Ok(row.try_get(col)?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_i64(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<Vec<i64>, PgmqError> {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        rows.iter()
            .map(|r| r.try_get::<_, i64>(col).map_err(Into::into))
            .collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_count(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<usize, PgmqError> {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        Ok(rows.len())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_one_message<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Message<T>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        let p = as_tpg_params(&params);
        let row = GenericClient::query_one(*self, sql, &p).await?;
        Message::<T>::from_tokio_postgres_row(&row)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_optional_message<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Option<Message<T>>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        let p = as_tpg_params(&params);
        let row = GenericClient::query_opt(*self, sql, &p).await?;
        row.as_ref()
            .map(Message::<T>::from_tokio_postgres_row)
            .transpose()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_messages<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<Message<T>>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        rows.iter()
            .map(Message::<T>::from_tokio_postgres_row)
            .collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_one_metrics(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<QueueMetrics, PgmqError> {
        let p = as_tpg_params(&params);
        let row = GenericClient::query_one(*self, sql, &p).await?;
        QueueMetrics::from_tokio_postgres_row(&row)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_metrics(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<QueueMetrics>, PgmqError> {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        rows.iter()
            .map(QueueMetrics::from_tokio_postgres_row)
            .collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_queue_meta(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<PGMQueueMeta>, PgmqError> {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        rows.iter()
            .map(PGMQueueMeta::from_tokio_postgres_row)
            .collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_topic_bindings(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        rows.iter()
            .map(ListTopicBindingsRow::from_tokio_postgres_row)
            .collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_notify_throttles(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        rows.iter()
            .map(ListNotifyInsertThrottlesRow::from_tokio_postgres_row)
            .collect()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn fetch_all_send_batch_topic(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        let p = as_tpg_params(&params);
        let rows = GenericClient::query(*self, sql, &p).await?;
        rows.iter()
            .map(SendBatchTopicRow::from_tokio_postgres_row)
            .collect()
    }
}
