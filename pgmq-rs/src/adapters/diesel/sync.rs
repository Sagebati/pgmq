//! # diesel (sync) adapter
//!
//! Implements [`QueueConn`](QueueConn) for
//! `&mut diesel::pg::PgConnection`. The full [`Queue`](crate::Queue) surface comes from the
//! central blanket `impl<C: QueueConn> Queue for C` in
//! [`super`].
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
//! ## Install
//!
//! ```ignore
//! use diesel::Connection;
//! let mut conn = diesel::pg::PgConnection::establish("postgres://...")?;
//! pgmq::install::diesel_sync::install_sql_from_embedded(&mut conn)?;
//! ```
//!
//! ## Normal use (with `block_on`)
//!
//! ```ignore
//! use pgmq::Queue;
//! // see crate-level docs for a no-runtime block_on; or use pollster::block_on
//! let mut conn = diesel::pg::PgConnection::establish(url)?;
//! block_on((&mut conn).create("orders"))?;
//! let id = block_on((&mut conn).send("orders", &my_order))?;
//! ```
//!
//! For most use cases where you're already in async land, prefer the
//! [diesel-async](super) adapter directly.

use super::{
    bind_diesel, unknown_scalar_col, ArchiveCol, DeleteCol, ExistsCol, MessageRowJson,
    PurgeQueueCol, SendBatchCol, SendCol, SendTopicCol,
};
use crate::adapters::queue_conn::{Params, QueueConn};
use crate::errors::PgmqError;
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::{sql_query, OptionalExtension, RunQueryDsl};
use serde::Deserialize;

#[async_trait]
impl QueueConn for &mut PgConnection {
    async fn execute(&mut self, sql: &str, params: Params<'_>) -> Result<u64, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        Ok(q.execute(&mut **self)? as u64)
    }

    async fn fetch_one_i64(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i64, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        match col {
            "send" => Ok(q.get_result::<SendCol>(&mut **self)?.send),
            "purge_queue" => Ok(q.get_result::<PurgeQueueCol>(&mut **self)?.purge_queue),
            _ => Err(unknown_scalar_col(col, "i64")),
        }
    }

    async fn fetch_one_i32(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i32, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        match col {
            "send_topic" => Ok(q.get_result::<SendTopicCol>(&mut **self)?.send_topic),
            _ => Err(unknown_scalar_col(col, "i32")),
        }
    }

    async fn fetch_one_bool(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<bool, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        match col {
            "exists" => Ok(q.get_result::<ExistsCol>(&mut **self)?.exists),
            "archive" => Ok(q.get_result::<ArchiveCol>(&mut **self)?.archive),
            "was_deleted" => Ok(q.get_result::<DeleteCol>(&mut **self)?.was_deleted),
            _ => Err(unknown_scalar_col(col, "bool")),
        }
    }

    async fn fetch_all_i64(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<Vec<i64>, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        match col {
            "send_batch" => {
                let rows: Vec<SendBatchCol> = q.load(&mut **self)?;
                Ok(rows.into_iter().map(|r| r.send_batch).collect())
            }
            _ => Err(unknown_scalar_col(col, "Vec<i64>")),
        }
    }

    async fn fetch_all_count(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<usize, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        let rows: Vec<ArchiveCol> = q.load(&mut **self)?;
        Ok(rows.len())
    }

    async fn fetch_one_message<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Message<T>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        let row: MessageRowJson = q.get_result(&mut **self)?;
        row.into_message()
    }

    async fn fetch_optional_message<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Option<Message<T>>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        // Sync diesel HAS .optional() — cheaper than load-and-pop.
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        let row: Option<MessageRowJson> = q.get_result(&mut **self).optional()?;
        row.map(MessageRowJson::into_message).transpose()
    }

    async fn fetch_all_messages<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<Message<T>>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        let rows: Vec<MessageRowJson> = q.load(&mut **self)?;
        rows.into_iter().map(MessageRowJson::into_message).collect()
    }

    async fn fetch_one_metrics(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<QueueMetrics, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        Ok(q.get_result(&mut **self)?)
    }

    async fn fetch_all_metrics(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<QueueMetrics>, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        Ok(q.load(&mut **self)?)
    }

    async fn fetch_all_queue_meta(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<PGMQueueMeta>, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        Ok(q.load(&mut **self)?)
    }

    async fn fetch_all_topic_bindings(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        Ok(q.load(&mut **self)?)
    }

    async fn fetch_all_notify_throttles(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        Ok(q.load(&mut **self)?)
    }

    async fn fetch_all_send_batch_topic(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
        Ok(q.load(&mut **self)?)
    }
}
