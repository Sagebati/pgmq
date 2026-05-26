//! # `QueueConn` — the one-trait-to-rule-them-all driver abstraction
//!
//! `pgmq`'s public [`Queue`] trait is implemented exactly **once** in this crate, via the
//! blanket `impl<C: QueueConn> Queue for C` at the bottom of this file. Every per-driver
//! file ([`crate::adapters::sqlx`], [`crate::adapters::tokio_postgres`],
//! [`crate::adapters::diesel`], [`crate::adapters::diesel::sync`]) contains only the
//! `QueueConn` impl(s) for its connection types — no Queue-method bodies live there.
//!
//! ## The split
//!
//! - **`QueueConn`** (this file): per-driver primitives. Each adapter implements ~15 typed
//!   fetch methods (`execute`, `fetch_one_i64`, `fetch_one_message`, `fetch_one_metrics`, …)
//!   using its native typed decoder (sqlx's `query_as`, tokio-postgres' `try_get`, diesel's
//!   `QueryableByName`). No JSON wrapping, no dynamic row trait.
//! - **`Queue` blanket impl** (this file): all ~30 Queue-method bodies — input validation,
//!   SQL string selection, parameter assembly, result transformation. Written once, used by
//!   every driver.
//!
//! ## How parameters cross the boundary
//!
//! `Queue` bodies build a [`Params`] vector of [`Param`] variants. Each `QueueConn` impl
//! knows how to encode each variant into its native bind API (sqlx's `.bind(v)`,
//! tokio-postgres' `&[&(dyn ToSql + Sync)]`, diesel's `.bind::<sql_types::T, _>(v)`).
//!
//! ## Why not blanket on a lower abstraction (rows)?
//!
//! See the design discussion at `adapters/mod.rs` — diesel's row decoding is compile-time
//! (`QueryableByName`), so a dynamic-row trait would require JSON-wrapping every diesel
//! query. Routing through typed fetch methods instead lets each adapter use its native fast
//! path.

use crate::errors::PgmqError;
use crate::pg_ext::{Queue, VisibilityTimeoutOffset};
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::helpers::{
    check_input, poll_interval_ms, poll_timeout_secs, serialize_list, serialize_optional_list,
};
use super::query;

// ---------------------------------------------------------------------------------------------
// Parameter encoding
// ---------------------------------------------------------------------------------------------

/// A single bind parameter for a `QueueConn` call. The enum closes over every Postgres type
/// `pgmq` queries actually bind. Adding a new variant means touching every adapter's
/// `QueueConn` impl — that's the price of one shared bind protocol.
///
/// Borrowed variants (`Text`, `BigIntArray`) avoid allocation for caller-owned data; the
/// other variants are owned because their values are typically built fresh per call
/// (`serde_json::to_value`, `serialize_list`, …).
#[derive(Debug)]
pub enum Param<'a> {
    Text(&'a str),
    BigInt(i64),
    Integer(i32),
    Jsonb(serde_json::Value),
    NullableJsonb(Option<serde_json::Value>),
    BigIntArray(&'a [i64]),
    JsonbArray(Vec<serde_json::Value>),
    NullableJsonbArray(Option<Vec<serde_json::Value>>),
}

/// Ordered, owned-by-the-call parameter list. Passed by value into `QueueConn` methods so
/// adapters can move owned values into their native bind sinks without extra clones.
#[derive(Debug, Default)]
pub struct Params<'a>(pub Vec<Param<'a>>);

impl Params<'_> {
    pub fn new() -> Self {
        Self(Vec::new())
    }
}

/// `params![Param::Text(x), Param::BigInt(n), ...]` — terse construction at call sites.
macro_rules! params {
    () => { Params::new() };
    ($($x:expr),+ $(,)?) => { Params(vec![$($x),+]) };
}

// ---------------------------------------------------------------------------------------------
// QueueConn — what each driver implements
// ---------------------------------------------------------------------------------------------

/// Driver-side primitives. Each adapter implements this trait for its connection types
/// (e.g. `&sqlx::PgPool`, `&mut diesel_async::AsyncPgConnection`, `&tokio_postgres::Client`).
/// The single blanket `impl<C: QueueConn + Send> Queue for C` then gives every implementer a
/// full [`Queue`] surface.
///
/// All methods take `&mut self` so the blanket Queue bodies can chain multiple calls per
/// invocation (`create_partitioned` runs two queries, the poll-variants run one). For
/// pool-style receivers like `&sqlx::PgPool` this is a no-op `&mut &PgPool` reborrow; the
/// pool internally acquires a connection per call.
#[async_trait]
pub trait QueueConn: Send {
    // ------ no-row execute ----------------------------------------------------------------

    async fn execute(&mut self, sql: &str, params: Params<'_>) -> Result<u64, PgmqError>;

    // ------ scalar fetches (single row, single column) ------------------------------------

    async fn fetch_one_i64(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i64, PgmqError>;

    async fn fetch_one_i32(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i32, PgmqError>;

    async fn fetch_one_bool(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<bool, PgmqError>;

    // ------ column extraction (many rows, single column) ----------------------------------

    async fn fetch_all_i64(
        &mut self,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<Vec<i64>, PgmqError>;

    /// Just the row count — used by `archive_batch` / `delete_batch` where the returned rows
    /// are bools the caller doesn't care about, just how many came back.
    async fn fetch_all_count(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<usize, PgmqError>;

    // ------ Message<T> generic fetches ----------------------------------------------------

    async fn fetch_one_message<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Message<T>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static;

    async fn fetch_optional_message<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Option<Message<T>>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static;

    async fn fetch_all_messages<T>(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<Message<T>>, PgmqError>
    where
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static;

    // ------ per-DTO fetches (concrete types from crate::types) ----------------------------

    async fn fetch_one_metrics(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<QueueMetrics, PgmqError>;

    async fn fetch_all_metrics(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<QueueMetrics>, PgmqError>;

    async fn fetch_all_queue_meta(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<PGMQueueMeta>, PgmqError>;

    async fn fetch_all_topic_bindings(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError>;

    async fn fetch_all_notify_throttles(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError>;

    async fn fetch_all_send_batch_topic(
        &mut self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError>;
}

// ---------------------------------------------------------------------------------------------
// The single Queue impl — all SQL bodies, written once
// ---------------------------------------------------------------------------------------------

#[async_trait]
impl<C: QueueConn + Send> Queue for C {
    async fn create(mut self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.execute(query::CREATE, params![Param::Text(queue_name)])
            .await?;
        Ok(())
    }

    async fn create_unlogged(mut self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.execute(query::CREATE_UNLOGGED, params![Param::Text(queue_name)])
            .await?;
        Ok(())
    }

    async fn create_partitioned(mut self, queue_name: &str) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = format!("pgmq.q_{queue_name}");
        let exists = self
            .fetch_one_bool(
                query::CREATE_PARTITIONED_EXISTS_CHECK,
                params![Param::Text(&queue_table)],
                "exists",
            )
            .await?;
        if exists {
            return Ok(false);
        }
        self.execute(query::CREATE_PARTITIONED, params![Param::Text(queue_name)])
            .await?;
        Ok(true)
    }

    async fn convert_archive_partitioned(
        mut self,
        table_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
    ) -> Result<(), PgmqError> {
        let sql = query::convert_archive_partitioned_sql(
            partition_interval.is_some(),
            retention_interval.is_some(),
        );
        let mut p = Params::new();
        p.0.push(Param::Text(table_name));
        if let Some(s) = partition_interval {
            p.0.push(Param::Text(s));
        }
        if let Some(s) = retention_interval {
            p.0.push(Param::Text(s));
        }
        self.execute(&sql, p).await?;
        Ok(())
    }

    async fn drop_queue(mut self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.execute(query::DROP_QUEUE, params![Param::Text(queue_name)])
            .await?;
        Ok(())
    }

    async fn purge_queue(mut self, queue_name: &str) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        self.fetch_one_i64(
            query::PURGE_QUEUE,
            params![Param::Text(queue_name)],
            "purge_queue",
        )
        .await
    }

    async fn list_queues(mut self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let rows = self.fetch_all_queue_meta(query::LIST_QUEUES, params![]).await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }

    async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        msg_id: i64,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        self.fetch_one_message::<T>(
            query::SET_VT,
            params![
                Param::Text(queue_name),
                Param::BigInt(msg_id),
                Param::Integer(vt_secs),
            ],
        )
        .await
    }

    async fn send<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, PgmqError> {
        self.send_delay(queue_name, message, VisibilityTimeoutOffset::seconds(0))
            .await
    }

    async fn send_delay<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        message: &T,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError> {
        self.send_delay_with_headers(queue_name, message, Option::<&()>::None, delay)
            .await
    }

    async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        mut self,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let message_v = serde_json::to_value(message)?;
        let headers_v = match headers {
            Some(h) => Some(serde_json::to_value(h)?),
            None => None,
        };
        let delay_secs = delay.into().as_seconds();
        self.fetch_one_i64(
            query::SEND,
            params![
                Param::Text(queue_name),
                Param::Jsonb(message_v),
                Param::NullableJsonb(headers_v),
                Param::Integer(delay_secs),
            ],
            "send",
        )
        .await
    }

    async fn send_batch<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        messages: &[T],
    ) -> Result<Vec<i64>, PgmqError> {
        self.send_batch_with_delay(queue_name, messages, VisibilityTimeoutOffset::seconds(0))
            .await
    }

    async fn send_batch_with_delay<T: Serialize + Send + Sync>(
        self,
        queue_name: &str,
        messages: &[T],
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError> {
        self.send_batch_with_delay_with_headers(queue_name, messages, Option::<&[()]>::None, delay)
            .await
    }

    async fn send_batch_with_delay_with_headers<
        T: Serialize + Send + Sync,
        H: Serialize + Send + Sync,
    >(
        mut self,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<i64>, PgmqError> {
        check_input(queue_name)?;
        let messages_v = serialize_list(messages)?;
        let headers_v = serialize_optional_list(headers)?;
        let delay_secs = delay.into().as_seconds();
        self.fetch_all_i64(
            query::SEND_BATCH,
            params![
                Param::Text(queue_name),
                Param::JsonbArray(messages_v),
                Param::NullableJsonbArray(headers_v),
                Param::Integer(delay_secs),
            ],
            "send_batch",
        )
        .await
    }

    async fn read<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Option<Message<T>>, PgmqError> {
        Ok(self
            .read_batch::<T>(queue_name, visibility_timeout, 1)
            .await?
            .into_iter()
            .next())
    }

    async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        self.fetch_all_messages::<T>(
            query::READ,
            params![
                Param::Text(queue_name),
                Param::Integer(vt_secs),
                Param::Integer(qty),
            ],
        )
        .await
    }

    async fn read_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Message<T>>, PgmqError> {
        Ok(self
            .read_batch_with_poll::<T>(
                queue_name,
                visibility_timeout,
                1,
                poll_timeout,
                poll_interval,
            )
            .await?
            .and_then(|v| v.into_iter().next()))
    }

    async fn read_batch_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        let sql = query::read_with_poll_sql(poll_timeout.is_some(), poll_interval.is_some());
        let mut p = params![
            Param::Text(queue_name),
            Param::Integer(vt_secs),
            Param::Integer(qty),
        ];
        if let Some(t) = poll_timeout {
            p.0.push(Param::Integer(poll_timeout_secs(t)));
        }
        if let Some(i) = poll_interval {
            p.0.push(Param::Integer(poll_interval_ms(i)));
        }
        Ok(Some(self.fetch_all_messages::<T>(&sql, p).await?))
    }

    async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        self.fetch_all_messages::<T>(
            query::READ_GROUPED,
            params![
                Param::Text(queue_name),
                Param::Integer(vt_secs),
                Param::Integer(qty),
            ],
        )
        .await
    }

    async fn read_grouped_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        let sql =
            query::read_grouped_with_poll_sql(poll_timeout.is_some(), poll_interval.is_some());
        let mut p = params![
            Param::Text(queue_name),
            Param::Integer(vt_secs),
            Param::Integer(qty),
        ];
        if let Some(t) = poll_timeout {
            p.0.push(Param::Integer(poll_timeout_secs(t)));
        }
        if let Some(i) = poll_interval {
            p.0.push(Param::Integer(poll_interval_ms(i)));
        }
        self.fetch_all_messages::<T>(&sql, p).await
    }

    async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        self.fetch_all_messages::<T>(
            query::READ_GROUPED_HEAD,
            params![
                Param::Text(queue_name),
                Param::Integer(vt_secs),
                Param::Integer(qty),
            ],
        )
        .await
    }

    async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        self.fetch_all_messages::<T>(
            query::READ_GROUPED_RR,
            params![
                Param::Text(queue_name),
                Param::Integer(vt_secs),
                Param::Integer(qty),
            ],
        )
        .await
    }

    async fn read_grouped_rr_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt_secs = visibility_timeout.into().as_seconds();
        let sql =
            query::read_grouped_rr_with_poll_sql(poll_timeout.is_some(), poll_interval.is_some());
        let mut p = params![
            Param::Text(queue_name),
            Param::Integer(vt_secs),
            Param::Integer(qty),
        ];
        if let Some(t) = poll_timeout {
            p.0.push(Param::Integer(poll_timeout_secs(t)));
        }
        if let Some(i) = poll_interval {
            p.0.push(Param::Integer(poll_interval_ms(i)));
        }
        self.fetch_all_messages::<T>(&sql, p).await
    }

    async fn archive(mut self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        self.fetch_one_bool(
            query::ARCHIVE,
            params![Param::Text(queue_name), Param::BigInt(msg_id)],
            "archive",
        )
        .await
    }

    async fn archive_batch(
        mut self,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        self.fetch_all_count(
            query::ARCHIVE_BATCH,
            params![Param::Text(queue_name), Param::BigIntArray(msg_ids)],
        )
        .await
    }

    async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        mut self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        self.fetch_optional_message::<T>(query::POP, params![Param::Text(queue_name)])
            .await
    }

    async fn delete(mut self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        self.fetch_one_bool(
            query::DELETE,
            params![Param::Text(queue_name), Param::BigInt(msg_id)],
            "was_deleted",
        )
        .await
    }

    async fn delete_batch(mut self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        self.fetch_all_count(
            query::DELETE_BATCH,
            params![Param::Text(queue_name), Param::BigIntArray(msg_ids)],
        )
        .await
    }

    async fn create_fifo_index(mut self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.execute(query::CREATE_FIFO_INDEX, params![Param::Text(queue_name)])
            .await?;
        Ok(())
    }

    async fn create_fifo_indexes_all(mut self) -> Result<(), PgmqError> {
        self.execute(query::CREATE_FIFO_INDEXES_ALL, params![]).await?;
        Ok(())
    }

    async fn bind_topic(mut self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.execute(
            query::BIND_TOPIC,
            params![Param::Text(pattern), Param::Text(queue_name)],
        )
        .await?;
        Ok(())
    }

    async fn unbind_topic(mut self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.execute(
            query::UNBIND_TOPIC,
            params![Param::Text(pattern), Param::Text(queue_name)],
        )
        .await?;
        Ok(())
    }

    async fn list_topic_bindings(
        mut self,
        queue_name: &str,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        self.fetch_all_topic_bindings(
            query::LIST_TOPIC_BINDINGS,
            params![Param::Text(queue_name)],
        )
        .await
    }

    async fn list_topic_bindings_all(mut self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        self.fetch_all_topic_bindings(query::LIST_TOPIC_BINDINGS_ALL, params![])
            .await
    }

    async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        mut self,
        routing_key: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<i32, PgmqError> {
        let message_v = serde_json::to_value(message)?;
        let headers_v = match headers {
            Some(h) => Some(serde_json::to_value(h)?),
            None => None,
        };
        let delay_secs = delay.into().as_seconds();
        self.fetch_one_i32(
            query::SEND_TOPIC,
            params![
                Param::Text(routing_key),
                Param::Jsonb(message_v),
                Param::NullableJsonb(headers_v),
                Param::Integer(delay_secs),
            ],
            "send_topic",
        )
        .await
    }

    async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
        mut self,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset> + Send,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        let messages_v = serialize_list(messages)?;
        let headers_v = serialize_optional_list(headers)?;
        let delay_secs = delay.into().as_seconds();
        self.fetch_all_send_batch_topic(
            query::SEND_BATCH_TOPIC,
            params![
                Param::Text(routing_key),
                Param::JsonbArray(messages_v),
                Param::NullableJsonbArray(headers_v),
                Param::Integer(delay_secs),
            ],
        )
        .await
    }

    async fn enable_notify_insert(
        mut self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle_interval.as_millis()).unwrap_or(i32::MAX);
        self.execute(
            query::ENABLE_NOTIFY_INSERT,
            params![Param::Text(queue_name), Param::Integer(ms)],
        )
        .await?;
        Ok(())
    }

    async fn disable_notify_insert(mut self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        self.execute(
            query::DISABLE_NOTIFY_INSERT,
            params![Param::Text(queue_name)],
        )
        .await?;
        Ok(())
    }

    async fn update_notify_insert(
        mut self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = i32::try_from(throttle_interval.as_millis()).unwrap_or(i32::MAX);
        self.execute(
            query::UPDATE_NOTIFY_INSERT,
            params![Param::Text(queue_name), Param::Integer(ms)],
        )
        .await?;
        Ok(())
    }

    async fn list_notify_insert_throttles(
        mut self,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        self.fetch_all_notify_throttles(query::LIST_NOTIFY_INSERT_THROTTLES, params![])
            .await
    }

    async fn metrics(mut self, queue_name: &str) -> Result<QueueMetrics, PgmqError> {
        check_input(queue_name)?;
        self.fetch_one_metrics(query::METRICS, params![Param::Text(queue_name)])
            .await
    }

    async fn metrics_all(mut self) -> Result<Vec<QueueMetrics>, PgmqError> {
        self.fetch_all_metrics(query::METRICS_ALL, params![]).await
    }
}
