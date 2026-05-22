//! # sqlx adapter
//!
//! Implements [`crate::PGMQueueExt`] for:
//! - [`&mut sqlx::PgConnection`](sqlx::PgConnection) — a connection (typically acquired from a pool)
//! - [`&mut sqlx::Transaction<'_, Postgres>`](sqlx::Transaction) — a user-managed transaction
//!
//! Methods bind params directly to sqlx and decode into typed DTOs via `sqlx::query_as`
//! (using `#[derive(sqlx::FromRow)]` on the DTOs).
//!
//! ## Cargo features
//!
//! ```toml
//! [dependencies]
//! pgmq = "0.34"              # default features include `sqlx`
//! sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
//! ```
//!
//! For one-time install of the pgmq extension into your database, enable
//! `install-sql-embedded` (scripts shipped with the crate) or `install-sql-github` (fetched
//! from GitHub):
//!
//! ```toml
//! pgmq = { version = "0.34", features = ["install-sql-embedded"] }
//! ```
//!
//! ## Install
//!
//! Run once per database (idempotent — safe to call on every startup):
//!
//! ```ignore
//! let pool = sqlx::PgPool::connect("postgres://postgres:postgres@localhost:5432").await?;
//! pgmq::install::sqlx::install_sql_from_embedded(&pool).await?;
//! ```
//!
//! Or fetch a specific version of the extension from GitHub:
//!
//! ```ignore
//! pgmq::install::sqlx::install_sql_from_github(&pool, Some("1.10.0")).await?;
//! ```
//!
//! ## Normal use (with a pool)
//!
//! Acquire a connection from your sqlx pool and call methods on it. pgmq's API is implemented
//! on `&mut PgConnection`, *not* on `PgPool` — bring your own pool semantics.
//!
//! ```ignore
//! use pgmq::PGMQueueExt;
//! use pgmq::pg_ext::VisibilityTimeoutOffset;
//!
//! let pool = sqlx::PgPool::connect(url).await?;
//! let mut conn = pool.acquire().await?;
//!
//! // create / drop the queue
//! conn.create("orders").await?;
//!
//! // send
//! let id = conn.send("orders", &my_order).await?;
//!
//! // read
//! let msg: Option<pgmq::Message<MyOrder>> =
//!     conn.read("orders", VisibilityTimeoutOffset::seconds(30)).await?;
//!
//! // archive (keeps in history) or delete (purge)
//! conn.archive("orders", id).await?;
//! conn.delete("orders", id).await?;
//! ```
//!
//! Because methods take `self` by value where `Self = &mut PgConnection`, Rust's
//! auto-reborrow lets you reuse `conn` across many calls — each invocation consumes a fresh
//! `&mut *conn`, the original binding stays alive.
//!
//! ## With a user-managed transaction
//!
//! Use pgmq inside the same sqlx transaction as your own queries to make the enqueue atomic
//! with your business work. Either the whole transaction commits or none of it does.
//!
//! ```ignore
//! use pgmq::PGMQueueExt;
//!
//! let mut tx = pool.begin().await?;
//!
//! // Your own SQL in the tx:
//! sqlx::query("INSERT INTO orders (id, total) VALUES ($1, $2)")
//!     .bind(order_id).bind(total).execute(&mut *tx).await?;
//!
//! // pgmq.send in the same tx — call it on `&mut tx`:
//! tx.send("orders_queue", &order).await?;
//!
//! // Either both succeed:
//! tx.commit().await?;
//! // ...or both are rolled back if anything failed earlier.
//! ```
//!
//! `tx.send(...)` works because the trait is implemented on `&mut Transaction<'_, Postgres>`
//! — Rust auto-reborrows `&mut tx` for the call, and the original `tx` binding stays alive
//! for `commit`/`rollback` after.
//!
//! ## One-connection workflow (no pool)
//!
//! sqlx supports single connections without a pool. Same API.
//!
//! ```ignore
//! use sqlx::Connection;
//! let mut conn = sqlx::PgConnection::connect(url).await?;
//! conn.create("q").await?;
//! conn.send("q", &payload).await?;
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
use sqlx::{PgConnection, Postgres, Row, Transaction};

impl From<sqlx::Error> for PgmqError {
    fn from(err: sqlx::Error) -> Self {
        PgmqError::DatabaseError(err.to_string())
    }
}

// ---------------------------------------------------------------------------------------------
// The `impl PGMQueueExt for PgPool` and `impl PGMQueueExt for Tx<'_>` are mechanically the same
// except for what `e!()` expands to (the executor used by sqlx). We define the body once via
// a macro and instantiate it twice.
// ---------------------------------------------------------------------------------------------

macro_rules! impl_pgmq_for_sqlx {
    (
        $target:ty,
        |$self:ident| { $($body:tt)* }
    ) => {
        #[async_trait]
        impl PGMQueueExt for $target {
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn create($self, queue_name: &str) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                let _ = sqlx::query(query::CREATE).bind(queue_name);
                $($body)*  // gives us `e!()` macro local to this impl
                e!(sqlx::query(query::CREATE).bind(queue_name).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn create_unlogged($self, queue_name: &str) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                $($body)*
                e!(sqlx::query(query::CREATE_UNLOGGED).bind(queue_name).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn create_partitioned($self, queue_name: &str) -> Result<bool, PgmqError> {
                check_input(queue_name)?;
                let queue_table = format!("pgmq.q_{queue_name}");
                $($body)*
                let exists: bool = e!(sqlx::query_scalar::<_, bool>(query::CREATE_PARTITIONED_EXISTS_CHECK).bind(&queue_table).fetch_one);
                if exists { return Ok(false); }
                e!(sqlx::query(query::CREATE_PARTITIONED).bind(queue_name).execute);
                Ok(true)
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn convert_archive_partitioned(
                $self,
                table_name: &str,
                partition_interval: Option<&str>,
                retention_interval: Option<&str>,
            ) -> Result<(), PgmqError> {
                let sql = query::convert_archive_partitioned_sql(
                    partition_interval.is_some(),
                    retention_interval.is_some(),
                );
                $($body)*
                let mut q = sqlx::query(&sql).bind(table_name);
                if let Some(p) = partition_interval { q = q.bind(p); }
                if let Some(r) = retention_interval { q = q.bind(r); }
                e!(q.execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn drop_queue($self, queue_name: &str) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                $($body)*
                e!(sqlx::query(query::DROP_QUEUE).bind(queue_name).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn purge_queue($self, queue_name: &str) -> Result<i64, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                let row = e!(sqlx::query(query::PURGE_QUEUE).bind(queue_name).fetch_one);
                Ok(row.try_get("purge_queue")?)
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn list_queues($self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
                $($body)*
                let rows: Vec<PGMQueueMeta> = e!(sqlx::query_as::<_, PGMQueueMeta>(query::LIST_QUEUES).fetch_all);
                if rows.is_empty() { Ok(None) } else { Ok(Some(rows)) }
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn set_vt<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                msg_id: i64,
                vt: VisibilityTimeoutOffset,
            ) -> Result<Message<T>, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::SET_VT).bind(queue_name).bind(msg_id).bind(vt.as_seconds()).fetch_one))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send<T: Serialize + Send + Sync>(
                $self,
                queue_name: &str,
                message: &T,
            ) -> Result<i64, PgmqError> {
                $self.send_delay(queue_name, message, VisibilityTimeoutOffset::seconds(0)).await
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send_delay<T: Serialize + Send + Sync>(
                $self,
                queue_name: &str,
                message: &T,
                delay: VisibilityTimeoutOffset,
            ) -> Result<i64, PgmqError> {
                $self.send_delay_with_headers(queue_name, message, Option::<&()>::None, delay).await
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
                $self,
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
                $($body)*
                Ok(e!(sqlx::query_scalar::<_, i64>(query::SEND).bind(queue_name).bind(message).bind(headers).bind(delay.as_seconds()).fetch_one))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send_batch<T: Serialize + Send + Sync>(
                $self,
                queue_name: &str,
                messages: &[T],
            ) -> Result<Vec<i64>, PgmqError> {
                $self.send_batch_with_delay(queue_name, messages, VisibilityTimeoutOffset::seconds(0)).await
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send_batch_with_delay<T: Serialize + Send + Sync>(
                $self,
                queue_name: &str,
                messages: &[T],
                delay: VisibilityTimeoutOffset,
            ) -> Result<Vec<i64>, PgmqError> {
                $self.send_batch_with_delay_with_headers(queue_name, messages, Option::<&[()]>::None, delay).await
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send_batch_with_delay_with_headers<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
                $self,
                queue_name: &str,
                messages: &[T],
                headers: Option<&[H]>,
                delay: VisibilityTimeoutOffset,
            ) -> Result<Vec<i64>, PgmqError> {
                check_input(queue_name)?;
                let messages = serialize_list(messages)?;
                let headers = serialize_optional_list(headers)?;
                $($body)*
                Ok(e!(sqlx::query_scalar::<_, i64>(query::SEND_BATCH).bind(queue_name).bind(messages).bind(headers).bind(delay.as_seconds()).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
            ) -> Result<Option<Message<T>>, PgmqError> {
                Ok($self.read_batch::<T>(queue_name, vt, 1).await?.into_iter().next())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                qty: i32,
            ) -> Result<Vec<Message<T>>, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::READ).bind(queue_name).bind(vt.as_seconds()).bind(qty).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                poll_timeout: Option<std::time::Duration>,
                poll_interval: Option<std::time::Duration>,
            ) -> Result<Option<Message<T>>, PgmqError> {
                Ok($self
                    .read_batch_with_poll::<T>(queue_name, vt, 1, poll_timeout, poll_interval)
                    .await?
                    .and_then(|v| v.into_iter().next()))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_batch_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                max_batch_size: i32,
                poll_timeout: Option<std::time::Duration>,
                poll_interval: Option<std::time::Duration>,
            ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
                check_input(queue_name)?;
                let pt = poll_timeout_to_secs(poll_timeout);
                let pi = poll_interval_to_ms(poll_interval);
                $($body)*
                let rows: Vec<Message<T>> = e!(sqlx::query_as::<_, Message<T>>(query::READ_WITH_POLL).bind(queue_name).bind(vt.as_seconds()).bind(max_batch_size).bind(pt).bind(pi).fetch_all);
                Ok(Some(rows))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                qty: i32,
            ) -> Result<Vec<Message<T>>, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED).bind(queue_name).bind(vt.as_seconds()).bind(qty).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_grouped_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                qty: i32,
                poll_timeout: Option<std::time::Duration>,
                poll_interval: Option<std::time::Duration>,
            ) -> Result<Vec<Message<T>>, PgmqError> {
                check_input(queue_name)?;
                let pt = poll_timeout_to_secs(poll_timeout);
                let pi = poll_interval_to_ms(poll_interval);
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED_WITH_POLL).bind(queue_name).bind(vt.as_seconds()).bind(qty).bind(pt).bind(pi).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                qty: i32,
            ) -> Result<Vec<Message<T>>, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED_HEAD).bind(queue_name).bind(vt.as_seconds()).bind(qty).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                qty: i32,
            ) -> Result<Vec<Message<T>>, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED_RR).bind(queue_name).bind(vt.as_seconds()).bind(qty).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn read_grouped_rr_with_poll<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
                vt: VisibilityTimeoutOffset,
                qty: i32,
                poll_timeout: Option<std::time::Duration>,
                poll_interval: Option<std::time::Duration>,
            ) -> Result<Vec<Message<T>>, PgmqError> {
                check_input(queue_name)?;
                let pt = poll_timeout_to_secs(poll_timeout);
                let pi = poll_interval_to_ms(poll_interval);
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED_RR_WITH_POLL).bind(queue_name).bind(vt.as_seconds()).bind(qty).bind(pt).bind(pi).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn archive($self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                let row = e!(sqlx::query(query::ARCHIVE).bind(queue_name).bind(msg_id).fetch_one);
                Ok(row.try_get("archive")?)
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn archive_batch($self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                let rows = e!(sqlx::query(query::ARCHIVE_BATCH).bind(queue_name).bind(msg_ids).fetch_all);
                Ok(rows.len())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
                $self,
                queue_name: &str,
            ) -> Result<Option<Message<T>>, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, Message<T>>(query::POP).bind(queue_name).fetch_optional))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn delete($self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
                $($body)*
                let row = e!(sqlx::query(query::DELETE).bind(queue_name).bind(msg_id).fetch_one);
                Ok(row.try_get("delete")?)
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn delete_batch($self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
                $($body)*
                let rows = e!(sqlx::query(query::DELETE_BATCH).bind(queue_name).bind(msg_ids).fetch_all);
                Ok(rows.len())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn create_fifo_index($self, queue_name: &str) -> Result<(), PgmqError> {
                $($body)*
                e!(sqlx::query(query::CREATE_FIFO_INDEX).bind(queue_name).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn create_fifo_indexes_all($self) -> Result<(), PgmqError> {
                $($body)*
                e!(sqlx::query(query::CREATE_FIFO_INDEXES_ALL).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn bind_topic($self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                $($body)*
                e!(sqlx::query(query::BIND_TOPIC).bind(pattern).bind(queue_name).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn unbind_topic($self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                $($body)*
                e!(sqlx::query(query::UNBIND_TOPIC).bind(pattern).bind(queue_name).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn list_topic_bindings($self, queue_name: &str) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
                $($body)*
                Ok(e!(sqlx::query_as::<_, ListTopicBindingsRow>(query::LIST_TOPIC_BINDINGS).bind(queue_name).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn list_topic_bindings_all($self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
                $($body)*
                Ok(e!(sqlx::query_as::<_, ListTopicBindingsRow>(query::LIST_TOPIC_BINDINGS_ALL).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
                $self,
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
                $($body)*
                Ok(e!(sqlx::query_scalar::<_, i32>(query::SEND_TOPIC).bind(routing_key).bind(message).bind(headers).bind(delay.as_seconds()).fetch_one))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn send_batch_topic<T: Serialize + Send + Sync, H: Serialize + Send + Sync>(
                $self,
                routing_key: &str,
                messages: &[T],
                headers: Option<&[H]>,
                delay: VisibilityTimeoutOffset,
            ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
                let messages = serialize_list(messages)?;
                let headers = serialize_optional_list(headers)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, SendBatchTopicRow>(query::SEND_BATCH_TOPIC).bind(routing_key).bind(messages).bind(headers).bind(delay.as_seconds()).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn enable_notify_insert($self, queue_name: &str, throttle_interval: std::time::Duration) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                let ms = i32::try_from(throttle_interval.as_millis()).unwrap_or(i32::MAX);
                $($body)*
                e!(sqlx::query(query::ENABLE_NOTIFY_INSERT).bind(queue_name).bind(ms).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn disable_notify_insert($self, queue_name: &str) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                $($body)*
                e!(sqlx::query(query::DISABLE_NOTIFY_INSERT).bind(queue_name).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn update_notify_insert($self, queue_name: &str, throttle_interval: std::time::Duration) -> Result<(), PgmqError> {
                check_input(queue_name)?;
                let ms = i32::try_from(throttle_interval.as_millis()).unwrap_or(i32::MAX);
                $($body)*
                e!(sqlx::query(query::UPDATE_NOTIFY_INSERT).bind(queue_name).bind(ms).execute);
                Ok(())
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn list_notify_insert_throttles($self) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
                $($body)*
                Ok(e!(sqlx::query_as::<_, ListNotifyInsertThrottlesRow>(query::LIST_NOTIFY_INSERT_THROTTLES).fetch_all))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn metrics($self, queue_name: &str) -> Result<QueueMetrics, PgmqError> {
                check_input(queue_name)?;
                $($body)*
                Ok(e!(sqlx::query_as::<_, QueueMetrics>(query::METRICS).bind(queue_name).fetch_one))
            }

            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn metrics_all($self) -> Result<Vec<QueueMetrics>, PgmqError> {
                $($body)*
                Ok(e!(sqlx::query_as::<_, QueueMetrics>(query::METRICS_ALL).fetch_all))
            }
        }
    };
}

// &mut PgConnection: sqlx Executor is implemented on &mut PgConnection. Reborrow each call
// so methods that use `e!()` multiple times still compile.
impl_pgmq_for_sqlx!(&mut PgConnection, |self| {
    macro_rules! e { ($($t:tt)*) => { $($t)*(&mut *self).await? } }
});

// &mut Transaction<'_, Postgres>: sqlx Executor accepts it via DerefMut to PgConnection.
impl_pgmq_for_sqlx!(&mut Transaction<'_, Postgres>, |self| {
    macro_rules! e { ($($t:tt)*) => { $($t)*(&mut **self).await? } }
});
