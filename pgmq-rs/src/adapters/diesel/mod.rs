//! # diesel adapter
//!
//! Two flavours under one roof:
//!
//! - **async** (this module's top-level surface) — [`QueueConn`](super::QueueConn)
//!   for `&mut diesel_async::AsyncPgConnection`. Enabled by the `diesel-async` Cargo feature.
//! - **sync** ([`self::sync`]) — [`QueueConn`](super::QueueConn) for
//!   `&mut diesel::pg::PgConnection`. Enabled by the `diesel-sync` Cargo feature.
//!
//! The full [`Queue`](crate::Queue) surface comes from the central blanket
//! `impl<C: QueueConn> Queue for C` in [`super`]. No Queue-method bodies live
//! here — only the per-driver primitives.
//!
//! Both flavours share the `#[derive(QueryableByName)]` row-decoding DTOs defined privately
//! at the top of this module. Scalar fetches (`fetch_one_i64`, `fetch_one_bool`, …) dispatch
//! on the column name to pick the right typed struct, because diesel needs the row layout at
//! compile time.
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
//! Swap `diesel-async` for `diesel-sync` for the blocking variant. Add `install-sql-embedded`
//! for the one-time installer.
//!
//! ## Install (async)
//!
//! ```ignore
//! pgmq::install::diesel_async::install_sql_from_embedded(&pool).await?;
//! ```
//!
//! ## Normal use (async, with a pool)
//!
//! ```ignore
//! use pgmq::Queue;
//!
//! let mut conn = pool.get().await?;        // Object<Manager>, derefs to &mut AsyncPgConnection
//! conn.create("orders").await?;
//! let id = conn.send("orders", &my_order).await?;
//! let msg: Option<pgmq::Message<MyOrder>> = conn.read("orders", 30).await?;
//! conn.archive("orders", id).await?;
//! ```
//!
//! ## With a user-managed transaction (async)
//!
//! ```ignore
//! conn.transaction::<_, pgmq::PgmqError, _>(|conn| async move {
//!     diesel::sql_query("INSERT INTO orders (id, total) VALUES ($1, $2)")
//!         .bind::<diesel::sql_types::BigInt, _>(order_id)
//!         .bind::<diesel::sql_types::BigInt, _>(total)
//!         .execute(conn).await?;
//!     conn.send("orders_q", &order).await?;
//!     Ok(())
//! }.scope_boxed()).await?;
//! ```
//!
//! See [`self::sync`] for the blocking diesel `PgConnection` impl plus `block_on` /
//! `spawn_blocking` patterns.

use crate::errors::PgmqError;
use diesel::pg::Pg;
use diesel::query_builder::SqlQuery;
use diesel::sql_types;
use diesel::QueryableByName;

use super::queue_conn::{Param, Params};

// ---------------------------------------------------------------------------------------------
// Shared row-decoding DTOs. Both the async impl below and the `sync` submodule decode the
// same Postgres column shapes, so the structs live once here. They're private but reachable
// from descendant modules through Rust's lexical privacy.
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
    was_deleted: bool,
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
// Param → diesel bind chain. Both flavours use `BoxedSqlQuery<'_, Pg, SqlQuery>` so we don't
// have to thread different concrete types through every call site — `.into_boxed::<Pg>()`
// gives a uniform shape that accepts every `.bind::<sql_types::T, _>(value)` we need.
// ---------------------------------------------------------------------------------------------

type Boxed<'a> = diesel::query_builder::BoxedSqlQuery<'a, Pg, SqlQuery>;

fn bind_diesel<'q>(mut q: Boxed<'q>, params: Params<'q>) -> Boxed<'q> {
    for p in params.0 {
        q = match p {
            Param::Text(s) => q.bind::<sql_types::Text, _>(s),
            Param::BigInt(n) => q.bind::<sql_types::BigInt, _>(n),
            Param::Integer(n) => q.bind::<sql_types::Integer, _>(n),
            Param::Jsonb(v) => q.bind::<sql_types::Jsonb, _>(v),
            Param::NullableJsonb(v) => q.bind::<sql_types::Nullable<sql_types::Jsonb>, _>(v),
            Param::BigIntArray(a) => q.bind::<sql_types::Array<sql_types::BigInt>, _>(a),
            Param::JsonbArray(a) => q.bind::<sql_types::Array<sql_types::Jsonb>, _>(a),
            Param::NullableJsonbArray(a) => {
                q.bind::<sql_types::Nullable<sql_types::Array<sql_types::Jsonb>>, _>(a)
            }
        };
    }
    q
}

/// Diesel can't decode dynamic-by-name scalar columns; every scalar fetch passes through a
/// typed `QueryableByName` struct. This helper centralises the "unknown column" error path.
fn unknown_scalar_col(col: &str, ty: &str) -> PgmqError {
    PgmqError::RowDecodeError {
        column: col.into(),
        reason: format!(
            "diesel adapter has no QueryableByName decoder for {ty} column `{col}` — \
             add a wrapper struct in adapters/diesel/mod.rs and a match arm in the dispatch"
        ),
    }
}

#[cfg(feature = "diesel-sync")]
pub mod sync;

// ---------------------------------------------------------------------------------------------
// Async impl (diesel-async). Gated on the `diesel-async` Cargo feature.
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "diesel-async")]
mod async_impl {
    use super::*;
    use super::super::queue_conn::QueueConn;
    use crate::types::{
        ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
        SendBatchTopicRow,
    };
    use async_trait::async_trait;
    use diesel::sql_query;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};
    use serde::Deserialize;

    #[async_trait]
    impl QueueConn for &mut AsyncPgConnection {
        async fn execute(
            &mut self,
            sql: &str,
            params: Params<'_>,
        ) -> Result<u64, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            let n = q.execute(&mut **self).await?;
            Ok(n as u64)
        }

        async fn fetch_one_i64(
            &mut self,
            sql: &str,
            params: Params<'_>,
            col: &str,
        ) -> Result<i64, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            match col {
                "send" => Ok(q.get_result::<SendCol>(&mut **self).await?.send),
                "purge_queue" => Ok(q
                    .get_result::<PurgeQueueCol>(&mut **self)
                    .await?
                    .purge_queue),
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
                "send_topic" => Ok(q.get_result::<SendTopicCol>(&mut **self).await?.send_topic),
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
                "exists" => Ok(q.get_result::<ExistsCol>(&mut **self).await?.exists),
                "archive" => Ok(q.get_result::<ArchiveCol>(&mut **self).await?.archive),
                "was_deleted" => Ok(q.get_result::<DeleteCol>(&mut **self).await?.was_deleted),
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
                    let rows: Vec<SendBatchCol> = q.load(&mut **self).await?;
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
            // Used by archive_batch / delete_batch: both SQLs return one row per processed
            // msg, so we count via ArchiveCol — DeleteCol's row shape is also a single bool
            // column and is interchangeable here since we discard the value.
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            let rows: Vec<ArchiveCol> = q.load(&mut **self).await?;
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
            let row: MessageRowJson = q.get_result(&mut **self).await?;
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
            // diesel-async's RunQueryDsl has no `.optional()`; load() and pop. pgmq's pop SQL
            // already includes LIMIT 1 server-side.
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            let mut rows: Vec<MessageRowJson> = q.load(&mut **self).await?;
            rows.pop().map(MessageRowJson::into_message).transpose()
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
            let rows: Vec<MessageRowJson> = q.load(&mut **self).await?;
            rows.into_iter().map(MessageRowJson::into_message).collect()
        }

        async fn fetch_one_metrics(
            &mut self,
            sql: &str,
            params: Params<'_>,
        ) -> Result<QueueMetrics, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            Ok(q.get_result(&mut **self).await?)
        }

        async fn fetch_all_metrics(
            &mut self,
            sql: &str,
            params: Params<'_>,
        ) -> Result<Vec<QueueMetrics>, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            Ok(q.load(&mut **self).await?)
        }

        async fn fetch_all_queue_meta(
            &mut self,
            sql: &str,
            params: Params<'_>,
        ) -> Result<Vec<PGMQueueMeta>, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            Ok(q.load(&mut **self).await?)
        }

        async fn fetch_all_topic_bindings(
            &mut self,
            sql: &str,
            params: Params<'_>,
        ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            Ok(q.load(&mut **self).await?)
        }

        async fn fetch_all_notify_throttles(
            &mut self,
            sql: &str,
            params: Params<'_>,
        ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            Ok(q.load(&mut **self).await?)
        }

        async fn fetch_all_send_batch_topic(
            &mut self,
            sql: &str,
            params: Params<'_>,
        ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
            let q = bind_diesel(sql_query(sql.to_string()).into_boxed::<Pg>(), params);
            Ok(q.load(&mut **self).await?)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Message<T> wire decoder. Diesel needs a `QueryableByName` struct for every column shape; for
// `pgmq.read*` / `pop` / `set_vt` we decode the JSON `message` column as `serde_json::Value`
// then parse into `T` on the Rust side. Lives here so both async and sync share it.
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
    fn into_message<T: for<'de> serde::Deserialize<'de>>(
        self,
    ) -> Result<crate::types::Message<T>, PgmqError> {
        Ok(crate::types::Message {
            msg_id: self.msg_id,
            read_ct: self.read_ct,
            enqueued_at: self.enqueued_at,
            vt: self.vt,
            message: serde_json::from_value(self.message)?,
        })
    }
}
