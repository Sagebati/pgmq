//! # sqlx adapter
//!
//! Implements [`QueueConn`](super::queue_conn::QueueConn) for sqlx connection types —
//! `&sqlx::PgPool`, `&mut sqlx::PgConnection`, and `&mut sqlx::Transaction<'_, Postgres>`.
//! The full [`Queue`](crate::Queue) surface comes from the single blanket
//! `impl<C: QueueConn> Queue for C` in [`super::queue_conn`], so no Queue-method bodies live
//! in this file — only the per-driver primitives.
//!
//! Three impls instead of a blanket because [`sqlx::Acquire::acquire`] consumes `self`, while
//! `QueueConn` takes `&mut self`. Each impl reborrows its receiver into the right form for
//! sqlx's [`Executor`] API and delegates to private helpers that take `&mut PgConnection`.
//!
//! Bonus: a sqlx-native [LISTEN/NOTIFY helper](listener) for `pgmq` insert channels, used by
//! the higher-level listener API in [`crate::pg_ext`].

use super::queue_conn::{Param, Params, QueueConn};
use crate::errors::PgmqError;
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use serde::Deserialize;
use sqlx::{Executor, Postgres, Row};

// ---------------------------------------------------------------------------------------------
// Bind helpers — turn a `Params` into a chain of sqlx `.bind(...)` calls. sqlx infers the
// Postgres type from each value's `Encode<Postgres>` impl (`&str` → `text`, `i64` → `bigint`,
// `serde_json::Value` → `jsonb`, etc.). Two variants because sqlx's typed-row decoder
// (`query_as`) returns a different Query type than the untyped one.
// ---------------------------------------------------------------------------------------------

fn bind_sqlx<'q>(
    mut q: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    params: Params<'q>,
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for p in params.0 {
        q = match p {
            Param::Text(s) => q.bind(s),
            Param::BigInt(n) => q.bind(n),
            Param::Integer(n) => q.bind(n),
            Param::Jsonb(v) => q.bind(v),
            Param::NullableJsonb(v) => q.bind(v),
            Param::BigIntArray(a) => q.bind(a),
            Param::JsonbArray(a) => q.bind(a),
            Param::NullableJsonbArray(a) => q.bind(a),
        };
    }
    q
}

fn bind_sqlx_as<'q, O>(
    mut q: sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments>,
    params: Params<'q>,
) -> sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments> {
    for p in params.0 {
        q = match p {
            Param::Text(s) => q.bind(s),
            Param::BigInt(n) => q.bind(n),
            Param::Integer(n) => q.bind(n),
            Param::Jsonb(v) => q.bind(v),
            Param::NullableJsonb(v) => q.bind(v),
            Param::BigIntArray(a) => q.bind(a),
            Param::JsonbArray(a) => q.bind(a),
            Param::NullableJsonbArray(a) => q.bind(a),
        };
    }
    q
}

// ---------------------------------------------------------------------------------------------
// Shared bodies — each primitive's actual logic, parameterized on any `Executor`. Both impls
// below acquire/reborrow into an `Executor` then call the helper. Keeps the per-receiver impl
// blocks down to one-line delegations.
// ---------------------------------------------------------------------------------------------

mod imp {
    use super::*;

    pub(super) async fn execute<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<u64, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let n = bind_sqlx(sqlx::query(sql), params)
            .execute(exec)
            .await?
            .rows_affected();
        Ok(n)
    }

    pub(super) async fn fetch_one_i64<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i64, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let row = bind_sqlx(sqlx::query(sql), params).fetch_one(exec).await?;
        Ok(row.try_get(col)?)
    }

    pub(super) async fn fetch_one_i32<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<i32, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let row = bind_sqlx(sqlx::query(sql), params).fetch_one(exec).await?;
        Ok(row.try_get(col)?)
    }

    pub(super) async fn fetch_one_bool<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<bool, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let row = bind_sqlx(sqlx::query(sql), params).fetch_one(exec).await?;
        Ok(row.try_get(col)?)
    }

    pub(super) async fn fetch_all_i64<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
        col: &str,
    ) -> Result<Vec<i64>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let rows = bind_sqlx(sqlx::query(sql), params).fetch_all(exec).await?;
        rows.iter()
            .map(|r| r.try_get::<i64, _>(col).map_err(Into::into))
            .collect()
    }

    pub(super) async fn fetch_all_count<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<usize, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let rows = bind_sqlx(sqlx::query(sql), params).fetch_all(exec).await?;
        Ok(rows.len())
    }

    pub(super) async fn fetch_one_message<'e, E, T>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Message<T>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        Ok(bind_sqlx_as(sqlx::query_as::<_, Message<T>>(sql), params)
            .fetch_one(exec)
            .await?)
    }

    pub(super) async fn fetch_optional_message<'e, E, T>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Option<Message<T>>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        Ok(bind_sqlx_as(sqlx::query_as::<_, Message<T>>(sql), params)
            .fetch_optional(exec)
            .await?)
    }

    pub(super) async fn fetch_all_messages<'e, E, T>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<Message<T>>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
        T: for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        Ok(bind_sqlx_as(sqlx::query_as::<_, Message<T>>(sql), params)
            .fetch_all(exec)
            .await?)
    }

    pub(super) async fn fetch_one_metrics<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<QueueMetrics, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        Ok(bind_sqlx_as(sqlx::query_as::<_, QueueMetrics>(sql), params)
            .fetch_one(exec)
            .await?)
    }

    pub(super) async fn fetch_all_metrics<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<QueueMetrics>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        Ok(bind_sqlx_as(sqlx::query_as::<_, QueueMetrics>(sql), params)
            .fetch_all(exec)
            .await?)
    }

    pub(super) async fn fetch_all_queue_meta<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<PGMQueueMeta>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        Ok(bind_sqlx_as(sqlx::query_as::<_, PGMQueueMeta>(sql), params)
            .fetch_all(exec)
            .await?)
    }

    pub(super) async fn fetch_all_topic_bindings<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        Ok(
            bind_sqlx_as(sqlx::query_as::<_, ListTopicBindingsRow>(sql), params)
                .fetch_all(exec)
                .await?,
        )
    }

    pub(super) async fn fetch_all_notify_throttles<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        Ok(bind_sqlx_as(
            sqlx::query_as::<_, ListNotifyInsertThrottlesRow>(sql),
            params,
        )
        .fetch_all(exec)
        .await?)
    }

    pub(super) async fn fetch_all_send_batch_topic<'e, E>(
        exec: E,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError>
    where
        E: Executor<'e, Database = Postgres>,
    {
        Ok(
            bind_sqlx_as(sqlx::query_as::<_, SendBatchTopicRow>(sql), params)
                .fetch_all(exec)
                .await?,
        )
    }
}

// ---------------------------------------------------------------------------------------------
// Per-receiver QueueConn impls. Each acquires/reborrows its receiver into an `Executor` and
// delegates to the matching `imp::*` helper. The reborrow expression is the only thing that
// changes between the three impls.
// ---------------------------------------------------------------------------------------------

// Per-receiver reborrow helpers — free fns with explicit lifetimes so the borrow checker can
// connect the input and output borrow lifetimes (closures with elided lifetimes can't).
// Each turns `&mut Self` into the `Executor` form sqlx's `query::execute` expects; auto-deref
// at the return site takes care of the actual reborrow.

fn reborrow_pool<'a>(s: &'a mut &sqlx::PgPool) -> &'a sqlx::PgPool {
    s
}

fn reborrow_conn<'a>(s: &'a mut &mut sqlx::PgConnection) -> &'a mut sqlx::PgConnection {
    s
}

fn reborrow_tx<'a>(
    s: &'a mut &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> &'a mut sqlx::PgConnection {
    // `Transaction` deref-coerces to `&mut PgConnection` via `DerefMut`; the explicit return
    // type drives the coercion.
    s
}

macro_rules! impl_queue_conn_for_sqlx {
    ($recv:ty, $exec:expr) => {
        #[async_trait]
        impl QueueConn for $recv {
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn execute(&mut self, sql: &str, params: Params<'_>) -> Result<u64, PgmqError> {
                imp::execute($exec(self), sql, params).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_one_i64(
                &mut self,
                sql: &str,
                params: Params<'_>,
                col: &str,
            ) -> Result<i64, PgmqError> {
                imp::fetch_one_i64($exec(self), sql, params, col).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_one_i32(
                &mut self,
                sql: &str,
                params: Params<'_>,
                col: &str,
            ) -> Result<i32, PgmqError> {
                imp::fetch_one_i32($exec(self), sql, params, col).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_one_bool(
                &mut self,
                sql: &str,
                params: Params<'_>,
                col: &str,
            ) -> Result<bool, PgmqError> {
                imp::fetch_one_bool($exec(self), sql, params, col).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_all_i64(
                &mut self,
                sql: &str,
                params: Params<'_>,
                col: &str,
            ) -> Result<Vec<i64>, PgmqError> {
                imp::fetch_all_i64($exec(self), sql, params, col).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_all_count(
                &mut self,
                sql: &str,
                params: Params<'_>,
            ) -> Result<usize, PgmqError> {
                imp::fetch_all_count($exec(self), sql, params).await
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
                imp::fetch_one_message($exec(self), sql, params).await
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
                imp::fetch_optional_message($exec(self), sql, params).await
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
                imp::fetch_all_messages($exec(self), sql, params).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_one_metrics(
                &mut self,
                sql: &str,
                params: Params<'_>,
            ) -> Result<QueueMetrics, PgmqError> {
                imp::fetch_one_metrics($exec(self), sql, params).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_all_metrics(
                &mut self,
                sql: &str,
                params: Params<'_>,
            ) -> Result<Vec<QueueMetrics>, PgmqError> {
                imp::fetch_all_metrics($exec(self), sql, params).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_all_queue_meta(
                &mut self,
                sql: &str,
                params: Params<'_>,
            ) -> Result<Vec<PGMQueueMeta>, PgmqError> {
                imp::fetch_all_queue_meta($exec(self), sql, params).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_all_topic_bindings(
                &mut self,
                sql: &str,
                params: Params<'_>,
            ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
                imp::fetch_all_topic_bindings($exec(self), sql, params).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_all_notify_throttles(
                &mut self,
                sql: &str,
                params: Params<'_>,
            ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
                imp::fetch_all_notify_throttles($exec(self), sql, params).await
            }
            #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
            async fn fetch_all_send_batch_topic(
                &mut self,
                sql: &str,
                params: Params<'_>,
            ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
                imp::fetch_all_send_batch_topic($exec(self), sql, params).await
            }
        }
    };
}

impl_queue_conn_for_sqlx!(&sqlx::PgPool, reborrow_pool);
impl_queue_conn_for_sqlx!(&mut sqlx::PgConnection, reborrow_conn);
impl_queue_conn_for_sqlx!(&mut sqlx::Transaction<'_, sqlx::Postgres>, reborrow_tx);

// ---------------------------------------------------------------------------------------------
// LISTEN/NOTIFY helpers (sqlx-specific surface, unchanged by the QueueConn refactor).
// ---------------------------------------------------------------------------------------------

/// Sqlx-native LISTEN/NOTIFY helpers for queue insert notifications.
///
/// Other adapters don't currently expose a listener (tokio-postgres's `AsyncMessage` stream
/// and diesel-sync's `pg_notifications()` are usable but require non-trivial wiring; we'll add
/// driver-specific listeners as separate sub-modules once their patterns settle). Use these
/// directly when you're on sqlx.
pub mod listener {
    use crate::errors::PgmqError;
    use crate::pg_ext::queue_name_to_insert_notification_channel_name;
    use async_trait::async_trait;
    use sqlx::PgPool;

    /// Sqlx-specific listener API. Convenience over `sqlx::postgres::PgListener` —
    /// builds a listener pre-subscribed to one or more pgmq insert-notification channels.
    #[async_trait]
    pub trait QueueListener {
        /// Build a [`sqlx::postgres::PgListener`] subscribed to the insert-notification
        /// channel for `queue_name`.
        async fn queue_insert_listener(
            self,
            queue_name: &str,
        ) -> Result<sqlx::postgres::PgListener, PgmqError>;

        /// Build a [`sqlx::postgres::PgListener`] subscribed to insert-notification channels
        /// for every name in `queue_names`.
        async fn queue_insert_listener_all<'a, I>(
            self,
            queue_names: I,
        ) -> Result<sqlx::postgres::PgListener, PgmqError>
        where
            I: IntoIterator<Item = &'a str> + Send,
            I::IntoIter: Send;
    }

    #[async_trait]
    impl QueueListener for &PgPool {
        async fn queue_insert_listener(
            self,
            queue_name: &str,
        ) -> Result<sqlx::postgres::PgListener, PgmqError> {
            queue_insert_listener(self, queue_name).await
        }

        async fn queue_insert_listener_all<'a, I>(
            self,
            queue_names: I,
        ) -> Result<sqlx::postgres::PgListener, PgmqError>
        where
            I: IntoIterator<Item = &'a str> + Send,
            I::IntoIter: Send,
        {
            queue_insert_listener_all(self, queue_names).await
        }
    }

    /// Free-function equivalent of [`QueueListener::queue_insert_listener`].
    pub async fn queue_insert_listener(
        pool: &PgPool,
        queue_name: &str,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        listener
            .listen(&queue_name_to_insert_notification_channel_name(queue_name))
            .await?;
        Ok(listener)
    }

    /// Free-function equivalent of [`QueueListener::queue_insert_listener_all`].
    pub async fn queue_insert_listener_all<'a>(
        pool: &PgPool,
        queue_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        let channel_names = queue_names
            .into_iter()
            .map(queue_name_to_insert_notification_channel_name)
            .collect::<Vec<_>>();
        listener
            .listen_all(channel_names.iter().map(|s| s.as_str()))
            .await?;
        Ok(listener)
    }
}
