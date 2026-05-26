//! # sqlx adapter
//!
//! Single generic [`Queue`][crate::Queue] impl covering everything that satisfies
//! [`sqlx::Acquire<'_, Database = sqlx::Postgres>`](sqlx::Acquire) — so the trait works on
//! `&sqlx::PgPool`, `&mut sqlx::PgConnection`, and `&mut sqlx::Transaction<'_, Postgres>`
//! without per-type duplication.
//!
//! Each method body acquires a connection via `self.acquire()` and runs a single query (or two
//! for the few methods that need them). For `&PgPool` this acquires from the pool (same cost
//! as sqlx's own pool-level `execute`); for `&mut PgConnection` / `&mut Transaction` the
//! acquire is a no-op.
//!
//! See the crate-root and per-method documentation for usage patterns.

use super::helpers::{
    check_input, duration_as_ms_i32, poll_timeout_secs, queue_table_name, serialize_list,
    serialize_optional_list,
};
use super::query;
use crate::errors::PgmqError;
use crate::pg_ext::{Queue, VisibilityTimeoutOffset};
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, PGMQueueMeta, QueueMetrics,
    SendBatchTopicRow,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Postgres};

/// Sealed marker trait — restricts the blanket [`Queue`] impl below to the three sqlx
/// types we want to support, without conflicting with the per-driver impls in
/// `tokio_postgres` / `diesel_async` / `diesel_sync` adapters.
mod sealed {
    pub trait SqlxConn {}
    impl SqlxConn for &sqlx::PgPool {}
    impl SqlxConn for &mut sqlx::PgConnection {}
    impl SqlxConn for &mut sqlx::Transaction<'_, sqlx::Postgres> {}
}

#[async_trait]
impl<'c, A> Queue for A
where
    A: sealed::SqlxConn + Acquire<'c, Database = Postgres> + Send + 'c,
    A::Connection: Send,
{
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        sqlx::query(query::CREATE)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_unlogged(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        sqlx::query(query::CREATE_UNLOGGED)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_partitioned(self, queue_name: &str) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = queue_table_name(queue_name);
        let mut conn = self.acquire().await?;
        let exists: bool = sqlx::query_scalar::<_, bool>(query::CREATE_PARTITIONED_EXISTS_CHECK)
            .bind(&queue_table)
            .fetch_one(&mut *conn)
            .await?;
        if exists {
            return Ok(false);
        }
        sqlx::query(query::CREATE_PARTITIONED)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
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
        let mut conn = self.acquire().await?;
        let mut qb = sqlx::query(&sql).bind(table_name);
        if let Some(partition) = partition_interval {
            qb = qb.bind(partition);
        }
        if let Some(retention) = retention_interval {
            qb = qb.bind(retention);
        }
        qb.execute(&mut *conn).await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn drop_queue(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        sqlx::query(query::DROP_QUEUE)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn purge_queue(self, queue_name: &str) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_scalar::<_, i64>(query::PURGE_QUEUE)
            .bind(queue_name)
            .fetch_one(&mut *conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_queues(self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let mut conn = self.acquire().await?;
        let rows: Vec<PGMQueueMeta> = sqlx::query_as::<_, PGMQueueMeta>(query::LIST_QUEUES)
            .fetch_all(&mut *conn)
            .await?;
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
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, Message<T>>(query::SET_VT)
            .bind(queue_name)
            .bind(msg_id)
            .bind(visibility_timeout.into().as_seconds())
            .fetch_one(&mut *conn)
            .await?)
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
        let headers = headers.map(serde_json::to_value).transpose()?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_scalar::<_, i64>(query::SEND)
            .bind(queue_name)
            .bind(message)
            .bind(headers)
            .bind(delay.into().as_seconds())
            .fetch_one(&mut *conn)
            .await?)
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
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_scalar::<_, i64>(query::SEND_BATCH)
            .bind(queue_name)
            .bind(messages)
            .bind(headers)
            .bind(delay.into().as_seconds())
            .fetch_all(&mut *conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_batch<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, Message<T>>(query::READ)
            .bind(queue_name)
            .bind(visibility_timeout.into().as_seconds())
            .bind(qty)
            .fetch_all(&mut *conn)
            .await?)
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
        let mut conn = self.acquire().await?;
        let mut qb = sqlx::query_as::<_, Message<T>>(&sql)
            .bind(queue_name)
            .bind(visibility_timeout.into().as_seconds())
            .bind(max_batch_size);
        if let Some(timeout) = poll_timeout {
            qb = qb.bind(poll_timeout_secs(timeout));
        }
        if let Some(interval) = poll_interval {
            qb = qb.bind(duration_as_ms_i32(interval));
        }
        let rows: Vec<Message<T>> = qb.fetch_all(&mut *conn).await?;
        Ok(Some(rows))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED)
            .bind(queue_name)
            .bind(visibility_timeout.into().as_seconds())
            .bind(qty)
            .fetch_all(&mut *conn)
            .await?)
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
        let mut conn = self.acquire().await?;
        let mut qb = sqlx::query_as::<_, Message<T>>(&sql)
            .bind(queue_name)
            .bind(visibility_timeout.into().as_seconds())
            .bind(qty);
        if let Some(timeout) = poll_timeout {
            qb = qb.bind(poll_timeout_secs(timeout));
        }
        if let Some(interval) = poll_interval {
            qb = qb.bind(duration_as_ms_i32(interval));
        }
        Ok(qb.fetch_all(&mut *conn).await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped_head<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED_HEAD)
            .bind(queue_name)
            .bind(visibility_timeout.into().as_seconds())
            .bind(qty)
            .fetch_all(&mut *conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn read_grouped_rr<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
        visibility_timeout: impl Into<VisibilityTimeoutOffset> + Send,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, Message<T>>(query::READ_GROUPED_RR)
            .bind(queue_name)
            .bind(visibility_timeout.into().as_seconds())
            .bind(qty)
            .fetch_all(&mut *conn)
            .await?)
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
        let mut conn = self.acquire().await?;
        let mut qb = sqlx::query_as::<_, Message<T>>(&sql)
            .bind(queue_name)
            .bind(visibility_timeout.into().as_seconds())
            .bind(qty);
        if let Some(timeout) = poll_timeout {
            qb = qb.bind(poll_timeout_secs(timeout));
        }
        if let Some(interval) = poll_interval {
            qb = qb.bind(duration_as_ms_i32(interval));
        }
        Ok(qb.fetch_all(&mut *conn).await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn archive(self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_scalar::<_, bool>(query::ARCHIVE)
            .bind(queue_name)
            .bind(msg_id)
            .fetch_one(&mut *conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn archive_batch(self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        let rows = sqlx::query(query::ARCHIVE_BATCH)
            .bind(queue_name)
            .bind(msg_ids)
            .fetch_all(&mut *conn)
            .await?;
        Ok(rows.len())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn pop<T: for<'de> Deserialize<'de> + Send + Unpin + 'static>(
        self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, Message<T>>(query::POP)
            .bind(queue_name)
            .fetch_optional(&mut *conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn delete(self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_scalar::<_, bool>(query::DELETE)
            .bind(queue_name)
            .bind(msg_id)
            .fetch_one(&mut *conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn delete_batch(self, queue_name: &str, msg_ids: &[i64]) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        let rows = sqlx::query(query::DELETE_BATCH)
            .bind(queue_name)
            .bind(msg_ids)
            .fetch_all(&mut *conn)
            .await?;
        Ok(rows.len())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_fifo_index(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        sqlx::query(query::CREATE_FIFO_INDEX)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn create_fifo_indexes_all(self) -> Result<(), PgmqError> {
        let mut conn = self.acquire().await?;
        sqlx::query(query::CREATE_FIFO_INDEXES_ALL)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn bind_topic(self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        sqlx::query(query::BIND_TOPIC)
            .bind(pattern)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn unbind_topic(self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        sqlx::query(query::UNBIND_TOPIC)
            .bind(pattern)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_topic_bindings(
        self,
        queue_name: &str,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let mut conn = self.acquire().await?;
        Ok(
            sqlx::query_as::<_, ListTopicBindingsRow>(query::LIST_TOPIC_BINDINGS)
                .bind(queue_name)
                .fetch_all(&mut *conn)
                .await?,
        )
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_topic_bindings_all(self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let mut conn = self.acquire().await?;
        Ok(
            sqlx::query_as::<_, ListTopicBindingsRow>(query::LIST_TOPIC_BINDINGS_ALL)
                .fetch_all(&mut *conn)
                .await?,
        )
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
        let headers = headers.map(serde_json::to_value).transpose()?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_scalar::<_, i32>(query::SEND_TOPIC)
            .bind(routing_key)
            .bind(message)
            .bind(headers)
            .bind(delay.into().as_seconds())
            .fetch_one(&mut *conn)
            .await?)
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
        let mut conn = self.acquire().await?;
        Ok(
            sqlx::query_as::<_, SendBatchTopicRow>(query::SEND_BATCH_TOPIC)
                .bind(routing_key)
                .bind(messages)
                .bind(headers)
                .bind(delay.into().as_seconds())
                .fetch_all(&mut *conn)
                .await?,
        )
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn enable_notify_insert(
        self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = duration_as_ms_i32(throttle_interval);
        let mut conn = self.acquire().await?;
        sqlx::query(query::ENABLE_NOTIFY_INSERT)
            .bind(queue_name)
            .bind(ms)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn disable_notify_insert(self, queue_name: &str) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        sqlx::query(query::DISABLE_NOTIFY_INSERT)
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn update_notify_insert(
        self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let ms = duration_as_ms_i32(throttle_interval);
        let mut conn = self.acquire().await?;
        sqlx::query(query::UPDATE_NOTIFY_INSERT)
            .bind(queue_name)
            .bind(ms)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn list_notify_insert_throttles(
        self,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        let mut conn = self.acquire().await?;
        Ok(
            sqlx::query_as::<_, ListNotifyInsertThrottlesRow>(query::LIST_NOTIFY_INSERT_THROTTLES)
                .fetch_all(&mut *conn)
                .await?,
        )
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn metrics(self, queue_name: &str) -> Result<QueueMetrics, PgmqError> {
        check_input(queue_name)?;
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, QueueMetrics>(query::METRICS)
            .bind(queue_name)
            .fetch_one(&mut *conn)
            .await?)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn metrics_all(self) -> Result<Vec<QueueMetrics>, PgmqError> {
        let mut conn = self.acquire().await?;
        Ok(sqlx::query_as::<_, QueueMetrics>(query::METRICS_ALL)
            .fetch_all(&mut *conn)
            .await?)
    }
}

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
            .listen_all(channel_names.iter().map(String::as_str))
            .await?;
        Ok(listener)
    }
}
