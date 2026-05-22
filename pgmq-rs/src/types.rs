use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::time::Duration;

pub const VT_DEFAULT: i32 = 30;
pub const READ_LIMIT_DEFAULT: i32 = 1;
pub const POLL_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);
pub const POLL_INTERVAL_DEFAULT: Duration = Duration::from_millis(250);

pub const QUEUE_PREFIX: &str = r#"q"#;
pub const ARCHIVE_PREFIX: &str = r#"a"#;
pub const PGMQ_SCHEMA: &str = "pgmq";

/// Metadata returned by `pgmq.list_queues()`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), derive(diesel::QueryableByName))]
pub struct PGMQueueMeta {
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Text))]
    pub queue_name: String,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Bool))]
    pub is_partitioned: bool,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Bool))]
    pub is_unlogged: bool,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Timestamptz))]
    pub created_at: DateTime<Utc>,
}

/// Message struct received from the queue.
///
/// Generic over the message body type `T`. The body is stored as JSONB in Postgres and
/// deserialized via serde on the way out.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct Message<T = serde_json::Value> {
    pub msg_id: i64,
    pub vt: DateTime<Utc>,
    pub enqueued_at: DateTime<Utc>,
    pub read_ct: i32,
    #[cfg_attr(feature = "sqlx", sqlx(json))]
    pub message: T,
}

/// A row returned by `pgmq.send_batch_topic`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), derive(diesel::QueryableByName))]
#[non_exhaustive]
pub struct SendBatchTopicRow {
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Text))]
    pub queue_name: String,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::BigInt))]
    pub msg_id: i64,
}

/// A row returned by `pgmq.list_topic_bindings`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), derive(diesel::QueryableByName))]
#[non_exhaustive]
pub struct ListTopicBindingsRow {
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Text))]
    pub pattern: String,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Text))]
    pub queue_name: String,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Timestamptz))]
    pub bound_at: DateTime<Utc>,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Text))]
    pub compiled_regex: String,
}

/// A row returned by `pgmq.list_notify_insert_throttles`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), derive(diesel::QueryableByName))]
#[non_exhaustive]
pub struct ListNotifyInsertThrottlesRow {
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Text))]
    pub queue_name: String,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Integer))]
    pub throttle_interval_ms: i32,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Timestamptz))]
    pub last_notified_at: DateTime<Utc>,
}

/// Metrics for a queue, returned by `pgmq.metrics` / `pgmq.metrics_all`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), derive(diesel::QueryableByName))]
#[non_exhaustive]
pub struct QueueMetrics {
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Text))]
    pub queue_name: String,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::BigInt))]
    pub queue_length: i64,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>))]
    pub newest_msg_age_sec: Option<i32>,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>))]
    pub oldest_msg_age_sec: Option<i32>,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::BigInt))]
    pub total_messages: i64,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::Timestamptz))]
    pub scrape_time: DateTime<Utc>,
    #[cfg_attr(any(feature = "diesel-async", feature = "diesel-sync"), diesel(sql_type = diesel::sql_types::BigInt))]
    pub queue_visible_length: i64,
}

// -------- tokio-postgres manual row decoders --------
//
// tokio-postgres has no derive macro for `Row -> Struct`, so we hand-write a small
// `from_tokio_postgres_row` per DTO. Reads columns by name from the wire-protocol decoder.

#[cfg(feature = "tokio-postgres")]
mod tpg_decode {
    use super::*;
    use crate::errors::PgmqError;

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
        pub(crate) fn from_tokio_postgres_row(
            row: &tokio_postgres::Row,
        ) -> Result<Self, PgmqError> {
            Ok(Self {
                queue_name: col(row.try_get("queue_name"), "queue_name")?,
                is_partitioned: col(row.try_get("is_partitioned"), "is_partitioned")?,
                is_unlogged: col(row.try_get("is_unlogged"), "is_unlogged")?,
                created_at: col(row.try_get("created_at"), "created_at")?,
            })
        }
    }

    impl<T: for<'de> Deserialize<'de>> Message<T> {
        pub(crate) fn from_tokio_postgres_row(
            row: &tokio_postgres::Row,
        ) -> Result<Self, PgmqError> {
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
        pub(crate) fn from_tokio_postgres_row(
            row: &tokio_postgres::Row,
        ) -> Result<Self, PgmqError> {
            Ok(Self {
                queue_name: col(row.try_get("queue_name"), "queue_name")?,
                msg_id: col(row.try_get("msg_id"), "msg_id")?,
            })
        }
    }

    impl ListTopicBindingsRow {
        pub(crate) fn from_tokio_postgres_row(
            row: &tokio_postgres::Row,
        ) -> Result<Self, PgmqError> {
            Ok(Self {
                pattern: col(row.try_get("pattern"), "pattern")?,
                queue_name: col(row.try_get("queue_name"), "queue_name")?,
                bound_at: col(row.try_get("bound_at"), "bound_at")?,
                compiled_regex: col(row.try_get("compiled_regex"), "compiled_regex")?,
            })
        }
    }

    impl ListNotifyInsertThrottlesRow {
        pub(crate) fn from_tokio_postgres_row(
            row: &tokio_postgres::Row,
        ) -> Result<Self, PgmqError> {
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
        pub(crate) fn from_tokio_postgres_row(
            row: &tokio_postgres::Row,
        ) -> Result<Self, PgmqError> {
            Ok(Self {
                queue_name: col(row.try_get("queue_name"), "queue_name")?,
                queue_length: col(row.try_get("queue_length"), "queue_length")?,
                newest_msg_age_sec: col(
                    row.try_get("newest_msg_age_sec"),
                    "newest_msg_age_sec",
                )?,
                oldest_msg_age_sec: col(
                    row.try_get("oldest_msg_age_sec"),
                    "oldest_msg_age_sec",
                )?,
                total_messages: col(row.try_get("total_messages"), "total_messages")?,
                scrape_time: col(row.try_get("scrape_time"), "scrape_time")?,
                queue_visible_length: col(
                    row.try_get("queue_visible_length"),
                    "queue_visible_length",
                )?,
            })
        }
    }
}
