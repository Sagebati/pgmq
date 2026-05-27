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
pub struct PGMQueueMeta {
    pub queue_name: String,
    pub is_partitioned: bool,
    pub is_unlogged: bool,
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
#[non_exhaustive]
pub struct SendBatchTopicRow {
    pub queue_name: String,
    pub msg_id: i64,
}

/// A row returned by `pgmq.list_topic_bindings`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[non_exhaustive]
pub struct ListTopicBindingsRow {
    pub pattern: String,
    pub queue_name: String,
    pub bound_at: DateTime<Utc>,
    pub compiled_regex: String,
}

/// A row returned by `pgmq.list_notify_insert_throttles`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[non_exhaustive]
pub struct ListNotifyInsertThrottlesRow {
    pub queue_name: String,
    pub throttle_interval_ms: i32,
    pub last_notified_at: DateTime<Utc>,
}

/// Metrics for a queue, returned by `pgmq.metrics` / `pgmq.metrics_all`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[non_exhaustive]
pub struct QueueMetrics {
    pub queue_name: String,
    pub queue_length: i64,
    pub newest_msg_age_sec: Option<i32>,
    pub oldest_msg_age_sec: Option<i32>,
    pub total_messages: i64,
    pub scrape_time: DateTime<Utc>,
    pub queue_visible_length: i64,
}
