//! Helpers shared by the per-driver adapter implementations. Not part of the public API —
//! the `helpers` module is declared `mod helpers` (private) in `adapters/mod.rs`, so external
//! callers cannot reach `pgmq::adapters::helpers::*`.
//!
//! Items are `pub` so sibling adapter modules can use them via `super::helpers::*`.

use crate::errors::PgmqError;
use serde::Serialize;

/// Convert a `Duration` poll timeout to seconds as `i32`, clamping on overflow.
///
/// Note: the caller is responsible for deciding whether to pass this to the extension at all —
/// when the caller does not specify a poll timeout/interval, we omit the parameter from the SQL
/// entirely so the extension's own defaults apply (rather than hard-coding our own).
pub fn poll_timeout_secs(dur: std::time::Duration) -> i32 {
    i32::try_from(dur.as_secs()).unwrap_or(i32::MAX)
}

/// Convert a `Duration` to milliseconds as `i32`, clamping on overflow. Used both for poll
/// intervals (`read_*_with_poll`) and notify-insert throttle intervals (`enable_notify_insert`
/// / `update_notify_insert`).
pub fn duration_as_ms_i32(dur: std::time::Duration) -> i32 {
    i32::try_from(dur.as_millis()).unwrap_or(i32::MAX)
}

pub fn serialize_list<T: Serialize>(
    list: &[T],
) -> Result<Vec<serde_json::Value>, serde_json::Error> {
    list.iter().map(serde_json::to_value).collect()
}

pub fn serialize_optional_list<H: Serialize>(
    list: Option<&[H]>,
) -> Result<Option<Vec<serde_json::Value>>, serde_json::Error> {
    list.map(serialize_list).transpose()
}

/// Build the schema-qualified Postgres table name for a pgmq queue. The pgmq extension stores
/// each queue's messages in `pgmq.q_<queue_name>` by convention; this helper is the single
/// source of truth for that mapping (used by every adapter's `create_partitioned` to check
/// whether the parent table already exists in `part_config`).
pub fn queue_table_name(queue_name: &str) -> String {
    format!("pgmq.q_{queue_name}")
}

/// Validate a queue or topic name. Returns `Err(PgmqError::InvalidQueueName)` if it fails.
pub fn check_input(input: &str) -> Result<(), PgmqError> {
    let valid = input.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !input.is_empty()
        && input.len() <= 48;
    if valid {
        Ok(())
    } else {
        Err(PgmqError::InvalidQueueName {
            name: input.to_owned(),
        })
    }
}
