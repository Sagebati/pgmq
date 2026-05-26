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
pub fn poll_timeout_secs(d: std::time::Duration) -> i32 {
    i32::try_from(d.as_secs()).unwrap_or(i32::MAX)
}

pub fn poll_interval_ms(d: std::time::Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX)
}

pub fn serialize_list<T: Serialize>(
    list: &[T],
) -> Result<Vec<serde_json::Value>, serde_json::Error> {
    list.iter().map(serde_json::to_value).collect()
}

pub fn serialize_optional_list<H: Serialize>(
    list: Option<&[H]>,
) -> Result<Option<Vec<serde_json::Value>>, serde_json::Error> {
    match list {
        Some(l) => Ok(Some(serialize_list(l)?)),
        None => Ok(None),
    }
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
