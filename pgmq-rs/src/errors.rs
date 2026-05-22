//! Custom errors types for PGMQ
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PgmqError {
    /// a json parsing error
    #[error("json parsing error {0}")]
    JsonParsingError(#[from] serde_json::error::Error),

    /// a database error returned by the underlying driver
    #[error("database error: {0}")]
    DatabaseError(String),

    /// a connection pool error returned by the underlying driver
    #[error("pool error: {0}")]
    PoolError(String),

    /// failed to decode a column from a returned row
    #[error("row decode error: column '{column}': {reason}")]
    RowDecodeError { column: String, reason: String },

    /// a queue name error
    /// queue names must be alphanumeric and start with a letter
    #[error("invalid queue name: '{name}'")]
    InvalidQueueName { name: String },

    /// a general error for installation operations
    #[cfg(feature = "install-sql")]
    #[error("installation error: {0}")]
    InstallationError(String),
}

#[cfg(any(feature = "diesel-async", feature = "diesel-sync"))]
impl From<diesel::result::Error> for PgmqError {
    fn from(err: diesel::result::Error) -> Self {
        PgmqError::DatabaseError(err.to_string())
    }
}
