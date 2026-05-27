//! Custom error types for PGMQ.
//!
//! The driver-specific variants preserve the source error so callers can downcast or pattern-match
//! on the underlying driver type (e.g. `sqlx::Error::RowNotFound`).
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PgmqError {
    /// JSON parsing error.
    #[error("json parsing error: {0}")]
    JsonParsingError(#[from] serde_json::error::Error),

    /// Failed to decode a column from a returned row.
    #[error("row decode error: column '{column}': {reason}")]
    RowDecodeError { column: String, reason: String },

    /// Returned when a queue/topic name fails client-side validation. Names must be
    /// non-empty, at most 47 characters, and contain only ASCII alphanumerics or underscores.
    #[error("invalid queue name: '{name}'")]
    InvalidQueueName { name: String },

    /// General installation operations error.
    #[cfg(feature = "install-sql")]
    #[error("installation error: {0}")]
    InstallationError(String),

    /// URL parse error, raised by the deprecated [`crate::PGMQueueExt::new`] constructor.
    #[cfg(feature = "sqlx")]
    #[error("url parse error: {0}")]
    UrlParseError(String),

    // ----- driver-specific variants -----
    /// sqlx driver error preserving the source.
    #[cfg(feature = "sqlx")]
    #[error("sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),

    /// tokio-postgres driver error.
    #[cfg(feature = "tokio-postgres")]
    #[error("tokio-postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),
}
