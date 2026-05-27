//! # Driver adapters
//!
//! Each driver implements [`crate::Queue`] for its connection and transaction types.
//! Pick the adapter for the driver you're already using:
//!
//! | Driver | Module | Cargo feature |
//! |--------|--------|----------------|
//! | [sqlx](https://github.com/launchbadge/sqlx) | [`sqlx`] | `sqlx` (default) |
//! | [tokio-postgres](https://github.com/sfackler/rust-postgres) | [`tokio_postgres`] | `tokio-postgres` |
//! | [diesel-async](https://github.com/weiznich/diesel_async) | [`diesel`] | `diesel-async` |
//! | [diesel](https://github.com/diesel-rs/diesel) (sync) | [`diesel::sync`] | `diesel-sync` |
//!
//! Each module's documentation covers setup, pool usage, transactions, and install with
//! runnable examples for that driver.

// Private — SQL constants and shared helpers used by the sibling adapters only.
// Adapters access via `super::query::*` and `super::helpers::*`. Both modules are
// gated on at least one driver feature being enabled; otherwise their contents are
// dead code.
#[cfg(any(
    feature = "sqlx",
    feature = "tokio-postgres",
    feature = "diesel-async",
    feature = "diesel-sync"
))]
mod helpers;
#[cfg(any(
    feature = "sqlx",
    feature = "tokio-postgres",
    feature = "diesel-async",
    feature = "diesel-sync"
))]
mod query;

#[cfg(feature = "sqlx")]
pub mod sqlx;

#[cfg(feature = "tokio-postgres")]
pub mod tokio_postgres;

#[cfg(any(feature = "diesel-async", feature = "diesel-sync"))]
pub mod diesel;
