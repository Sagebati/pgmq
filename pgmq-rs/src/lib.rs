//! # Postgres Message Queue (PGMQ)
//!
//! [![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)
//!
//! A lightweight, durable message queue built on top of the [pgmq Postgres extension].
//!
//! [pgmq Postgres extension]: https://github.com/pgmq/pgmq
//!
//! ## How this crate works
//!
//! `pgmq` is an **extension trait** on top of your existing Postgres driver. There is no
//! `PGMQClient::new(...)` constructor that opens its own pool — you bring your own
//! connection (or your own pool), and the trait adds queue methods to it.
//!
//! Bring [`Queue`] into scope and call queue methods directly on a connection or
//! transaction:
//!
//! ```ignore
//! use pgmq::Queue;
//!
//! let mut conn = your_pool.acquire().await?;
//! conn.create("my_queue").await?;
//! conn.send("my_queue", &payload).await?;
//! ```
//!
//! ## Supported drivers
//!
//! Each driver has its own cargo feature and adapter module. The trait is implemented on the
//! driver's connection and transaction types, *not* on its pool — this means pgmq works with
//! any pool implementation (sqlx pool, deadpool, bb8, mobc, custom, or no pool at all).
//!
//! | Driver | Feature | Async? | Implements `Queue` on |
//! |--------|---------|--------|------------------------------|
//! | [sqlx](https://github.com/launchbadge/sqlx) (**default**) | `sqlx` | yes | `&PgPool`, `&mut PgConnection`, `&mut Transaction<'_, Postgres>` |
//! | [tokio-postgres](https://github.com/sfackler/rust-postgres) | `tokio-postgres` | yes | `&Client`, `&Transaction<'_>` |
//! | [diesel-async](https://github.com/weiznich/diesel_async) | `diesel-async` | yes | `&mut AsyncPgConnection` |
//! | [diesel](https://github.com/diesel-rs/diesel) (sync) | `diesel-sync` | sync body, async signature | `&mut PgConnection` |
//!
//! All four drivers share the same `Queue` trait surface — same method names, same
//! return types. Switching drivers requires changing your imports and how you obtain a
//! connection, but the queue calls themselves are identical.
//!
//! ## Quick start (sqlx, the default)
//!
//! ```rust,no_run
//! use pgmq::{PgmqError, Message, Queue};
//! use serde::{Deserialize, Serialize};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), PgmqError> {
//!     let pool = sqlx::PgPool::connect("postgres://postgres:postgres@0.0.0.0:5432").await.unwrap();
//!
//!     // One-time: install the pgmq schema into your database.
//!     #[cfg(feature = "install-sql-embedded")]
//!     pgmq::install::sqlx::install_sql_from_embedded(&pool).await?;
//!
//!     // Get a connection and use it.
//!     let mut conn = pool.acquire().await.unwrap();
//!     conn.create("my_queue").await?;
//!
//!     #[derive(Serialize, Deserialize, Debug)]
//!     struct MyMessage { foo: String }
//!     let msg_id = conn.send("my_queue", &MyMessage { foo: "bar".into() }).await?;
//!
//!     let received: Option<Message<MyMessage>> = conn
//!         .read("my_queue", 30).await?;
//!     if let Some(msg) = received {
//!         assert_eq!(msg.msg_id, msg_id);
//!         conn.archive("my_queue", msg.msg_id).await?;
//!     }
//!     Ok(())
//! }
//! ```
//!
//! For the other drivers, see [`adapters::tokio_postgres`], [`adapters::diesel`] (async), and
//! [`adapters::diesel::sync`]. Each has an exhaustive module doc covering setup, pool usage,
//! transactions, and install.
//!
//! ## Common patterns
//!
//! ### Auto-reborrow
//!
//! `Queue` methods take `self` by value, but `Self` is always a reference type
//! (`&mut PgConnection`, `&Client`, etc.). Rust's auto-reborrow rules mean each call
//! consumes a *fresh* reborrow, not the original binding:
//!
//! ```ignore
//! let mut conn = pool.acquire().await?;       // owned connection
//! conn.create("q").await?;                     // auto-reborrows &mut *conn
//! conn.send("q", &msg).await?;                 // fresh &mut *conn
//! conn.archive("q", id).await?;                // fresh again — original `conn` still usable
//! ```
//!
//! With shared references (`&Client`, `&PgPool` in some drivers), it's the same idea —
//! `&conn` is `Copy`, so reborrowing is free.
//!
//! ### Composing with user-managed transactions
//!
//! Every adapter implements `Queue` on its driver's transaction type, so you can run
//! pgmq calls inside the same transaction as the rest of your business work. Concrete
//! examples appear in each adapter's module doc.
//!
//! ### Install once, run many
//!
//! The pgmq extension is installed into a database one time (per database). Each adapter has
//! a matching install module:
//!
//! - `pgmq::install::sqlx::install_sql_from_embedded(&pool).await?;`
//! - `pgmq::install::tokio_postgres::install_sql_from_embedded(&pool).await?;`
//! - `pgmq::install::diesel_async::install_sql_from_embedded(&pool).await?;`
//! - `pgmq::install::diesel_sync::install_sql_from_embedded(&mut conn)?;`
//!
//! Install is idempotent — calling it on an already-installed database is a no-op. There's
//! also `install_sql_from_github` (async drivers only) that fetches a specific version
//! from GitHub instead of using the embedded scripts.

#![doc(html_root_url = "https://docs.rs/pgmq/")]

pub mod adapters;
pub mod errors;
#[cfg(feature = "install-sql")]
pub mod install;
pub mod pg_ext;
pub mod types;

pub use errors::PgmqError;
#[cfg(feature = "sqlx")]
#[allow(deprecated)]
pub use pg_ext::PGMQueueExt;
pub use pg_ext::Queue;
pub use types::Message;
