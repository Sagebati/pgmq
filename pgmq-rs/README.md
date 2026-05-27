# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

The Rust client for [PGMQ](https://github.com/pgmq/pgmq), a lightweight, durable message queue built on top of the
`pgmq` Postgres extension.

`pgmq` is an **extension trait**: bring [`Queue`] into scope and call queue methods directly on your existing
Postgres connection or transaction. There's no constructor and no pool wrapper — you bring your own pool (or no pool
at all). Works with **sqlx** (default), **tokio-postgres**, **diesel-async**, and **diesel** (sync).

## Quick start (sqlx)

```toml
[dependencies]
pgmq = { version = "0.34", features = ["install-sql-embedded"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
```

```rust
use pgmq::Queue;
use pgmq::pg_ext::VisibilityTimeoutOffset;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct MyMessage { foo: String }

#[tokio::main]
async fn main() -> Result<(), pgmq::PgmqError> {
    let pool = sqlx::PgPool::connect("postgres://postgres:postgres@localhost:5432/postgres").await.unwrap();

    // One-time: install the pgmq schema into your database (idempotent).
    pgmq::install::sqlx::install_sql_from_embedded(&pool).await?;

    // Acquire a connection from your pool and call methods on it.
    let mut conn = pool.acquire().await.unwrap();
    conn.create("my_queue").await?;

    let id = conn.send("my_queue", &MyMessage { foo: "bar".into() }).await?;

    let received: Option<pgmq::Message<MyMessage>> =
        conn.read("my_queue", VisibilityTimeoutOffset::seconds(30)).await?;
    if let Some(msg) = received {
        conn.archive("my_queue", msg.msg_id).await?;
    }
    Ok(())
}
```

## Supported drivers

| Driver | Feature | `Queue` implemented on |
|---|---|---|
| [sqlx](https://github.com/launchbadge/sqlx) (default) | `sqlx` | `&mut PgConnection`, `&mut Transaction<'_, Postgres>` |
| [tokio-postgres](https://github.com/sfackler/rust-postgres) | `tokio-postgres` | `&Client`, `&Transaction<'_>` |
| [diesel-async](https://github.com/weiznich/diesel_async) | `diesel-async` | `&mut AsyncPgConnection` |
| [diesel](https://github.com/diesel-rs/diesel) (sync) | `diesel-sync` | `&mut PgConnection` |

All four drivers share the same trait surface. Switching drivers means changing how you obtain a connection — the queue
calls themselves are identical.

```toml
# Use tokio-postgres instead of sqlx
pgmq = { version = "0.34", default-features = false, features = ["tokio-postgres", "install-sql-embedded"] }
```

Each adapter has its own module-level documentation covering setup, pool usage, transactions, and install with
runnable examples: see [`pgmq::adapters::sqlx`](https://docs.rs/pgmq/latest/pgmq/adapters/sqlx/),
[`pgmq::adapters::tokio_postgres`](https://docs.rs/pgmq/latest/pgmq/adapters/tokio_postgres/),
[`pgmq::adapters::diesel_async`](https://docs.rs/pgmq/latest/pgmq/adapters/diesel_async/),
[`pgmq::adapters::diesel_sync`](https://docs.rs/pgmq/latest/pgmq/adapters/diesel_sync/).

## Composing with your own transactions

Each adapter also implements `Queue` on its driver's transaction type, so enqueue/dequeue can be atomic with
your own business work:

```rust,no_run
# use pgmq::Queue;
# async fn example(pool: sqlx::PgPool) -> Result<(), pgmq::PgmqError> {
let mut tx = pool.begin().await?;
sqlx::query("INSERT INTO orders (id) VALUES ($1)").bind(1i64).execute(&mut *tx).await?;
tx.send("orders_queue", &"order #1").await?;
tx.commit().await?;
# Ok(())
# }
```

## Installing PGMQ

PGMQ can be installed into any Postgres database directly from this client — useful when the `pgmq` extension binary
isn't available on your Postgres instance. Two install methods, both idempotent and version-aware:

- **Embedded** (`install-sql-embedded`): SQL scripts shipped with the crate. No network. Pins to the version bundled
  with this crate.
- **From GitHub** (`install-sql-github`): fetches scripts from the pgmq GitHub repo. Requires network. Lets you pin
  a specific extension version.

### In Rust

```rust,no_run
# async fn ex() -> Result<(), pgmq::PgmqError> {
let pool = sqlx::PgPool::connect("postgres://...").await.unwrap();
pgmq::install::sqlx::install_sql_from_embedded(&pool).await?;
// or pin a specific version from GitHub:
pgmq::install::sqlx::install_sql_from_github(&pool, Some("1.9.0")).await?;
# Ok(())
# }
```

For other drivers, see the per-driver install module:
[`pgmq::install::tokio_postgres`](https://docs.rs/pgmq/latest/pgmq/install/tokio_postgres/),
[`pgmq::install::diesel_async`](https://docs.rs/pgmq/latest/pgmq/install/diesel_async/),
[`pgmq::install::diesel_sync`](https://docs.rs/pgmq/latest/pgmq/install/diesel_sync/) (sync, embedded only).

### Via CLI (one-shot install, no app code)

```bash
cargo install pgmq --features cli --bin pgmq-cli
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres install-from-embedded
# or
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres install-from-github -v 1.9.0
```

### Upgrading from pre-versioned installs (<= 0.32.1)

If you used the very old pre-versioned installer, run `init-migrations-table` once to seed the migration tracking
table at your current version, then run the regular installer:

```bash
pgmq-cli install -d postgres://... init-migrations-table -v 1.9.0
pgmq-cli install -d postgres://... install-from-embedded
```

Not needed for fresh installations.

## Optional features

- `tracing` — adds `#[tracing::instrument(skip_all)]` to every queue method. Off by default; opt in with `features = ["tracing"]`. Conservative defaults — span timing only, no payload capture.

## Examples

Runnable examples in [`examples/`](./examples/):

```bash
cargo run --example basic
cargo run --example transactions --features install-sql-embedded
cargo run --example tokio_postgres_basic --no-default-features --features tokio-postgres,install-sql-embedded
cargo run --example install --features install-sql-github,install-sql-embedded
```

## Messages

Messages can be any `Serialize + Deserialize` type. `conn.read::<T>("queue", vt)` returns
`Result<Option<Message<T>>, PgmqError>`; `PgmqError::JsonParsingError` is returned if the on-queue payload can't
deserialize to `T`, and `PgmqError::DatabaseError` if the underlying driver fails.

License: [PostgreSQL](LICENSE)
