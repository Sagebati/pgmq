//! sqlx-native install/migration entry points. Each function opens a transaction on the
//! provided `sqlx::PgPool` and uses sqlx directly — no abstraction layer.

use super::internal::*;
use super::Version;
use crate::errors::PgmqError;
use crate::install::script::ParsedScriptName;
#[cfg(feature = "install-sql-github")]
use crate::install::script::ScriptFetcher;
use sqlx::{PgPool, Postgres, Transaction};
use std::str::FromStr;

/// Initialize pgmq in the target database by creating the Postgres extension. Idempotent;
/// uses a transaction-scoped advisory lock to safely handle concurrent callers.
pub async fn init(pool: &PgPool) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(&init_sql()).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

#[doc = include_str!("init_migrations_table.md")]
pub async fn init_migrations_table(pool: &PgPool, version: Version) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;
    create_migrations_table(&mut tx).await?;
    if !fetch_applied(&mut tx).await?.is_empty() {
        return Ok(());
    }
    insert_applied(&mut tx, &ParsedScriptName::init_script(version)).await?;
    tx.commit().await?;
    Ok(())
}

#[doc = include_str!("installed_version.md")]
pub async fn installed_version(pool: &PgPool) -> Result<Option<Version>, PgmqError> {
    let mut tx = pool.begin().await?;
    create_migrations_table(&mut tx).await?;
    let applied = fetch_applied(&mut tx).await?;
    let version = applied.into_iter().map(|mig| mig.version).max();
    tx.commit().await?;
    Ok(version)
}

#[cfg(feature = "install-sql-github")]
#[doc = include_str!("./github/install_sql_github.md")]
pub async fn install_sql_from_github(
    pool: &PgPool,
    version: Option<&str>,
) -> Result<(), PgmqError> {
    // Observe the installed version in a short read transaction, release the lock, do the
    // network fetch, then reacquire the lock for the apply transaction. Holding the migrations
    // advisory lock across network I/O would block other installers for the duration of the
    // HTTP round trip; `filter_unapplied_scripts` defends against races inside the apply tx.
    let mut tx = pool.begin().await?;
    create_migrations_table(&mut tx).await?;
    let installed_version = max_applied_version(&fetch_applied(&mut tx).await?).cloned();
    tx.commit().await?;

    let fetcher = super::internal::github_fetcher(version).await?;
    let available = fetcher.fetch(installed_version.as_ref()).await?;
    apply_scripts(pool, available).await
}

#[cfg(feature = "install-sql-embedded")]
#[doc = include_str!("./embedded/install_sql_embedded.md")]
pub async fn install_sql_from_embedded(pool: &PgPool) -> Result<(), PgmqError> {
    // Embedded fetcher is in-memory only — fetch all scripts up front, then apply inside a
    // single transaction that holds the advisory lock across the whole install.
    let available = embedded_fetcher().fetch_sync(None)?;
    apply_scripts(pool, available).await
}

#[cfg(any(feature = "install-sql-embedded", feature = "install-sql-github"))]
async fn apply_scripts(
    pool: &PgPool,
    available: Vec<crate::install::script::MigrationScript>,
) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;
    create_migrations_table(&mut tx).await?;
    let applied = fetch_applied(&mut tx).await?;
    let to_apply = filter_unapplied_scripts(available, &applied);

    for script in &to_apply {
        {
            use futures_util::StreamExt;
            use sqlx::Executor;
            let mut stream = tx.fetch_many(script.content.as_ref());
            while let Some(step) = stream.next().await {
                let _ = step?;
            }
        }
        insert_applied(&mut tx, &script.name).await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn create_migrations_table(tx: &mut Transaction<'static, Postgres>) -> Result<(), PgmqError> {
    sqlx::raw_sql(&setup_migrations_table_sql())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn fetch_applied(
    tx: &mut Transaction<'static, Postgres>,
) -> Result<Vec<AppliedMigration>, PgmqError> {
    let rows: Vec<(String, String)> = sqlx::query_as(SELECT_APPLIED_MIGRATIONS_SQL)
        .fetch_all(&mut **tx)
        .await?;
    rows.into_iter()
        .map(|(name, ver)| {
            Ok(AppliedMigration {
                name,
                version: Version::from_str(&ver)?,
            })
        })
        .collect()
}

async fn insert_applied(
    tx: &mut Transaction<'static, Postgres>,
    name: &ParsedScriptName,
) -> Result<(), PgmqError> {
    sqlx::query(INSERT_APPLIED_MIGRATION_SQL)
        .bind(&name.original)
        .bind(name.to.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}
