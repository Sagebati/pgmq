//! sqlx-native install/migration entry points. Each function opens a transaction on the
//! provided `sqlx::PgPool` and uses sqlx directly — no abstraction layer.

use crate::errors::PgmqError;
use crate::install::script::{ParsedScriptName, ScriptFetcher};
use super::internal::*;
use super::Version;
use sqlx::{PgPool, Postgres, Transaction};
use std::str::FromStr;

/// Initialize pgmq in the target database by creating the Postgres extension. Idempotent;
/// uses a transaction-scoped advisory lock to safely handle concurrent callers.
pub async fn init(pool: &PgPool) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(INIT_SQL).execute(&mut *tx).await?;
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
    let version = applied.into_iter().map(|a| a.version).max();
    tx.commit().await?;
    Ok(version)
}

#[cfg(feature = "install-sql-github")]
#[doc = include_str!("./github/install_sql_github.md")]
pub async fn install_sql_from_github(
    pool: &PgPool,
    version: Option<&str>,
) -> Result<(), PgmqError> {
    let fetcher = super::internal::github_fetcher(version).await?;
    install_sql(pool, fetcher).await
}

#[cfg(feature = "install-sql-embedded")]
#[doc = include_str!("./embedded/install_sql_embedded.md")]
pub async fn install_sql_from_embedded(pool: &PgPool) -> Result<(), PgmqError> {
    install_sql(pool, embedded_fetcher()).await
}

async fn install_sql(pool: &PgPool, script_fetcher: impl ScriptFetcher) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;
    create_migrations_table(&mut tx).await?;

    let applied = fetch_applied(&mut tx).await?;
    let installed_version = max_applied_version(&applied).cloned();

    let available = script_fetcher.fetch(installed_version.as_ref()).await?;
    let to_apply = filter_unapplied_scripts(available, &applied);

    for script in &to_apply {
        // Run the migration script's SQL.
        {
            use futures_util::StreamExt;
            use sqlx::Executor;
            let mut stream = tx.fetch_many(script.content.as_ref());
            while let Some(step) = stream.next().await {
                let _ = step?;
            }
        }
        // Record it as applied.
        insert_applied(&mut tx, &script.name).await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn create_migrations_table(tx: &mut Transaction<'static, Postgres>) -> Result<(), PgmqError> {
    sqlx::raw_sql(SETUP_MIGRATIONS_TABLE_SQL)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn fetch_applied(
    tx: &mut Transaction<'static, Postgres>,
) -> Result<Vec<AppliedMigration>, PgmqError> {
    let rows: Vec<(String, String)> =
        sqlx::query_as(SELECT_APPLIED_MIGRATIONS_SQL)
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
