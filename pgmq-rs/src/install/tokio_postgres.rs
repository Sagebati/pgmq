//! tokio-postgres install/migration entry points.
//!
//! These take `&mut tokio_postgres::Client` — bring your own pool, acquire a client, then
//! pass it in. The installer opens its own transaction internally for atomicity.

use super::internal::*;
use super::Version;
use crate::errors::PgmqError;
use crate::install::script::{ParsedScriptName, ScriptFetcher};
use std::str::FromStr;
use tokio_postgres::{Client, Transaction};

/// Initialize pgmq in the target database by creating the Postgres extension. Idempotent;
/// uses a transaction-scoped advisory lock to safely handle concurrent callers.
pub async fn init(client: &mut Client) -> Result<(), PgmqError> {
    let tx = client.transaction().await?;
    tx.batch_execute(&init_sql()).await?;
    tx.commit().await?;
    Ok(())
}

#[doc = include_str!("init_migrations_table.md")]
pub async fn init_migrations_table(client: &mut Client, version: Version) -> Result<(), PgmqError> {
    let tx = client.transaction().await?;
    create_migrations_table(&tx).await?;
    if !fetch_applied(&tx).await?.is_empty() {
        return Ok(());
    }
    insert_applied(&tx, &ParsedScriptName::init_script(version)).await?;
    tx.commit().await?;
    Ok(())
}

#[doc = include_str!("installed_version.md")]
pub async fn installed_version(client: &mut Client) -> Result<Option<Version>, PgmqError> {
    let tx = client.transaction().await?;
    create_migrations_table(&tx).await?;
    let applied = fetch_applied(&tx).await?;
    let version = applied.into_iter().map(|mig| mig.version).max();
    tx.commit().await?;
    Ok(version)
}

#[cfg(feature = "install-sql-github")]
#[doc = include_str!("./github/install_sql_github.md")]
pub async fn install_sql_from_github(
    client: &mut Client,
    version: Option<&str>,
) -> Result<(), PgmqError> {
    let fetcher = super::internal::github_fetcher(version).await?;
    install_sql(client, fetcher).await
}

#[cfg(feature = "install-sql-embedded")]
#[doc = include_str!("./embedded/install_sql_embedded.md")]
pub async fn install_sql_from_embedded(client: &mut Client) -> Result<(), PgmqError> {
    install_sql(client, embedded_fetcher()).await
}

async fn install_sql(
    client: &mut Client,
    script_fetcher: impl ScriptFetcher,
) -> Result<(), PgmqError> {
    let tx = client.transaction().await?;
    create_migrations_table(&tx).await?;

    let applied = fetch_applied(&tx).await?;
    let installed_version = max_applied_version(&applied).cloned();

    let available = script_fetcher.fetch(installed_version.as_ref()).await?;
    let to_apply = filter_unapplied_scripts(available, &applied);

    for script in &to_apply {
        // Run the migration script's SQL (may contain multiple statements).
        tx.batch_execute(&script.content).await?;
        insert_applied(&tx, &script.name).await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn create_migrations_table(tx: &Transaction<'_>) -> Result<(), PgmqError> {
    tx.batch_execute(&setup_migrations_table_sql()).await?;
    Ok(())
}

async fn fetch_applied(tx: &Transaction<'_>) -> Result<Vec<AppliedMigration>, PgmqError> {
    let rows = tx.query(SELECT_APPLIED_MIGRATIONS_SQL, &[]).await?;
    rows.into_iter()
        .map(|row| {
            let name: String = row.try_get(0).map_err(|e| PgmqError::RowDecodeError {
                column: "name".into(),
                reason: e.to_string(),
            })?;
            let ver: String = row.try_get(1).map_err(|e| PgmqError::RowDecodeError {
                column: "version".into(),
                reason: e.to_string(),
            })?;
            Ok(AppliedMigration {
                name,
                version: Version::from_str(&ver)?,
            })
        })
        .collect()
}

async fn insert_applied(tx: &Transaction<'_>, name: &ParsedScriptName) -> Result<(), PgmqError> {
    let version = name.to.to_string();
    tx.execute(INSERT_APPLIED_MIGRATION_SQL, &[&name.original, &version])
        .await?;
    Ok(())
}
