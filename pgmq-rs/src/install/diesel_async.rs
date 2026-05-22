//! diesel-async install/migration entry points. Each function uses diesel-async's
//! `AsyncConnection::transaction(|conn| ...)` callback for multi-statement atomicity.

use super::internal::*;
use super::Version;
use crate::errors::PgmqError;
use crate::install::script::{ParsedScriptName, ScriptFetcher};
use diesel::sql_types;
use diesel::{sql_query, QueryableByName};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use std::str::FromStr;

#[derive(QueryableByName)]
struct AppliedMigrationRow {
    #[diesel(sql_type = sql_types::Text)]
    name: String,
    #[diesel(sql_type = sql_types::Text)]
    version: String,
}

pub async fn init(pool: &Pool<AsyncPgConnection>) -> Result<(), PgmqError> {
    let mut conn = pool.get().await?;
    conn.transaction::<_, PgmqError, _>(|conn| {
        async move {
            conn.batch_execute(INIT_SQL).await?;
            Ok(())
        }
        .scope_boxed()
    })
    .await
}

#[doc = include_str!("init_migrations_table.md")]
pub async fn init_migrations_table(
    pool: &Pool<AsyncPgConnection>,
    version: Version,
) -> Result<(), PgmqError> {
    let mut conn = pool.get().await?;
    conn.transaction::<_, PgmqError, _>(|conn| {
        async move {
            create_migrations_table(conn).await?;
            if !fetch_applied(conn).await?.is_empty() {
                return Ok(());
            }
            insert_applied(conn, &ParsedScriptName::init_script(version)).await?;
            Ok(())
        }
        .scope_boxed()
    })
    .await
}

#[doc = include_str!("installed_version.md")]
pub async fn installed_version(
    pool: &Pool<AsyncPgConnection>,
) -> Result<Option<Version>, PgmqError> {
    let mut conn = pool.get().await?;
    conn.transaction::<_, PgmqError, _>(|conn| {
        async move {
            create_migrations_table(conn).await?;
            let applied = fetch_applied(conn).await?;
            Ok(applied.into_iter().map(|a| a.version).max())
        }
        .scope_boxed()
    })
    .await
}

#[cfg(feature = "install-sql-github")]
#[doc = include_str!("./github/install_sql_github.md")]
pub async fn install_sql_from_github(
    pool: &Pool<AsyncPgConnection>,
    version: Option<&str>,
) -> Result<(), PgmqError> {
    let fetcher = super::internal::github_fetcher(version).await?;
    install_sql(pool, fetcher).await
}

#[cfg(feature = "install-sql-embedded")]
#[doc = include_str!("./embedded/install_sql_embedded.md")]
pub async fn install_sql_from_embedded(pool: &Pool<AsyncPgConnection>) -> Result<(), PgmqError> {
    install_sql(pool, embedded_fetcher()).await
}

async fn install_sql<F: ScriptFetcher + Send>(
    pool: &Pool<AsyncPgConnection>,
    script_fetcher: F,
) -> Result<(), PgmqError> {
    let mut conn = pool.get().await?;

    // Fetch scripts outside the transaction (network IO for the github fetcher).
    let installed_version_opt = {
        conn.transaction::<_, PgmqError, _>(|conn| {
            async move {
                create_migrations_table(conn).await?;
                let applied = fetch_applied(conn).await?;
                Ok(max_applied_version(&applied).cloned())
            }
            .scope_boxed()
        })
        .await?
    };
    let available = script_fetcher.fetch(installed_version_opt.as_ref()).await?;

    conn.transaction::<_, PgmqError, _>(|conn| {
        async move {
            create_migrations_table(conn).await?;
            let applied = fetch_applied(conn).await?;
            let to_apply = filter_unapplied_scripts(available, &applied);

            for script in &to_apply {
                // Multi-statement migration SQL — batch_execute equivalent in diesel-async is
                // running the whole thing as a single sql_query (which Postgres splits at
                // semicolons in a single message).
                sql_query(script.content.as_ref()).execute(conn).await?;
                insert_applied(conn, &script.name).await?;
            }
            Ok(())
        }
        .scope_boxed()
    })
    .await
}

async fn create_migrations_table(conn: &mut AsyncPgConnection) -> Result<(), PgmqError> {
    conn.batch_execute(SETUP_MIGRATIONS_TABLE_SQL).await?;
    Ok(())
}

async fn fetch_applied(conn: &mut AsyncPgConnection) -> Result<Vec<AppliedMigration>, PgmqError> {
    let rows: Vec<AppliedMigrationRow> =
        sql_query(SELECT_APPLIED_MIGRATIONS_SQL).load(conn).await?;
    rows.into_iter()
        .map(|r| {
            Ok(AppliedMigration {
                name: r.name,
                version: Version::from_str(&r.version)?,
            })
        })
        .collect()
}

async fn insert_applied(
    conn: &mut AsyncPgConnection,
    name: &ParsedScriptName,
) -> Result<(), PgmqError> {
    sql_query(INSERT_APPLIED_MIGRATION_SQL)
        .bind::<sql_types::Text, _>(&name.original)
        .bind::<sql_types::Text, _>(name.to.to_string())
        .execute(conn)
        .await?;
    Ok(())
}
