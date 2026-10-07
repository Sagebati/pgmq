//! SQL-only installation for `diesel-async`. These are the `diesel-async` counterparts of the
//! `sqlx` entry points in the parent module (`install_sql_from_embedded` & co.), and they keep
//! the same `pgmq.__pgmq_migrations` bookkeeping, so the two drivers can be mixed against one
//! database and the `pgmq-cli` tool stays usable.
//!
//! Each function borrows an [`AsyncPgConnection`] rather than a pool so that callers are free to
//! use any pool implementation (bb8, deadpool, mobc, ...).

use crate::install::script::ParsedScriptName;
#[cfg(any(feature = "install-sql-embedded", feature = "install-sql-github"))]
use crate::install::script::ScriptFetcher;
use crate::install::sql::{
    CREATE_MIGRATIONS_TABLE, CREATE_SCHEMA, INSERT_APPLIED_MIGRATION, LOCK_MIGRATIONS_TABLE,
    SELECT_APPLIED_MIGRATIONS,
};
use crate::install::Version;
use crate::util::ADVISORY_LOCK_KEY;
use crate::PgmqError;
use diesel::sql_types::{BigInt, Text};
use diesel::{sql_query, QueryableByName};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
#[cfg(any(feature = "install-sql-embedded", feature = "install-sql-github"))]
use itertools::Itertools;
use std::str::FromStr;

/// A row of `pgmq.__pgmq_migrations`.
#[derive(QueryableByName)]
#[cfg_attr(
    not(any(feature = "install-sql-embedded", feature = "install-sql-github")),
    allow(dead_code)
)]
struct AppliedMigrationRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    version: String,
}

#[doc = include_str!("init_migrations_table.md")]
pub async fn init_migrations_table(
    conn: &mut AsyncPgConnection,
    version: Version,
) -> Result<(), PgmqError> {
    conn.transaction::<_, PgmqError, _>(async |conn| {
        create_migrations_table(conn).await?;
        if !fetch_applied(conn).await?.is_empty() {
            // If the migration table already has items in it, it does not need to be initialized
            return Ok(());
        }
        insert_applied(conn, &ParsedScriptName::init_script(version)).await
    })
    .await
}

#[doc = include_str!("installed_version.md")]
pub async fn installed_version(conn: &mut AsyncPgConnection) -> Result<Option<Version>, PgmqError> {
    conn.transaction::<_, PgmqError, _>(async |conn| {
        create_migrations_table(conn).await?;
        fetch_applied(conn)
            .await?
            .iter()
            .map(|applied| Version::from_str(&applied.version))
            .collect::<Result<Vec<_>, _>>()
            .map(|versions| versions.into_iter().max())
    })
    .await
}

#[cfg(feature = "install-sql-github")]
#[doc = include_str!("./github/install_sql_github.md")]
pub async fn install_sql_from_github(
    conn: &mut AsyncPgConnection,
    version: Option<&str>,
) -> Result<(), PgmqError> {
    install_sql(
        conn,
        crate::install::github::GitHubScriptFetcher::new(version).await?,
    )
    .await
}

#[cfg(feature = "install-sql-embedded")]
#[doc = include_str!("./embedded/install_sql_embedded.md")]
pub async fn install_sql_from_embedded(conn: &mut AsyncPgConnection) -> Result<(), PgmqError> {
    install_sql(conn, crate::install::embedded::EmbeddedScriptFetcher).await
}

#[cfg(any(feature = "install-sql-embedded", feature = "install-sql-github"))]
async fn install_sql(
    conn: &mut AsyncPgConnection,
    script_fetcher: impl ScriptFetcher,
) -> Result<(), PgmqError> {
    // Fetching may do network I/O (GitHub), so it happens outside the locked transaction. The
    // apply step re-reads the applied set under the lock, so a concurrent installer that won the
    // race can't cause a script to run twice.
    let installed_version = installed_version(conn).await?;
    let scripts = script_fetcher.fetch(installed_version.as_ref()).await?;

    conn.transaction::<_, PgmqError, _>(async move |conn| {
        create_migrations_table(conn).await?;
        let applied = fetch_applied(conn).await?;

        let scripts = scripts
            .into_iter()
            .filter(|script| !applied.iter().any(|a| a.name == script.name.original))
            .sorted();

        for script in scripts {
            // Scripts are many `;`-separated statements, which only the simple-query protocol
            // (`batch_execute`) accepts; `sql_query` prepares a single statement.
            conn.batch_execute(&script.content).await?;
            insert_applied(conn, &script.name).await?;
        }
        Ok(())
    })
    .await
}

/// Same lock + DDL sequence as the sqlx path (`AppliedMigration::create_table`): a transaction-
/// scoped advisory lock serialises installers, and the table lock guards the applied list.
async fn create_migrations_table(conn: &mut AsyncPgConnection) -> Result<(), PgmqError> {
    sql_query("SELECT pg_advisory_xact_lock($1);")
        .bind::<BigInt, _>(ADVISORY_LOCK_KEY)
        .execute(conn)
        .await?;
    conn.batch_execute(CREATE_SCHEMA).await?;
    conn.batch_execute(CREATE_MIGRATIONS_TABLE).await?;
    conn.batch_execute(LOCK_MIGRATIONS_TABLE).await?;
    Ok(())
}

async fn fetch_applied(
    conn: &mut AsyncPgConnection,
) -> Result<Vec<AppliedMigrationRow>, PgmqError> {
    Ok(sql_query(SELECT_APPLIED_MIGRATIONS).load(conn).await?)
}

async fn insert_applied(
    conn: &mut AsyncPgConnection,
    name: &ParsedScriptName,
) -> Result<(), PgmqError> {
    sql_query(INSERT_APPLIED_MIGRATION)
        .bind::<Text, _>(&name.original)
        .bind::<Text, _>(name.to.to_string())
        .execute(conn)
        .await?;
    Ok(())
}
