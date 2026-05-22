//! diesel (sync) install/migration entry points. Each function uses diesel's
//! `Connection::transaction(|conn| ...)` callback for multi-statement atomicity.
//!
//! Fully synchronous — no async runtime needed. If you want the GitHub installer for sync diesel
//! (which requires network I/O), use the async `diesel-async` adapter inside a small tokio
//! runtime instead — pgmq doesn't ship a sync HTTP path.

use crate::errors::PgmqError;
use crate::install::script::ParsedScriptName;
use super::internal::*;
use super::Version;
use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::{sql_query, sql_types, Connection, QueryableByName, RunQueryDsl};
use std::str::FromStr;

#[derive(QueryableByName)]
struct AppliedMigrationRow {
    #[diesel(sql_type = sql_types::Text)]
    name: String,
    #[diesel(sql_type = sql_types::Text)]
    version: String,
}

pub fn init(conn: &mut PgConnection) -> Result<(), PgmqError> {
    conn.transaction::<_, PgmqError, _>(|conn| {
        conn.batch_execute(INIT_SQL)?;
        Ok(())
    })
}

#[doc = include_str!("init_migrations_table.md")]
pub fn init_migrations_table(conn: &mut PgConnection, version: Version) -> Result<(), PgmqError> {
    conn.transaction::<_, PgmqError, _>(|conn| {
        create_migrations_table(conn)?;
        if !fetch_applied(conn)?.is_empty() {
            return Ok(());
        }
        insert_applied(conn, &ParsedScriptName::init_script(version))?;
        Ok(())
    })
}

#[doc = include_str!("installed_version.md")]
pub fn installed_version(conn: &mut PgConnection) -> Result<Option<Version>, PgmqError> {
    conn.transaction::<_, PgmqError, _>(|conn| {
        create_migrations_table(conn)?;
        let applied = fetch_applied(conn)?;
        Ok(applied.into_iter().map(|a| a.version).max())
    })
}

#[cfg(feature = "install-sql-embedded")]
#[doc = include_str!("./embedded/install_sql_embedded.md")]
pub fn install_sql_from_embedded(conn: &mut PgConnection) -> Result<(), PgmqError> {
    let installed_version_opt = conn.transaction::<_, PgmqError, _>(|conn| {
        create_migrations_table(conn)?;
        let applied = fetch_applied(conn)?;
        Ok(max_applied_version(&applied).cloned())
    })?;

    let available = embedded_fetcher().fetch_sync(installed_version_opt.as_ref())?;

    conn.transaction::<_, PgmqError, _>(|conn| {
        create_migrations_table(conn)?;
        let applied = fetch_applied(conn)?;
        let to_apply = filter_unapplied_scripts(available, &applied);
        for script in &to_apply {
            sql_query(script.content.as_ref()).execute(conn)?;
            insert_applied(conn, &script.name)?;
        }
        Ok(())
    })
}

fn create_migrations_table(conn: &mut PgConnection) -> Result<(), PgmqError> {
    conn.batch_execute(SETUP_MIGRATIONS_TABLE_SQL)?;
    Ok(())
}

fn fetch_applied(conn: &mut PgConnection) -> Result<Vec<AppliedMigration>, PgmqError> {
    let rows: Vec<AppliedMigrationRow> =
        sql_query(SELECT_APPLIED_MIGRATIONS_SQL).load(conn)?;
    rows.into_iter()
        .map(|r| {
            Ok(AppliedMigration {
                name: r.name,
                version: Version::from_str(&r.version)?,
            })
        })
        .collect()
}

fn insert_applied(conn: &mut PgConnection, name: &ParsedScriptName) -> Result<(), PgmqError> {
    sql_query(INSERT_APPLIED_MIGRATION_SQL)
        .bind::<sql_types::Text, _>(&name.original)
        .bind::<sql_types::Text, _>(name.to.to_string())
        .execute(conn)?;
    Ok(())
}
