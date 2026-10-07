use crate::install::script::ParsedScriptName;
use crate::install::sql::{
    CREATE_MIGRATIONS_TABLE, CREATE_SCHEMA, INSERT_APPLIED_MIGRATION, LOCK_MIGRATIONS_TABLE,
    SELECT_APPLIED_MIGRATIONS,
};
use crate::install::version::Version;
use crate::util::init_lock;
use crate::PgmqError;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{Acquire, FromRow, Postgres, Transaction};

/// Struct to represent a row of the DB table that tracks which migration scripts have been applied.
#[derive(FromRow)]
#[non_exhaustive]
pub struct AppliedMigration {
    /// The name of the migration script.
    pub name: String,
    /// The version of `pgmq` after the migration script was applied. Mainly useful to record
    /// which version was installed by the `pgmq.sql` script for a fresh installation -- this
    /// script does not embed the `pgmq` version in its name, unlike the other migration scripts.
    pub version: Version,
}

impl AppliedMigration {
    /// Create the DB table used to keep track of which migration scripts have been applied.
    pub async fn create_table(txn: &mut Transaction<'static, Postgres>) -> Result<(), PgmqError> {
        init_lock(txn).await?;

        /*
        The `pgmq` schema will not exist yet if we're currently performing a fresh installation
        of `pgmq`, so we first need to make sure the schema exists.
         */
        sqlx::query(CREATE_SCHEMA).execute(&mut **txn).await?;

        sqlx::query(CREATE_MIGRATIONS_TABLE)
            .execute(&mut **txn)
            .await?;

        /*
        The advisory lock above is probably sufficient, but we also lock on the
        `pgmq.__pgmq_migrations` table to be sure that only one transaction can access the
        list of applied migrations at once.
         */
        sqlx::query(LOCK_MIGRATIONS_TABLE)
            .execute(&mut **txn)
            .await?;

        Ok(())
    }

    /// Fetch all of the migrations that were previously applied.
    pub async fn fetch_all(
        tx: &mut Transaction<'static, Postgres>,
    ) -> Result<Vec<AppliedMigration>, PgmqError> {
        let applied_migrations = sqlx::query_as(SELECT_APPLIED_MIGRATIONS)
            .fetch_all(tx.acquire().await?)
            .await?;
        Ok(applied_migrations)
    }

    /// Record that a script with the provided [`ParsedScriptName`] was applied.
    pub fn insert_script(
        name: &'_ ParsedScriptName,
    ) -> Result<Query<'_, Postgres, PgArguments>, PgmqError> {
        let query = sqlx::query(INSERT_APPLIED_MIGRATION)
            .bind(&name.original)
            .bind(&name.to);
        Ok(query)
    }
}
