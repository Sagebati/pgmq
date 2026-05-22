Install PGMQ using the SQL-only approach. This method will perform a fresh installation if PGMQ is not installed, or
it will upgrade PGMQ to the latest version if it was previously installed and there's a newer version available.

If the previous SQL-only installation approach (in crate version <= 0.32.1) was used to install PGMQ,
run the matching `init_migrations_table` function in this module before running this method.

This method uses PGMQ extension installation scripts that are embedded directly in the crate, which allows installing
PGMQ without performing any network requests to external services. However, this approach only allows installing the
latest PGMQ version that's bundled with the crate. If a specific version of PGMQ is required, use
`install_sql_from_github` (in the same module) instead.

Note: This installation method should not be used if PGMQ was installed as an actual Postgres extension using
`CREATE EXTENSION pgmq;`.
