use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};

use crate::{context::Config, error::Error};

pub mod checkpoint;
pub mod checksum;
pub mod wal;

pub async fn init(
    db_path: &str,
    encryption_key: Option<String>,
) -> Result<(SqlitePool, Config), Error> {
    let pool = create_db_pool(db_path, encryption_key).await?;
    run_migrations(&pool).await?;
    let mut config = db_config(&pool).await?;
    config.db_path = db_path.to_string();

    Ok((pool, config))
}

pub async fn create_db_pool(
    db_path: &str,
    encryption_key: Option<String>,
) -> Result<SqlitePool, Error> {
    let mut conn_opts = SqliteConnectOptions::new()
        .pragma("journal_mode", "wal")
        .pragma("wal_autocheckpoint", "0")
        .pragma("busy_timeout", "5000")
        // Sync the WAL on every commit frame.
        .pragma("synchronous", "FULL")
        .create_if_missing(true)
        .filename(db_path);
    if let Some(key) = encryption_key {
        conn_opts = conn_opts.pragma("key", key);
    }

    SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(5)
        .test_before_acquire(true)
        .connect_with(conn_opts)
        .await
        .map_err(Error::DbConnect)
}

async fn run_migrations(pool: &SqlitePool) -> Result<(), Error> {
    // Create a table to force writes to the WAL when empty.
    // There should only ever be one row with id=1.
    sqlx::query("CREATE TABLE IF NOT EXISTS _synclite_seq (id INTEGER PRIMARY KEY, seq INTEGER);")
        .execute(pool)
        .await
        .map_err(Error::CreateSeqTable)?;

    // Create a lock table to force write locks during sync.
    // The sync write transaction always rolls back so no data should be in this table.
    sqlx::query("CREATE TABLE IF NOT EXISTS _synclite_lock (id INTEGER);")
        .execute(pool)
        .await
        .map_err(Error::CreateLockTable)?;

    Ok(())
}

async fn db_config(pool: &SqlitePool) -> Result<Config, Error> {
    let mut config = Config::default();

    // Page size
    let res = sqlx::query("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .map_err(Error::ReadPageSize)?;
    let page_size: u32 = res.try_get(0).map_err(Error::ReadPageSize)?;
    assert!(page_size % 8 == 0);
    config.page_size = page_size as usize;

    Ok(config)
}
