use std::io::SeekFrom;

use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{debug, error};

use crate::{
    context::Context,
    copy_to_shadow_wal,
    db::{checksum::Checksum, wal::WalHeader},
    error::Error,
    replica::Replica,
    utils::{get_wal_path, write_wal_segment, WAL_HEADER_SIZE},
};

/// Perform a checkpoint on the WAL file and initializes a new shadow WAL file.
pub async fn checkpoint(
    ctx: &mut Context<'_>,
    mode: CheckpointMode,
    replica: &mut Replica,
) -> Result<(), Error> {
    // Read WAL header before checkpoint to check if it has been restarted.
    let hdr = read_wal_header(&get_wal_path(&ctx.config.db_path)).await?;

    // Copy shadow WAL before checkpoint to copy as much as possible.
    copy_to_shadow_wal(ctx, replica).await?;

    // Execute checkpoint and immediately issue a write to the WAL to ensure
    // a new page is written.
    exec_checkpoint(ctx, mode).await?;
    sqlx::query("INSERT INTO _synclite_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1").execute(&ctx.db).await.map_err(Error::WriteSequenceTable)?;

    // If WAL hasn't been restarted, exit.
    let other = read_wal_header(&get_wal_path(&ctx.config.db_path)).await?;
    if hdr == other {
        debug!("Wal hasn't been restarted. Exiting ...");
        return Ok(());
    }

    // Start a transaction. This will be promoted immediately after.
    let mut tx = ctx.db.begin().await.map_err(Error::BeginTx)?;

    // Insert into the lock table to promote to a write tx. The lock table
    // insert will never actually occur because our tx will be rolled back,
    // however, it will ensure our tx grabs the write lock. Unfortunately,
    // we can't call "BEGIN IMMEDIATE" as we are already in a transaction.
    sqlx::query("INSERT INTO _synclite_lock (id) VALUES (1)")
        .execute(&mut tx)
        .await
        .map_err(Error::WriteLockTable)?;

    // Verify we can re-read the last frame copied to the shadow WAL.
    // This ensures that another transaction has not overrun the WAL past where
    // our previous copy was which would overwrite any additional unread
    // frames between the checkpoint & the new write lock.
    //
    // This only occurs with high load and a short sync frequency so it is rare.
    if !verify_last_shadow_frame(ctx).await? {
        error!("cannot verify last frame copied from shadow wal");
        return Err(Error::LastShadowFrameMismatchAfterCheckpoint);
    }

    // Copy the end of the previous WAL before starting a new shadow WAL.
    copy_to_shadow_wal(ctx, replica).await?;

    // Start a new shadow WAL file with next index.
    ctx.pos.increment_index();
    init_shadow_wal_index(ctx, replica).await?;

    // Release write lock before checkpointing & exiting.
    tx.rollback().await.map_err(Error::RollbackWriteTx)?;

    Ok(())
}

async fn read_wal_header(wal_path: &str) -> Result<[u8; WAL_HEADER_SIZE], Error> {
    let mut wal = tokio::fs::File::open(wal_path)
        .await
        .map_err(|error| Error::ReadWalFile { error })?;
    let mut wal_header = [0; WAL_HEADER_SIZE];
    wal.read(&mut wal_header)
        .await
        .map_err(|error| Error::ReadWalFile { error })?;

    Ok(wal_header)
}

async fn exec_checkpoint(ctx: &mut Context<'_>, mode: CheckpointMode) -> Result<(), Error> {
    // Ensure the read lock has been removed before issuing a checkpoint.
    // We defer the re-acquire to ensure it occurs even on an early return.
    ctx.release_read_lock().await?;
    // A non-forced checkpoint is issued as "PASSIVE". This will only checkpoint
    // if there are not pending transactions. A forced checkpoint ("RESTART")
    // will wait for pending transactions to end & block new transactions before
    // forcing the checkpoint and restarting the WAL.
    //
    // See: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
    let (col1, col2, col3): (u32, u32, u32) =
        sqlx::query_as(&format!("PRAGMA wal_checkpoint('{mode}')"))
            .fetch_one(&ctx.db)
            .await
            .map_err(Error::Checkpoint)?;
    debug!("Checkpointing with mode: '{mode}'. Result: '{col1},{col2},{col3}'");

    // Clear last read frame if we are truncating.
    // TODO: shouldn't this be the case for restarts as well?
    if mode == CheckpointMode::Truncate {
        ctx.last_wal_frame = None;
        ctx.checksum = Checksum::default();
        ctx.wal_header = None;
    }

    // Reacquire the read lock immediately after the checkpoint.
    ctx.acquire_read_lock().await
}

pub async fn init_shadow_wal_index(
    ctx: &mut Context<'_>,
    replica: &mut Replica,
) -> Result<(), Error> {
    if ctx.pos.offset() != 0 {
        return Err(Error::InitShadowWalWithoutZeroOffset);
    }

    let mut new_wal_segment = vec![];

    let hdr = read_wal_header(&get_wal_path(&ctx.config.db_path)).await?;
    new_wal_segment.extend_from_slice(hdr.as_slice());
    let wal_header = WalHeader::new(&hdr)?;
    ctx.checksum = wal_header.checksum();
    ctx.wal_header = Some(wal_header);

    write_wal_segment(&ctx.pos, &new_wal_segment, replica).await?;
    ctx.pos.increment_offset(WAL_HEADER_SIZE);

    // Copy as much shadow WAL as available.
    copy_to_shadow_wal(ctx, replica).await
}

/// Re-reads the last frame read during the shadow copy.
/// This ensures that the frame has not been overrun after a checkpoint occurs
/// but before the new write lock has been obtained to initialize the new wal index.
async fn verify_last_shadow_frame(ctx: &Context<'_>) -> Result<bool, Error> {
    let Some(last_wal_frame) = ctx.last_wal_frame.as_ref() else {
        return Ok(true);
    };
    let mut last_wal_frame_2 = vec![0; last_wal_frame.len()];

    let mut wal = tokio::fs::File::open(get_wal_path(&ctx.config.db_path))
        .await
        .map_err(|error| Error::ReadWalFile { error })?;
    wal.seek(SeekFrom::Start(
        (ctx.pos.offset() - last_wal_frame.len()) as u64,
    ))
    .await
    .map_err(|error| Error::ReadWalFile { error })?;
    wal.read(&mut last_wal_frame_2)
        .await
        .map_err(|error| Error::ReadWalFile { error })?;

    Ok(*last_wal_frame == last_wal_frame_2)
}

/// `SQLite` checkpoint modes.
#[derive(Debug, strum_macros::Display, PartialEq, Eq)]
pub enum CheckpointMode {
    #[strum(serialize = "PASSIVE")]
    Passive,
    #[strum(serialize = "FULL")]
    Full,
    #[strum(serialize = "RESTART")]
    Restart,
    #[strum(serialize = "TRUNCATE")]
    Truncate,
}
