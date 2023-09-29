#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::pedantic)]
#![deny(clippy::get_unwrap)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

// TODO: only export stuff that is needed
pub mod config;
pub mod context;
pub mod db;
mod error;
mod pos;
pub mod replica;
mod restore;
mod utils;
mod wal_segment_iterator;

use std::{io::SeekFrom, path::Path, sync::Arc, time::Duration};

use error::Error;
use notify::{RecommendedWatcher, Watcher};
use replica::Replica;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::{broadcast, watch},
};
use tracing::{debug, error, info};

use crate::{
    config::{ReplicateConfig, RestoreConfig},
    context::Context,
    db::{
        checkpoint::{init_shadow_wal_index, CheckpointMode},
        wal::{WalFrameHeader, WalHeader},
    },
    pos::Pos,
    utils::{
        calc_wal_size, get_wal_path, write_wal_segment, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
    },
};

struct FileWatcher {
    pub event_rx: watch::Receiver<()>,
    _watcher: RecommendedWatcher,
}

impl FileWatcher {
    pub fn new(path: &str) -> Result<Self, Error> {
        let (event_tx, event_rx) = watch::channel(());

        let mut watcher =
        notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| match res {
            Ok(_event) => { if let Err(err) = event_tx.send(()) {
                // TODO: handle this better
                error!("Failed to sent file change notification. Error: `{err:#?}`");
            } },
            Err(e) => {
                match &e.kind {
                    notify::ErrorKind::Io(io_error) => {
                        if io_error.raw_os_error() == Some(38) {
                            // Github Issue: https://github.com/notify-rs/notify/issues/423
                            // TODO: fallback to use tokio::interval
                            error!(
                                "Detected known issue where emulated docker is running on MacOS M1. Relevant GitHub issue: https://github.com/notify-rs/notify/issues/423"
                            );
                        }
                    }
                    _ => {
                        error!("watch error: {:?}", e);
                    }
                };
            }
        }).map_err(Error::CreateFileWatcher)?;

        // Add a path to be watched. All files and directories at that path and
        // below will be monitored for changes.
        watcher
            .watch(Path::new(path), notify::RecursiveMode::NonRecursive)
            .map_err(Error::CreateFileWatcher)?;

        Ok(Self {
            event_rx,
            _watcher: watcher,
        })
    }
}

pub async fn replicate(
    config: ReplicateConfig,
    mut stop_rx: broadcast::Receiver<()>,
) -> Result<(), Error> {
    let replica_client = match config.replica {
        replica::Config::S3(config) => Arc::new(replica::s3::S3Replica::new(config).await?),
    };
    let (pool, config) = db::init(&config.db_path, config.encryption_key.clone()).await?;

    let mut replica = Replica {
        pos: Pos::default(),
        wal_itr: None,
        client: replica_client,
        db: pool.clone(),
        db_path: config.db_path.clone(),
    };

    let mut ctx = context::init(pool, config).await?;

    debug!("Calling replicate with: {}", ctx.config.db_path);
    let mut watcher = FileWatcher::new(&get_wal_path(&ctx.config.db_path))?;

    let mut interval = tokio::time::interval(Duration::from_secs(1));

    let retry_count = 5;

    loop {
        let mut count = 0;
        while let Err(err) = sync(&mut ctx, &mut replica).await {
            count += 1;
            error!("Failed to sync with error: `{err:?}`");
            if count == retry_count {
                error!("Failed to sync after {count} retries. Giving up.");
                break;
            }
        }

        tokio::select! {
            biased;
            _ = stop_rx.recv() => {
                info!("Received stop signal. Stopping replication.");
                if let Err(err) = ctx.release_read_lock().await {
                    error!("Failed to release read lock. Error: `{err:#?}`");
                }
                ctx.db.close().await;
                drop(ctx);
                break;
            }
            // Sync to replicas
            _ = interval.tick() => {
                if let Err(err) = replica.sync(&ctx.pos).await {
                    error!("Failed to sync to replica. Error: `{err:#?}`");
                }
            }
            _ = watcher.event_rx.changed() => {
                // TODO: take value from config
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
    }

    Ok(())
}

pub async fn restore(config: RestoreConfig) -> Result<(), Error> {
    if config.if_not_exists {
        let Ok(exists) = tokio::fs::try_exists(&config.db_path).await else {
            error!("Unable to deteremine if db already exists during restore.");
            return Err(Error::DbAlreadyExistsOnRestore);
        };
        if exists {
            info!("DB already exists. Skip restoring.");
        }
    } else {
        let exists = tokio::fs::try_exists(&config.db_path)
            .await
            .unwrap_or(false);
        if exists {
            match tokio::fs::remove_file(&config.db_path).await {
                Ok(_) => {
                    info!("Removed existing DB file");
                }
                Err(err) => {
                    error!("Unable to remove existing DB file. Exiting. Error: `{err:#?}`");
                    return Err(Error::UnableToRemoveExistingDbOnRestore);
                }
            }
        }

        // Try delete any WAL and WAL index file if exists
        let _ = tokio::fs::remove_file(format!("{}-wal", config.db_path)).await;
        let _ = tokio::fs::remove_file(format!("{}-shm", config.db_path)).await;
    }

    let replica = match config.replica {
        replica::Config::S3(config) => Arc::new(replica::s3::S3Replica::new(config).await?),
    };

    crate::restore::restore(
        replica.as_ref(),
        &config.db_path,
        config.encryption_key.clone(),
    )
    .await
}

async fn copy_to_shadow_wal(ctx: &mut Context<'_>, replica: &mut Replica) -> Result<(), Error> {
    let mut pos = ctx.pos.clone();
    assert_ne!(pos, Pos::default(), "zero pos for wal copy");
    let mut new_wal_segment = vec![];

    let old_pos = ctx.pos.clone();
    let mut wal = tokio::fs::File::open(get_wal_path(&ctx.config.db_path))
        .await
        .map_err(|error| Error::ReadWalFile { error })?;
    let n = wal
        .seek(SeekFrom::Start(ctx.pos.offset() as u64))
        .await
        .map_err(|error| Error::ReadWalFile { error })?;
    assert_eq!(n, ctx.pos.offset() as u64);
    if ctx.wal_header.is_none() {
        assert_eq!(ctx.pos.offset(), 0);
        let mut wal_header = [0; WAL_HEADER_SIZE];
        wal.read(&mut wal_header)
            .await
            .map_err(|error| Error::ReadWalFile { error })?;
        assert_ne!(wal_header, [0; WAL_HEADER_SIZE]);
        new_wal_segment.extend_from_slice(wal_header.as_slice());
        pos.increment_offset(WAL_HEADER_SIZE);

        let wal_header = WalHeader::new(&wal_header)?;
        ctx.checksum = wal_header.checksum();

        ctx.wal_header = Some(wal_header);
    }

    let wal_header = ctx.wal_header.as_ref().expect("wal header should be set");

    loop {
        let mut frame_header = [0; WAL_FRAME_HEADER_SIZE];
        let n = wal
            .read(&mut frame_header)
            .await
            .map_err(|error| Error::ReadWalFile { error })?;
        if n == 0 {
            break;
        }
        let mut frame = vec![0; wal_header.page_size() as usize];
        wal.read(&mut frame)
            .await
            .map_err(|error| Error::ReadWalFile { error })?;
        let Ok(wal_frame_header) =
            WalFrameHeader::new(&frame_header, &frame, wal_header, ctx.checksum)
        else {
            // Invalid checksum or salt. Probably a frame from an earlier index that hasn't been truncated
            // during a checkpoint. Just break out of the loop and wait for new frames to overwrite it.
            break;
        };
        new_wal_segment.extend_from_slice(frame_header.as_slice());
        new_wal_segment.extend_from_slice(frame.as_slice());
        ctx.last_wal_frame = Some([frame_header.as_slice(), frame.as_slice()].concat());
        pos.increment_offset(WAL_FRAME_HEADER_SIZE + wal_header.page_size() as usize);

        ctx.checksum = wal_frame_header.checksum();
    }

    if old_pos == pos {
        debug!("Nothing to sync");
        return Ok(());
    }
    debug!("Flushing changes");

    write_wal_segment(&ctx.pos, &new_wal_segment, replica).await?;

    // TODO: also only set ctx.checksum etc here
    ctx.pos = pos.clone();

    Ok(())
}

async fn sync(ctx: &mut Context<'_>, replica: &mut Replica) -> Result<(), Error> {
    if ctx.pos == Pos::default() {
        ctx.invalidate().await?;
    }

    // Ensure WAL has at least one frame in it.
    ctx.ensure_wal_exists().await?;

    // Verify our last sync matches the current state of the WAL.
    // This ensures that we have an existing generation & that the last sync
    // position of the real WAL hasn't been overwritten by another process.
    let mut info = ctx.verify().await?;

    // If we are unable to verify the WAL state then we start a new generation.
    if let Some(reason) = info.reason.as_ref() {
        // TODO: Start new generation & notify user via log message.
        let generation = ctx.create_generation(replica).await.map_err(|err| {
            error!("Failed to create generation");
            err
        })?;
        info!("sync: new generation {}, {}", generation, reason);
        info.generation = Some(generation);

        // Clear shadow wal info.
        info.restart = false;
        info.reason = None;
    }

    // Synchronize real WAL with current shadow WAL.
    copy_to_shadow_wal(ctx, replica).await?;

    // If we are at the end of the WAL file, start a new index.
    if info.restart {
        // Move to beginning of next index.
        ctx.pos.increment_index();

        // TODO:
        // Attempt to restart WAL from beginning of new index.
        // Position is only committed to cache if successful.
        init_shadow_wal_index(ctx, replica).await?;
        // if err := db.initShadowWALIndex(ctx, pos); err != nil {
        // 	return fmt.Errorf("cannot init shadow wal: pos=%s err=%w", pos, err)
        // }
    }

    // If WAL size is great than max threshold, force checkpoint.
    // If WAL size is greater than min threshold, attempt checkpoint.
    let mode = if ctx.config.max_checkpoint_page_count > 0
        && ctx.pos.offset()
            >= calc_wal_size(ctx.config.page_size, ctx.config.max_checkpoint_page_count)
    {
        Some(CheckpointMode::Restart)
    } else if ctx.pos.offset()
        >= calc_wal_size(ctx.config.page_size, ctx.config.min_checkpoint_page_count)
    {
        Some(CheckpointMode::Passive)
    } else if let Some(db_mod_time) = info.db_mod_time {
        let time_since_db_modified = db_mod_time.elapsed().unwrap_or(Duration::ZERO);
        if ctx.config.checkpoint_interval > Duration::ZERO
            && time_since_db_modified > ctx.config.checkpoint_interval
            && ctx.pos.offset() > calc_wal_size(ctx.config.page_size, 1)
        {
            Some(CheckpointMode::Passive)
        } else {
            None
        }
    } else {
        None
    };

    // Issue the checkpoint.
    if let Some(mode) = mode {
        debug!("Checkpointing with mode='{mode}'");
        match crate::db::checkpoint::checkpoint(ctx, mode, replica).await {
            Ok(_) => debug!("Checkpoint complete"),
            // Under rare circumstances, a checkpoint can be unable to verify continuity
            // and will require a restart.
            Err(Error::LastShadowFrameMismatchAfterCheckpoint) => {
                // TODO: this isn't working correctly
                let generation = ctx.create_generation(replica).await.map_err(|err| {
                    error!(
                        "Failed to create a new generation when last shadow frame did not validate"
                    );
                    err
                })?;
                info!("sync: new generation {generation}, possible WAL overrun occurred");
            }
            Err(err) => return Err(err),
        }
    }

    // Clean up any old files.
    ctx.clean().await?;

    Ok(())
}
