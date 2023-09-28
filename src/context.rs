use std::{
    io::SeekFrom,
    time::{Duration, SystemTime},
};

use rand::{distributions::Alphanumeric, Rng};
use sqlx::{Sqlite, SqlitePool, Transaction};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};
use tracing::{debug, error};

use crate::{
    db::{
        checkpoint::init_shadow_wal_index,
        checksum::Checksum,
        wal::{WalFrameHeader, WalHeader},
    },
    error::Error,
    pos::Pos,
    replica::Replica,
    utils::{
        generation_name_path, get_generation_dir_path, get_generation_root_dir_path, get_wal_path,
        get_wal_segment_path, meta_path, parse_offset, read_wal_entries, read_wal_segment,
        wal_header_exists, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
    },
    wal_segment_iterator::WALSegmentIterator,
};

#[derive(Debug)]
pub struct Context<'read_tx> {
    pub pos: Pos,
    pub wal_header: Option<WalHeader>,
    pub checksum: Checksum,
    pub config: Config,
    pub db: SqlitePool,
    pub read_tx: Mutex<Option<Transaction<'read_tx, Sqlite>>>,
    pub last_wal_frame: Option<Vec<u8>>,
}

impl<'c> Context<'c> {
    pub async fn release_read_lock(&mut self) -> Result<(), Error> {
        let mut read_tx = self.read_tx.lock().await;
        if let Some(read_tx) = read_tx.take() {
            read_tx.rollback().await.map_err(Error::RollbackReadTx)?;
        }
        Ok(())
    }

    pub async fn acquire_read_lock(&mut self) -> Result<(), Error> {
        let mut read_tx = self.read_tx.lock().await;
        if read_tx.is_some() {
            return Ok(());
        }
        let mut new_read_tx = self.db.begin().await.map_err(Error::BeginTx)?;
        sqlx::query("SELECT COUNT(1) FROM _synclite_seq")
            .fetch_all(&mut new_read_tx)
            .await
            .map_err(Error::ReadSequenceTable)?;
        *read_tx = Some(new_read_tx);
        Ok(())
    }

    fn reset(&mut self) {
        self.pos = Pos::default();
        self.wal_header = None;
        self.last_wal_frame = None;
        self.checksum = Checksum::default();
    }

    // clean removes old generations & WAL files.
    pub async fn clean(&self) -> Result<(), Error> {
        self.clean_generations().await
    }

    // cleanGenerations removes old generations.
    async fn clean_generations(&self) -> Result<(), Error> {
        let Some(generation) = self.current_generation().await else {
            // No current generation so probably no generations to clean.
            return Ok(());
        };

        let Ok(mut dir) = fs::read_dir(get_generation_root_dir_path(&self.config.db_path)).await
        else {
            // Probably doesn't exist for some reason
            return Ok(());
        };

        while let Ok(Some(entry)) = dir.next_entry().await {
            let generation_dir = entry.file_name();
            let generation_dir = generation_dir.to_string_lossy();
            if generation_dir == generation {
                continue;
            }

            // Delete all other generations.
            if let Err(err) = fs::remove_dir_all(entry.path()).await {
                // Only log error, but continue try to delete other generation dirs
                error!("Failed to delete generation {generation_dir:?}. Error: `{err:#?}`");
            }
        }

        Ok(())
    }

    // invalidate refreshes cached position, salt, & checksum from on-disk data.
    pub async fn invalidate(&mut self) -> Result<(), Error> {
        // Clear cached data before starting.
        self.reset();

        // Determine the last position of the current generation.
        self.invalidate_pos().await?;

        if self.pos == Pos::default() {
            debug!("init: no wal files available, clearing generation");

            self.clear_generation().await.map_err(|err| {
                error!("Unable to clear generation");
                err
            })?;
            return Ok(()); // no position, exit
        }

        // Determine salt & last checksum.
        self.invalidate_checksum().await.map_err(|err| {
            error!("cannot determine last salt/checksum");
            err
        })?;

        Ok(())
    }

    async fn invalidate_pos(&mut self) -> Result<(), Error> {
        // Determine generation based off "generation" file in meta directory.
        let Some(generation) = self.current_generation().await else {
            return Ok(());
        };

        // Iterate over all segments to find the last one.
        let mut itr = WALSegmentIterator::new(self.config.db_path.clone(), generation).await;

        while let Some(pos) = itr.next().await? {
            self.pos = pos;
        }

        // Exit if no WAL segments exist.
        if self.pos == Pos::default() {
            return Ok(());
        }

        // Read size of last segment to determine ending position.
        let wal_segment_path = get_wal_segment_path(&self.config.db_path, &self.pos);
        let last_segment = fs::metadata(wal_segment_path).await.map_err(|error| {
            Error::ReadWalSegmentMetadata {
                pos: self.pos.clone(),
                error,
            }
        })?;
        let Ok(len) = usize::try_from(last_segment.len()) else {
            return Err(Error::NumberCast("size of last wal segment to usize"));
        };
        self.pos.increment_offset(len);

        Ok(())
    }

    async fn invalidate_checksum(&mut self) -> Result<(), Error> {
        assert_ne!(
            self.pos,
            Pos::default(),
            "position required to invalidate checksum"
        );

        let offsets = self
            .wal_segment_offsets_by_index(self.pos.generation(), self.pos.index())
            .await
            .map_err(|e| {
                error!("cannot read last wal");
                e
            })?;

        // TODO: this should be a function called read_shadow_wal
        // TODO: don't read everything into memory
        let mut wal = Vec::new();

        // TODO: parallell
        for offset in offsets {
            let pos = Pos::new(self.pos.generation().to_string(), self.pos.index(), offset);
            let mut wal_segment = read_wal_segment(&self.config.db_path, &pos).await?;
            wal.append(&mut wal_segment);
        }

        // Read header
        let mut cursor = 0;
        if wal.len() < WAL_HEADER_SIZE {
            error!("missing wal header");
            return Err(Error::MissingWalHeader);
        }
        // TODO: this is very bad
        let hdr = WalHeader::new(&wal)?;
        cursor += WAL_HEADER_SIZE;

        let mut prev_checksum = hdr.checksum();
        loop {
            let len = self.config.page_size + WAL_FRAME_HEADER_SIZE;
            if wal.len() < len + cursor {
                break;
            }
            let raw = &wal[cursor..cursor + WAL_FRAME_HEADER_SIZE];
            let frame = &wal[cursor + WAL_FRAME_HEADER_SIZE
                ..cursor + WAL_FRAME_HEADER_SIZE + self.config.page_size];
            let Ok(frame) = WalFrameHeader::new(raw, frame, &hdr, prev_checksum) else {
                break;
            };
            prev_checksum = frame.checksum();
            self.last_wal_frame = Some(wal.as_slice()[cursor..cursor + len].to_vec());

            cursor += len;
        }

        self.checksum = prev_checksum;
        self.wal_header = Some(hdr);

        Ok(())
    }

    async fn wal_segment_offsets_by_index(
        &self,
        generation: &str,
        index: usize,
    ) -> Result<Vec<usize>, Error> {
        let mut entries =
            read_wal_entries(&self.config.db_path, generation, &Pos::format(index)).await?;

        let mut offsets = vec![];

        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !file_name.ends_with(".wal.data") {
                continue;
            }
            let offset = file_name.replace(".wal.data", "");
            let offset = parse_offset(&offset)?;
            offsets.push(offset);
        }

        // Sort before returning.
        offsets.sort_unstable();

        Ok(offsets)
    }

    async fn current_generation(&self) -> Option<String> {
        let generation_file = generation_name_path(&self.config.db_path);
        fs::read(generation_file)
            .await
            .ok()
            .and_then(|content| String::from_utf8(content).ok())
    }

    async fn clear_generation(&self) -> Result<(), Error> {
        let generation_file = generation_name_path(&self.config.db_path);
        if fs::try_exists(&generation_file)
            .await
            .map_err(Error::ClearGeneration)?
        {
            fs::remove_file(generation_file)
                .await
                .map_err(Error::ClearGeneration)
        } else {
            Ok(())
        }
    }

    // verify_headers_match returns Ok if the primary WAL and last shadow WAL header match.
    async fn verify_headers_match(&self) -> Result<(), Error> {
        // Skip verification if we have no current position.
        if self.pos == Pos::default() {
            return Ok(());
        }

        // Read header from the real WAL file.
        let hdr = fs::read(format!("{}-wal", self.config.db_path))
            .await
            .map_err(|error| Error::ReadWalFile { error })?;
        let hdr = if hdr.len() >= WAL_HEADER_SIZE {
            Some(WalHeader::new(&hdr)?)
        } else {
            None
        };

        // Compare real WAL header with shadow WAL header.
        // If there is a mismatch then the real WAL has been restarted outside Synclite.
        if hdr != self.wal_header {
            error!("Headers did not match");
            return Err(Error::WalHeaderMismatch);
        }

        Ok(())
    }

    // ensure_wal_exists checks that the real WAL exists and has a header.
    pub async fn ensure_wal_exists(&self) -> Result<(), Error> {
        // Exit early if WAL header exists.
        if wal_header_exists(&self.config.db_path).await? {
            return Ok(());
        }

        // Otherwise create transaction that updates the internal synclite table.
        sqlx::query("INSERT INTO _synclite_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1")
            .execute(&self.db)
            .await
            .map_err(Error::WriteSequenceTable)
            .map(|_| ())
    }

    // verify ensures the current shadow WAL state matches where it left off from
    // the real WAL. Returns generation & WAL sync information. If info.reason is
    // not blank, verification failed and a new generation should be started.
    pub async fn verify(&mut self) -> Result<SyncInfo, Error> {
        // Look up existing generation.
        let Some(generation) = self.current_generation().await else {
            return Ok(SyncInfo {
                generation: None,
                restart: false,
                reason: Some("no generation exists".to_string()),
                db_mod_time: None,
            });
        };

        let mut info = SyncInfo {
            generation: Some(generation),
            restart: false,
            reason: None,
            db_mod_time: None,
        };

        // Determine total bytes of real DB for metrics.
        // TODO: make metadata calls parallel
        let db_file = fs::metadata(&self.config.db_path)
            .await
            .map_err(|_| Error::ReadDbFile)?;
        let time = db_file
            .modified()
            .map_err(|err| {
                error!("Unable to determine modification time of db. Error: {err:#?}");
                err
            })
            .ok();
        info.db_mod_time = time;

        // Determine total bytes of real WAL.
        let fi = fs::metadata(get_wal_path(&self.config.db_path))
            .await
            .map_err(|_| Error::ReadWalFileMetadata)?;
        let wal_size =
            usize::try_from(fi.len()).map_err(|_| Error::NumberCast("size of WAL to usize"))?;

        // Verify the index is not out of bounds.
        // if db.pos.Index >= MaxIndex {
        // 	info.reason = "max index exceeded"
        // 	return info, nil
        // }

        // If shadow WAL position is larger than real WAL then the WAL has been
        // truncated so we cannot determine our last state.
        if self.pos.offset() > wal_size {
            info.reason = Some("wal truncated by another process".to_string());
            return Ok(info);
        }

        // Compare WAL headers. Start a new shadow WAL if they are mismatched.
        let mut wal = fs::File::open(get_wal_path(&self.config.db_path))
            .await
            .map_err(|error| {
                error!("Failed to read wal");
                Error::ReadWalFile { error }
            })?;
        let mut raw_hdr = [0; WAL_HEADER_SIZE];
        let count = wal
            .read(&mut raw_hdr)
            .await
            .map_err(|error| Error::ReadWalFile { error })?;
        let hdr = (count == WAL_HEADER_SIZE)
            .then(|| WalHeader::new(&raw_hdr))
            .transpose()?;
        if hdr != self.wal_header {
            info.restart = true;
        }

        // Verify last frame synced still matches.
        if self.pos.offset() > WAL_HEADER_SIZE {
            let offset = self.pos.offset() - self.config.page_size - WAL_FRAME_HEADER_SIZE;
            wal.seek(SeekFrom::Start(offset as u64))
                .await
                .map_err(|error| Error::ReadWalFile { error })?;
            let mut frame = vec![0; self.config.page_size + WAL_FRAME_HEADER_SIZE];
            wal.read(&mut frame)
                .await
                .map_err(|error| Error::ReadWalFile { error })?;
            if let Some(last_wal_frame) = self.last_wal_frame.as_ref() {
                if last_wal_frame != &frame {
                    info.reason = Some("wal overwritten by another process".to_string());
                    return Ok(info);
                }
            }
        }

        Ok(info)
    }

    // create_generation starts a new generation by creating the generation
    // directory, snapshotting to each replica, and updating the current
    // generation name.
    pub async fn create_generation(&mut self, replica: &mut Replica) -> Result<String, Error> {
        // Generate random generation hex name.
        let generation: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        // Generate new directory.
        fs::create_dir_all(get_generation_dir_path(&self.config.db_path, &generation))
            .await
            .map_err(|error| Error::CreateGenerationDirectory {
                generation: generation.clone(),
                error,
            })?;

        // Initialize shadow WAL with copy of header.
        // TODO:
        self.pos.set_generation(generation.clone());
        init_shadow_wal_index(self, replica).await?;
        // if err := db.initShadowWALIndex(ctx, Pos{Generation: generation}); err != nil {
        // 	return "", fmt.Errorf("initialize shadow wal: %w", err)
        // }

        // Atomically write generation name as current generation.
        let generation_name_path = generation_name_path(&self.config.db_path);
        let mut generation_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(generation_name_path)
            .await
            .map_err(|error| Error::WriteCurrentGenerationFile { error })?;
        generation_file
            .write_all(generation.as_bytes())
            .await
            .map_err(|error| Error::WriteCurrentGenerationFile { error })?;
        generation_file
            .sync_all()
            .await
            .map_err(|error| Error::WriteCurrentGenerationFile { error })?;

        // Remove old generations.
        self.clean().await?;

        Ok(generation)
    }
}

pub struct SyncInfo {
    /// generation name
    pub generation: Option<String>,
    /// if true, real WAL header does not match shadow WAL
    pub restart: bool,
    /// if non-blank, reason for sync failure
    pub reason: Option<String>,
    /// last modified date of real DB file
    pub db_mod_time: Option<SystemTime>,
}

#[derive(Debug)]
pub struct Config {
    /// Minimum threshold of WAL size, in pages, before a passive checkpoint.
    /// A passive checkpoint will attempt a checkpoint but fail if there are
    /// active transactions occurring at the same time.
    pub min_checkpoint_page_count: usize,

    /// Maximum threshold of WAL size, in pages, before a forced checkpoint.
    /// A forced checkpoint will block new transactions and wait for existing
    /// transactions to finish before issuing a checkpoint and resetting the WAL.
    //
    /// If zero, no checkpoints are forced. This can cause the WAL to grow
    /// unbounded if there are always read transactions occurring.
    pub max_checkpoint_page_count: usize,

    /// Page size, in bytes
    pub page_size: usize,

    /// Time between automatic checkpoints in the WAL. This is done to allow
    /// more fine-grained WAL files so that restores can be performed with
    /// better precision.
    pub checkpoint_interval: Duration,

    pub db_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_checkpoint_page_count: 100,
            max_checkpoint_page_count: 10000,
            page_size: 1024,
            checkpoint_interval: Duration::from_secs(60),
            db_path: String::default(),
        }
    }
}

pub async fn init<'c>(db: SqlitePool, config: Config) -> Result<Context<'c>, Error> {
    let mut ctx = Context {
        pos: Pos::default(),
        wal_header: None,
        checksum: Checksum::default(),
        config,
        db,
        read_tx: Mutex::new(None),
        last_wal_frame: None,
    };

    // Start a long-running read transaction to prevent other transactions
    // from checkpointing.
    ctx.acquire_read_lock().await?;

    // Ensure meta directory structure exists.
    tokio::fs::create_dir_all(meta_path(&ctx.config.db_path))
        .await
        .map_err(|error| Error::CreateMetaDirectory { error })?;

    // Determine current position, if available.
    ctx.invalidate().await.map_err(|err| {
        error!("Failed to invalidate");
        err
    })?;

    // If we have an existing shadow WAL, ensure the headers match.
    if ctx.verify_headers_match().await.is_err() {
        error!("init: cannot determine last wal position, clearing generation");
        ctx.clear_generation().await.map_err(|err| {
            error!("Failed to clear generation");
            err
        })?;
    }

    // Clean up previous generations.
    ctx.clean().await?;

    Ok(ctx)
}
