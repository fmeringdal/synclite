use std::{cmp::Ordering, sync::Arc, time::SystemTime};

use sqlx::SqlitePool;
use tokio::io::AsyncRead;
use tracing::debug;

use crate::{
    db::checkpoint::CheckpointMode, error::Error, pos::Pos, utils::get_wal_segment_path,
    wal_segment_iterator::WALSegmentIterator,
};

pub mod s3;

#[derive(Debug, Clone)]
pub enum Config {
    S3(s3::Config),
}

#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub generation: String,
    pub index: usize,
    pub updated_at: Option<SystemTime>,
}

#[async_trait::async_trait]
pub trait ReplicaClient: Send + Sync {
    /// Returns snapshots for generation
    async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>, Error>;
    /// Returns number of snapshots for generation
    async fn snapshots_count(&self, generation: &str) -> Result<usize, Error>;
    async fn write_snapshot(
        &self,
        generation: &str,
        index: &str,
        db_path: &str,
    ) -> Result<SnapshotInfo, Error>;
    async fn write_wal_segment(&self, pos: &Pos, file_path: &str) -> Result<(), Error>;
    async fn wal_segments(&self, generation: &str) -> Result<Vec<Pos>, Error>;
    async fn wal_segment_reader(
        &self,
        pos: &Pos,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send + Sync>, Error>;
    async fn snapshot_reader(
        &self,
        generation: &str,
        index: &str,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send + Sync>, Error>;
    /// Returns a list of available generations.
    /// Note: not sorted according to last time the generation was active.
    async fn generations(&self) -> Result<Vec<String>, Error>;
}

pub struct Replica {
    pub pos: Pos,
    pub wal_itr: Option<WALSegmentIterator>,
    pub client: Arc<dyn ReplicaClient>,
    pub db: SqlitePool,
    pub db_path: String,
}

impl Replica {
    // Sync copies new WAL frames from the shadow WAL to the replica client.
    pub async fn sync(&mut self, db_pos: &Pos) -> Result<(), Error> {
        // Clear last position if if an error occurs during sync.
        // defer func() {
        // 	if err != nil {
        // 		r.mu.Lock()
        // 		r.pos = Pos{}
        // 		r.mu.Unlock()
        // 	}
        // }()

        // Find current position of database.
        if *db_pos == Pos::default() {
            // TODO: is this actually possible
            return Err(Error::NoGeneration);
        }
        let generation = db_pos.generation();

        // Close out iterator if the generation has changed.
        if matches!(self.wal_itr.as_ref(), Some(itr) if itr.generation() != generation) {
            debug!("Resetting wal itr");
            self.wal_itr = None;
        }

        // Ensure we obtain a WAL iterator before we snapshot so we don't miss any segments.
        let reset_itr = self.wal_itr.is_none();
        if reset_itr {
            self.wal_itr =
                Some(WALSegmentIterator::new(self.db_path.clone(), generation.to_string()).await);
        }

        // Create snapshot if no snapshots exist for generation.
        let snapshot_count = self.client.snapshots_count(generation).await?;
        if snapshot_count == 0 {
            let info = self.snapshot(db_pos).await?;
            if info.generation != generation {
                // TODO: is this actually possible
                return Err(Error::GenerationChangedDuringSnapshot {
                    generation: generation.to_string(),
                    new_generation: info.generation,
                });
            }
        }

        // Determine position, if necessary.
        if reset_itr {
            self.pos = self.calc_pos(generation).await?;
        }

        // Read all WAL files since the last position.
        self.sync_wal().await
    }

    // Snapshot copies the entire database to the replica path.
    async fn snapshot(&self, db_pos: &Pos) -> Result<SnapshotInfo, Error> {
        // Issue a passive checkpoint to flush any pages to disk before snapshotting.
        let mode = CheckpointMode::Passive;
        let (col1, col2, col3): (u32, u32, u32) =
            sqlx::query_as(&format!("PRAGMA wal_checkpoint('{mode}')"))
                .fetch_one(&self.db)
                .await
                .map_err(Error::Checkpoint)?;
        debug!("Checkpointing with mode: '{mode}'. Result: '{col1},{col2},{col3}'");

        // Acquire a read lock on the database during snapshot to prevent checkpoints.
        let mut tx = self.db.begin().await.map_err(Error::BeginTx)?;
        sqlx::query("SELECT COUNT(1) FROM _synclite_seq;")
            .execute(&mut tx)
            .await
            .map_err(Error::ReadSequenceTable)?;

        // Obtain current position.
        if *db_pos == Pos::default() {
            // return info, ErrNoGeneration
        }

        let info = self
            .client
            .write_snapshot(db_pos.generation(), &db_pos.index_str(), &self.db_path)
            .await?;

        Ok(info)
    }

    // calcPos returns the last position for the given generation.
    async fn calc_pos(&self, generation: &str) -> Result<Pos, Error> {
        // Fetch last snapshot. Return error if no snapshots exist.
        let snapshot =
            self.max_snapshot(generation)
                .await?
                .ok_or_else(|| Error::NoSnapshotForGeneration {
                    generation: generation.to_string(),
                })?;

        // Determine last WAL segment available. Use snapshot if none exist.
        let Some(segment) = self.max_wal_segment(generation).await? else {
            return Ok(Pos::new(snapshot.generation, snapshot.index, 0));
        };

        // Read segment to determine size to add to offset.
        let mut last_segment_reader = self.client.wal_segment_reader(&segment).await?;

        let mut sink = tokio::io::sink();
        let n = tokio::io::copy(&mut last_segment_reader, &mut sink)
            .await
            .map_err(|_| Error::ReadWalSegmentFromReplica {
                pos: segment.clone(),
            })?;
        let n = usize::try_from(n)
            .map_err(|_| Error::NumberCast("size of last WAL segment to usize"))?;

        // Return the position at the end of the last WAL segment.
        Ok(Pos::new(
            segment.generation().to_string(),
            segment.index(),
            segment.offset() + n,
        ))
    }

    // maxSnapshot returns the last snapshot in a generation.
    async fn max_snapshot(&self, generation: &str) -> Result<Option<SnapshotInfo>, Error> {
        let snapshots = self.client.snapshots(generation).await?;

        Ok(snapshots.into_iter().max_by_key(|s1| s1.index))
    }

    // maxWALSegment returns the highest WAL segment in a generation.
    async fn max_wal_segment(&self, generation: &str) -> Result<Option<Pos>, Error> {
        let segments = self.client.wal_segments(generation).await?;

        Ok(segments
            .into_iter()
            .max_by(|s1, s2| match s1.index().cmp(&s2.index()) {
                Ordering::Equal => s1.offset().cmp(&s2.offset()),
                ord => ord,
            }))
    }

    async fn sync_wal(&mut self) -> Result<(), Error> {
        // Group segments by index.
        let mut segments: Vec<Vec<Pos>> = vec![];

        let Some(wal_itr) = self.wal_itr.as_mut() else {
            return Ok(());
        };

        while let Some(info) = wal_itr.next().await? {
            if self.pos > info {
                continue; // already processed, skip
            }

            // Start a new chunk if index has changed.
            match segments.last_mut() {
                Some(last_segments) => {
                    if last_segments
                        .first()
                        .map_or(false, |segment| segment.index() != info.index())
                    {
                        segments.push(vec![info]);
                    } else {
                        last_segments.push(info);
                    }
                }
                None => {
                    segments.push(vec![info]);
                }
            }
        }

        // Write out segments to replica by index so they can be combined.
        for index_segments in segments {
            self.write_index_segments(index_segments).await?;
        }

        Ok(())
    }

    async fn write_index_segments(&mut self, segments: Vec<Pos>) -> Result<(), Error> {
        // First segment position must be equal to last replica position or
        // the start of the next index.
        let first_segment = segments.first().expect("segments required for replication");
        if &self.pos != first_segment {
            let mut next_idx_pos = self.pos.clone();
            next_idx_pos.increment_index();
            if &next_idx_pos != first_segment {
                return Err(Error::ReplicaSkippedPosition {
                    current: self.pos.clone(),
                    new_segment: first_segment.clone(),
                })?;
            }
        }

        let mut pos = first_segment.clone();
        let initial_pos = pos.clone();

        let tmp_dir = tempfile::tempdir().map_err(|_| Error::WriteToTmpWalSegment)?;
        let tmp_file_path = tmp_dir.path().join("tmp.wal");
        let mut tmp_file = tokio::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(tmp_file_path.clone())
            .await
            .map_err(|_| Error::WriteToTmpWalSegment)?;
        // // Copy shadow WAL to client write via io.Pipe().
        // pr, pw := io.Pipe()
        // defer func() { _ = pw.CloseWithError(err) }()

        // // Copy through pipe into client from the starting position.
        // var g errgroup.Group
        // g.Go(func() error {
        //     _, err := r.client.WriteWALSegment(ctx, initialPos, pr)
        //     return err
        // })

        // // Wrap writer to LZ4 compress.
        // zw := lz4.NewWriter(pw)

        for segment in segments {
            // Ensure segments are in order and no bytes are skipped.
            if pos != segment {
                return Err(Error::ReplicaNonContiguousSegment {
                    current: pos.clone(),
                    new_segment: segment.clone(),
                })?;
            }

            let wal_segment_path = get_wal_segment_path(&self.db_path, &pos);
            // TODO: set create new option to true?
            let mut wal_segment = tokio::fs::OpenOptions::new()
                .write(false)
                .read(true)
                .open(wal_segment_path)
                .await
                .map_err(|_| Error::WriteToTmpWalSegment)?;
            let n = tokio::io::copy(&mut wal_segment, &mut tmp_file)
                .await
                .map_err(|_| Error::WriteToTmpWalSegment)?;
            let n = usize::try_from(n)
                .map_err(|_| Error::NumberCast("size of last WAL segment to usize"))?;

            wal_segment
                .sync_all()
                .await
                .map_err(|_| Error::WriteToTmpWalSegment)?;

            // Track last position written.
            pos.increment_offset(n);
        }

        // // Write each segment out to the replica.
        // for i := range segments {
        //     info := &segments[i]

        //     if err := func() error {
        //         // Ensure segments are in order and no bytes are skipped.
        //         if pos != info.Pos() {
        //             return fmt.Errorf("non-contiguous segment: expected=%s current=%s", pos, info.Pos())
        //         }

        //         rc, err := r.db.WALSegmentReader(ctx, info.Pos())
        //         if err != nil {
        //             return err
        //         }
        //         defer rc.Close()

        //         n, err := io.Copy(zw, lz4.NewReader(rc))
        //         if err != nil {
        //             return err
        //         } else if err := rc.Close(); err != nil {
        //             return err
        //         }

        //         // Track last position written.
        //         pos = info.Pos()
        //         pos.Offset += n

        //         return nil
        //     }(); err != nil {
        //         return fmt.Errorf("wal segment: pos=%s err=%w", info.Pos(), err)
        //     }
        // }

        // // Flush LZ4 writer, close pipe, and wait for write to finish.
        // if err := zw.Close(); err != nil {
        //     return fmt.Errorf("lz4 writer close: %w", err)
        // } else if err := pw.Close(); err != nil {
        //     return fmt.Errorf("pipe writer close: %w", err)
        // } else if err := g.Wait(); err != nil {
        //     return err
        // }

        tmp_file
            .sync_all()
            .await
            .map_err(|_| Error::WriteToTmpWalSegment)?;
        self.client
            .write_wal_segment(
                &initial_pos,
                tmp_file_path.as_path().to_str().expect("never None"),
            )
            .await?;

        // Save last replicated position.
        self.pos = pos;

        Ok(())
    }
}
