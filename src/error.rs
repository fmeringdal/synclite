use crate::pos::Pos;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unexpected magic number in WAL header `{0}`")]
    UnexpectedMagicNumberInHeader(u32),
    #[error("Unable to restore to a database that already exists")]
    DbAlreadyExistsOnRestore,
    #[error("Unable to remove existing database during restore")]
    UnableToRemoveExistingDbOnRestore,
    #[error("Failed to clear generation: `{0}`")]
    ClearGeneration(tokio::io::Error),
    #[error("Failed to read wal entries for generation `{generation}` and index `{index}`. Error: `{error}`")]
    ReadWalEntries {
        generation: String,
        index: String,
        error: tokio::io::Error,
    },
    #[error("Failed to create index directory for generation `{generation}` and index `{index}`. Error: `{error}`")]
    CreateIndexDirectory {
        generation: String,
        index: String,
        error: tokio::io::Error,
    },
    #[error("Failed to write to wal segment for position `{pos:?}`. Error: `{error}`")]
    WriteToWalSegment { pos: Pos, error: tokio::io::Error },
    #[error("Failed to read wal segment for position `{pos:?}`. Error: `{error}`")]
    ReadWalSegment { pos: Pos, error: tokio::io::Error },
    #[error("Failed to read wal segment metadata for position `{pos:?}`. Error: `{error}`")]
    ReadWalSegmentMetadata { pos: Pos, error: tokio::io::Error },
    #[error("Failed to read the wal file. Error: `{error}`")]
    ReadWalFile { error: tokio::io::Error },
    #[error("Failed to read the wal file metadata")]
    ReadWalFileMetadata,
    #[error("Failed to read the DB file")]
    ReadDbFile,
    #[error("No WAL header")]
    MissingWalHeader,
    #[error("Last shadow frame did not match after the checkpoint")]
    LastShadowFrameMismatchAfterCheckpoint,
    #[error(
        "Failed to create generation directory for generation `{generation}`. Error: `{error}`"
    )]
    CreateGenerationDirectory {
        generation: String,
        error: tokio::io::Error,
    },
    #[error("Failed to write current generation to file. Error: `{error}`")]
    WriteCurrentGenerationFile { error: tokio::io::Error },
    #[error("Failed to create meta directory. Error: `{error}`")]
    CreateMetaDirectory { error: tokio::io::Error },
    #[error("Offset `{offset}` is not a valid offset")]
    InvalidOffset { offset: String },
    #[error("Index `{index}` is not a valid index")]
    InvalidIndex { index: String },
    #[error("Failed to create file watcher. Error: `{0}`")]
    CreateFileWatcher(#[source] notify::Error),
    #[error("Failed to restore. Error: `{0}`")]
    Restore(#[from] RestoreError),
    #[error("Found no snapshot in generation `{generation}`")]
    NoSnapshotForGeneration { generation: String },
    #[error("Generation changed during snapshot. From `{generation}` to `{new_generation}`")]
    GenerationChangedDuringSnapshot {
        generation: String,
        new_generation: String,
    },
    #[error("No generation found during a replica sync")]
    NoGeneration,
    #[error("Failed to read wal segment from replica at pos: `{pos:?}`")]
    ReadWalSegmentFromReplica { pos: Pos },
    #[error("Failed to write to temporary wal segment")]
    WriteToTmpWalSegment,
    #[error("Got s3 error: `{0}`")]
    S3(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Replica skipped position. Current replica position: `{current:?}` and new segment is at: `{new_segment:?}`")]
    ReplicaSkippedPosition { current: Pos, new_segment: Pos },
    #[error("Non-contiguous segment in replica. Current replica position: `{current:?}` and new segment is at: `{new_segment:?}`")]
    ReplicaNonContiguousSegment { current: Pos, new_segment: Pos },
    #[error("Received unexpected segment in WAL Segment iterator: `{new_segment:?}`")]
    UnexpcetedSegmentInWalIterator { new_segment: Pos },
    #[error("WAL segment iterator cannot be appended to in error state")]
    WalIteratorAppendWhenErrorState,
    #[error("Encountered invalid salts in frame header")]
    InvalidSaltsInFrameHeader,
    #[error("Encountered invalid checksum in frame header")]
    InvalidChecksumInFrameHeader,
    #[error("Encountered invalid replica prefix")]
    InvalidReplicaPrefix(String),
    #[error("Encountered invalid checksum in WAL header")]
    InvalidChecksumInWalHeader,
    #[error("Wal header has changed")]
    WalHeaderMismatch,
    #[error("Failed to cast number. Error: `{0}`")]
    NumberCast(&'static str),
    #[error("Expected shadow wal index to be called when the current pos has zero offset")]
    InitShadowWalWithoutZeroOffset,
    #[error("Not enough bytes to construct a WAL header")]
    WalHeaderTooShort,
    #[error("Not enough bytes to construct a WAL frame header")]
    WalFrameHeaderTooShort,
    #[error("Failed to rollback read transaction. Error: `{0}`")]
    RollbackReadTx(#[source] sqlx::Error),
    #[error("Failed to rollback write transaction. Error: `{0}`")]
    RollbackWriteTx(#[source] sqlx::Error),
    #[error("Failed to begin a transaction. Error: `{0}`")]
    BeginTx(#[source] sqlx::Error),
    #[error("Failed to read from sequence table. Error: `{0}`")]
    ReadSequenceTable(#[source] sqlx::Error),
    #[error("Failed to insert into sequence table. Error: `{0}`")]
    WriteSequenceTable(#[source] sqlx::Error),
    #[error("Failed to insert into lock table. Error: `{0}`")]
    WriteLockTable(#[source] sqlx::Error),
    #[error("Failed to checkpoint. Error: `{0}`")]
    Checkpoint(#[source] sqlx::Error),
    #[error("Failed to create sequence table. Error: `{0}`")]
    CreateSeqTable(#[source] sqlx::Error),
    #[error("Failed to create lock table. Error: `{0}`")]
    CreateLockTable(#[source] sqlx::Error),
    #[error("Failed to get page size. Error: `{0}`")]
    ReadPageSize(#[source] sqlx::Error),
    #[error("Failed to create connection to DB. Error: `{0}`")]
    DbConnect(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("Found no generation to restore from")]
    NoGeneration,
    #[error("Found no snapshot to restore in generation `{generation}`")]
    NoSnapshotForGeneration { generation: String },
    #[error("Could not determine latest WAL index in generation `{generation}`")]
    LatestWalIndex { generation: String },
    #[error("Could not create DB to restore to. Error: `{0}`")]
    CreateRestoreDb(#[source] tokio::io::Error),
    #[error("Could not create temporary WAL. Error: `{0}`")]
    CreateTemporaryWal(#[source] tokio::io::Error),
    #[error("Could not apply WAL")]
    ApplyWal,
    #[error("Could not truncate WAL. Error: `{0}`")]
    TruncateWal(#[source] sqlx::Error),
}
