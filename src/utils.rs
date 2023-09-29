use tokio::{
    fs::{self, ReadDir},
    io::AsyncWriteExt,
};
use tracing::error;

use crate::{error::Error, pos::Pos, replica::Replica};

/// Size of the WAL header in bytes.
pub const WAL_HEADER_SIZE: usize = 32;

// Size of the WAL frame header in bytes.
pub const WAL_FRAME_HEADER_SIZE: usize = 24;

#[must_use]
pub fn meta_path(db_path: &str) -> String {
    format!("{db_path}-synclite")
}

#[must_use]
pub fn generation_name_path(db_path: &str) -> String {
    format!("{}/generation", meta_path(db_path))
}

#[must_use]
pub fn get_generation_root_dir_path(db_path: &str) -> String {
    format!("{}/generations", meta_path(db_path),)
}

#[must_use]
pub fn get_generation_dir_path(db_path: &str, generation: &str) -> String {
    format!("{}/generations/{}", meta_path(db_path), generation)
}

#[must_use]
pub fn get_index_dir_path(db_path: &str, generation: &str, idx: &str) -> String {
    format!(
        "{}/generations/{}/wal/{}",
        meta_path(db_path),
        generation,
        idx
    )
}

#[must_use]
pub fn get_wal_path(db_path: &str) -> String {
    format!("{db_path}-wal")
}

#[must_use]
pub fn get_wal_segment_path(db_path: &str, pos: &Pos) -> String {
    format!(
        "{}/generations/{}/wal/{}/{}.wal.data",
        meta_path(db_path),
        pos.generation(),
        pos.index_str(),
        pos.offset_str()
    )
}

pub async fn read_wal_entries(
    db_path: &str,
    generation: &str,
    idx: &str,
) -> Result<ReadDir, Error> {
    let idx_dir_path = get_index_dir_path(db_path, generation, idx);
    fs::read_dir(idx_dir_path)
        .await
        .map_err(|error| Error::ReadWalEntries {
            generation: generation.to_string(),
            index: idx.to_string(),
            error,
        })
}

// TODO: async io
pub async fn write_wal_segment(
    pos: &Pos,
    bytes: &[u8],
    replica: &mut Replica,
) -> Result<(), Error> {
    ensure_index_dir_exists(&replica.db_path, pos.generation(), &pos.index_str()).await?;

    let wal_segment_path = get_wal_segment_path(&replica.db_path, pos);
    let mut wal_segment = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(wal_segment_path)
        .await
        .map_err(|error| Error::WriteToWalSegment {
            pos: pos.clone(),
            error,
        })?;
    wal_segment
        .write_all(bytes)
        .await
        .map_err(|error| Error::WriteToWalSegment {
            pos: pos.clone(),
            error,
        })?;
    wal_segment
        .sync_all()
        .await
        .map_err(|error| Error::WriteToWalSegment {
            pos: pos.clone(),
            error,
        })?;

    if let Some(replica_wal_itr) = replica.wal_itr.as_mut() {
        if let Err(err) = replica_wal_itr.append(pos.clone()) {
            error!("Failed to append wal segment to wal iterator: `{err:#?}`");
        }
    }
    Ok(())
}

// TODO: async io + compression
pub async fn read_wal_segment(db_path: &str, pos: &Pos) -> Result<Vec<u8>, Error> {
    let path = get_wal_segment_path(db_path, pos);
    tokio::fs::read(path)
        .await
        .map_err(|error| Error::ReadWalSegment {
            pos: pos.clone(),
            error,
        })
}

async fn ensure_index_dir_exists(db_path: &str, generation: &str, idx: &str) -> Result<(), Error> {
    let idx_dir_path = get_index_dir_path(db_path, generation, idx);
    tokio::fs::DirBuilder::new()
        .recursive(true)
        .create(idx_dir_path)
        .await
        .map_err(|error| Error::CreateIndexDirectory {
            generation: generation.to_string(),
            index: idx.to_string(),
            error,
        })
}

pub async fn wal_header_exists(db_path: &str) -> Result<bool, Error> {
    fs::metadata(get_wal_path(db_path))
        .await
        .map_err(|error| Error::ReadWalFile { error })
        .and_then(|meta| {
            usize::try_from(meta.len())
                .map_err(|_| Error::NumberCast("size of WAL to usize"))
                .map(|len| len >= WAL_HEADER_SIZE)
        })
}

pub fn parse_offset(offset: &str) -> Result<usize, Error> {
    offset.parse().map_err(|_| Error::InvalidOffset {
        offset: offset.to_string(),
    })
}

pub fn parse_index(index: &str) -> Result<usize, Error> {
    index.parse().map_err(|_| Error::InvalidIndex {
        index: index.to_string(),
    })
}

// calc_wal_size returns the size of the WAL, in bytes, for a given number of pages.
#[must_use]
pub fn calc_wal_size(page_size: usize, n: usize) -> usize {
    WAL_HEADER_SIZE + ((WAL_FRAME_HEADER_SIZE + page_size) * n)
}
