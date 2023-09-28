use tracing::error;

use crate::{
    error::Error,
    pos::Pos,
    utils::{meta_path, parse_index, parse_offset, read_wal_entries},
};

#[derive(Debug)]
pub struct WALSegmentIterator {
    generation: String,
    indexes: Vec<String>,
    infos: Vec<Pos>,
    error: bool,
    db_path: String,
}

impl WALSegmentIterator {
    pub async fn new(db_path: String, generation: String) -> Self {
        let generation_dir_path = format!("{}/generations/{}/wal", meta_path(&db_path), generation);
        let mut indexes = vec![];

        if let Ok(mut dir) = tokio::fs::read_dir(generation_dir_path).await {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let file_name = entry.file_name();
                let file_name = file_name.to_string_lossy().to_string();
                indexes.push(file_name);
            }
        }
        indexes.sort();

        Self {
            generation,
            indexes,
            infos: Vec::new(),
            error: false,
            db_path,
        }
    }

    pub fn append(&mut self, pos: Pos) -> Result<(), Error> {
        if self.error {
            error!("was in error");
            return Err(Error::WalIteratorAppendWhenErrorState);
        }
        if self.generation != pos.generation() {
            self.error = true;
            error!("Encountered unexpected generation");
            return Err(Error::WalIteratorAppendWhenErrorState);
        }

        // If the info has an index that is still waiting to be read from disk into
        // the cache then simply append it to the end of the indices.
        //
        // If we have no pending indices, then append to the end of the infos. If
        // we don't have either then just append to the infos and avoid validation.
        if let Some(max_idx) = self.indexes.last() {
            let Ok(max_idx) = parse_index(max_idx) else {
                error!("Encountered invalid index in wal segment iterator: {max_idx}");
                self.error = true;
                return Err(Error::WalIteratorAppendWhenErrorState);
            };

            if pos.index() < max_idx || pos.index() > max_idx + 1 {
                error!("bad max idx");
                self.error = true;
                return Err(Error::UnexpcetedSegmentInWalIterator { new_segment: pos });
            } else if pos.index() == max_idx + 1 {
                self.indexes.push(pos.index_str());
            }
        // NOTE: no-op if segment index matches the current last index
        } else if let Some(last_info) = self.infos.last() {
            if pos.index() < last_info.index() || pos.index() > last_info.index() + 1 {
                self.error = true;
                return Err(Error::UnexpcetedSegmentInWalIterator { new_segment: pos });
            } else if pos.index() == last_info.index() + 1 {
                self.indexes.push(pos.index_str());
            } else {
                // If the index matches the current infos, verify its offset and append.
                if pos.offset() <= last_info.offset() {
                    error!("bad offset");
                    self.error = true;
                    return Err(Error::UnexpcetedSegmentInWalIterator { new_segment: pos });
                }
                self.infos.push(pos);
            }
        } else {
            self.infos.push(pos);
        }

        Ok(())
    }

    pub fn has_error(&self) -> bool {
        self.error
    }

    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub async fn next(&mut self) -> Result<Option<Pos>, Error> {
        if self.error {
            return Ok(None);
        }

        if !self.infos.is_empty() {
            return Ok(Some(self.infos.remove(0)));
        }

        if self.indexes.is_empty() {
            return Ok(None);
        };
        let next_idx = self.indexes.remove(0);
        let mut wal_entries = read_wal_entries(&self.db_path, &self.generation, &next_idx).await?;
        while let Ok(Some(entry)) = wal_entries.next_entry().await {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !file_name.ends_with(".wal.data") {
                continue;
            }
            let offset = file_name.replace(".wal.data", "");
            self.infos.push(Pos::new(
                self.generation.clone(),
                parse_index(&next_idx)?,
                parse_offset(&offset)?,
            ));
        }
        self.infos.sort_by_key(Pos::offset);

        if !self.infos.is_empty() {
            return Ok(Some(self.infos.remove(0)));
        }

        Ok(None)
    }
}
