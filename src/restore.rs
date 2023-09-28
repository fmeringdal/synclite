use std::cmp::Ordering;

use tracing::{debug, error, info};

use crate::{
    db::create_db_pool,
    error::{Error, RestoreError},
    pos::Pos,
    replica::ReplicaClient,
};

pub async fn restore(
    client: &impl ReplicaClient,
    db_output: &str,
    encryption_key: Option<String>,
) -> Result<(), Error> {
    info!("Restoring");
    let generation = find_latest_generation(client)
        .await?
        .ok_or(RestoreError::NoGeneration)?;
    info!("Latest generation to restore '{generation}'");
    let snapshot_idx = find_max_snapshot_for_generation(client, &generation)
        .await?
        .ok_or_else(|| RestoreError::NoSnapshotForGeneration {
            generation: generation.clone(),
        })?;
    let wal_idx = find_latest_wal_index(client, &generation)
        .await?
        .ok_or_else(|| RestoreError::LatestWalIndex {
            generation: generation.clone(),
        })?;
    assert!(snapshot_idx <= wal_idx, "TODO");

    // Download snapshat to db output path
    let mut snapshot_reader = client
        .snapshot_reader(&generation, &Pos::format(snapshot_idx))
        .await?;
    let mut db = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(db_output)
        .await
        .map_err(RestoreError::CreateRestoreDb)?;
    tokio::io::copy(snapshot_reader.as_mut(), &mut db)
        .await
        .map_err(RestoreError::CreateRestoreDb)?;
    db.sync_all().await.map_err(RestoreError::CreateRestoreDb)?;
    drop(db);

    // Download all wal files from snapshot_idx..=wal_idx and apply the wal segments to the db
    let wal_segments = client.wal_segments(&generation).await?;

    let mut wal_segment_indexes: Vec<Vec<Pos>> = vec![];

    for wal_segment in wal_segments {
        if wal_segment.index() < snapshot_idx {
            continue;
        }
        if wal_segment.index() > wal_idx {
            break;
        }
        if let Some(last_wal_segment) = wal_segment_indexes.last_mut() {
            if last_wal_segment
                .last()
                .expect("always a wal segment in index")
                .index()
                < wal_segment.index()
            {
                wal_segment_indexes.push(vec![wal_segment]);
            } else {
                last_wal_segment.push(wal_segment);
            }
        } else {
            wal_segment_indexes.push(vec![wal_segment]);
        };
    }

    for index in wal_segment_indexes {
        let wal_file_name = format!("{db_output}-wal");
        let wal_idx_file_name = format!("{db_output}-shm");
        let tmp_wal_file_name = format!("{wal_file_name}-{}-tmp", index[0].index_str());
        let mut wal_file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_wal_file_name)
            .await
            .map_err(RestoreError::CreateTemporaryWal)?;

        if tokio::fs::remove_file(&wal_idx_file_name).await.is_ok() {
            info!("Removed wal index file");
        } else {
            info!("Failed to remove wal index file");
        }

        for wal_segment in index.clone() {
            let mut wal = client.wal_segment_reader(&wal_segment).await?;
            tokio::io::copy(wal.as_mut(), &mut wal_file)
                .await
                .map_err(RestoreError::CreateTemporaryWal)?;
        }
        wal_file
            .sync_all()
            .await
            .map_err(RestoreError::CreateTemporaryWal)?;
        tokio::fs::rename(tmp_wal_file_name, &wal_file_name)
            .await
            .map_err(RestoreError::CreateTemporaryWal)?;

        // Apply wal to db
        let pool = create_db_pool(db_output, encryption_key.clone()).await?;
        let (col1, col2, col3): (u32, u32, u32) =
            sqlx::query_as("PRAGMA wal_checkpoint('TRUNCATE');")
                .fetch_one(&pool)
                .await
                .map_err(RestoreError::TruncateWal)?;
        if col1 != 0 {
            error!("truncation checkpoint failed during restore ({col1},{col2},{col3})");
            return Err(RestoreError::ApplyWal)?;
        }
        debug!("checkpoint result({col1},{col2},{col3})");
        // TODO: this is needed when doing manual restore. Try to
        // write a test that fails without this.
        pool.close().await;
    }

    Ok(())
}

async fn find_max_snapshot_for_generation(
    client: &dyn ReplicaClient,
    generation: &str,
) -> Result<Option<usize>, Error> {
    client
        .snapshots(generation)
        .await
        .map(|snapshots| snapshots.into_iter().last().map(|snapshot| snapshot.index))
}

async fn find_latest_generation(client: &dyn ReplicaClient) -> Result<Option<String>, Error> {
    let generations = client.generations().await?;
    let mut futures = Vec::with_capacity(generations.len());

    for generation in &generations {
        futures.push(async {
            let snapshots = client.snapshots(generation).await?;
            let first_snapshot_time = snapshots.first().and_then(|s| s.updated_at);
            Ok((generation.clone(), first_snapshot_time))
        });
    }

    futures::future::try_join_all(futures).await.map(|results| {
        results
            .into_iter()
            .max_by(|s1, s2| match (s1.1, s2.1) {
                (Some(s1), Some(s2)) => s1.cmp(&s2),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            })
            .map(|(latest_generation, _)| latest_generation)
    })
}

async fn find_latest_wal_index(
    client: &dyn ReplicaClient,
    generation: &str,
) -> Result<Option<usize>, Error> {
    client
        .wal_segments(generation)
        .await
        .map(|segments| segments.last().map(Pos::index))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::replica::s3::S3Replica;

    use super::*;

    #[tokio::test]
    async fn test_find_latest_generation() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");

        let config = crate::replica::s3::Config {
            bucket: Uuid::new_v4().to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            region: "eu-west-1".to_string(),
            prefix: "tmp.db".to_string(),
        };
        let s3_client = S3Replica::new(config.clone()).await.unwrap();
        s3_client.create_bucket().await.unwrap();

        let generations = vec!["7", "3", "2", "1", "101241", "8", "5"];
        for generation in generations {
            let tmp_dir = tempfile::tempdir().unwrap();
            let tmp_file_path = tmp_dir.path().join("tmp.db");
            let tmp_file_path = tmp_file_path.to_str().unwrap_or_default();
            tokio::fs::write(tmp_file_path, "foobar".as_bytes())
                .await
                .unwrap();

            let index = 123;
            s3_client
                .write_snapshot(generation, &Pos::format(index), tmp_file_path)
                .await
                .unwrap();
        }

        let latest_generation = find_latest_generation(&s3_client).await.unwrap();
        assert_eq!(latest_generation, Some("5".to_string()));
    }
}
