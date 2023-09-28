use std::time::SystemTime;

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_sdk_s3 as s3;
use s3::{config::Region, primitives::ByteStream};
use tokio::io::AsyncRead;
use tracing::{debug, error};

use crate::{
    error::Error,
    pos::Pos,
    utils::{parse_index, parse_offset},
};

use super::{ReplicaClient, SnapshotInfo};

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub prefix: String,
}

pub struct S3Replica {
    client: s3::Client,
    bucket: String,
    prefix: String,
}

impl S3Replica {
    pub async fn new(config: Config) -> Result<Self, Error> {
        if config.prefix.starts_with('/') || config.prefix.ends_with('/') {
            return Err(Error::InvalidReplicaPrefix(config.prefix.clone()));
        }

        let credentials = DefaultCredentialsChain::builder().build().await;
        let mut s3_config = s3::config::Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(config.region.clone()))
            .force_path_style(true);
        if let Some(endpoint) = config.endpoint.as_ref() {
            s3_config = s3_config.endpoint_url(endpoint.to_string());
        }

        let client = s3::Client::from_conf(s3_config.build());

        Ok(Self {
            client,
            bucket: config.bucket,
            prefix: config.prefix,
        })
    }

    pub async fn create_bucket(&self) -> Result<(), Error> {
        self.client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| Error::S3(Box::new(err)))
            .map(|_| ())
    }
}

// https://github.com/awslabs/aws-sdk-rust/blob/main/examples/s3/src/bin/get-object.rs
async fn get_object(
    client: &s3::Client,
    bucket: String,
    key: String,
) -> Result<Box<dyn AsyncRead + Unpin + Send + Sync>, Error> {
    let object = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|err| Error::S3(Box::new(err)))?;

    // TODO: maybe stream rather than async read?
    // object.body.into_stream()
    Ok(Box::new(object.body.into_async_read()))
}

async fn put_object(
    client: &s3::Client,
    bucket: String,
    key: String,
    path: String,
) -> Result<(), Error> {
    let body = ByteStream::read_from()
        .path(path)
        .build()
        .await
        .map_err(|err| Error::S3(Box::new(err)))?;

    let request = client.put_object().bucket(&bucket).key(&key).body(body);

    let _out = request
        .send()
        .await
        .map_err(|err| Error::S3(Box::new(err)))?;

    Ok(())
}

#[async_trait::async_trait]
impl ReplicaClient for S3Replica {
    async fn generations(&self) -> Result<Vec<String>, Error> {
        let resp = self
            .client
            .list_objects_v2()
            .prefix(format!("{}/generations/", self.prefix))
            .delimiter("/")
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| Error::S3(Box::new(err)))?;

        Ok(resp
            .common_prefixes()
            .unwrap_or_default()
            .iter()
            .map(|c| {
                c.prefix()
                    .unwrap_or_default()
                    .replace(&self.prefix, "")
                    .replace("/generations/", "")
                    .replace('/', "")
            })
            .collect())
    }

    async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>, Error> {
        debug!("Looking for snapshots for generation: {generation}");
        let resp = self
            .client
            .list_objects_v2()
            .prefix(format!(
                "{}/generations/{generation}/snapshots/",
                self.prefix
            ))
            .delimiter("/")
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| Error::S3(Box::new(err)))?;

        // TODO: check that elements are sorted
        let mut snapshots = resp
            .contents()
            .unwrap_or_default()
            .iter()
            .map(|c| {
                let index_str = std::path::Path::new(c.key().unwrap_or_default())
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .replace(".snapshot.data", "");
                let index = parse_index(&index_str)?;

                Ok(SnapshotInfo {
                    generation: generation.to_string(),
                    index,
                    updated_at: c.last_modified().and_then(|updated_at| {
                        SystemTime::try_from(*updated_at)
                            .map_err(|_| error!("Failed to covert aws datetime to systemtime"))
                            .ok()
                    }),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        snapshots.sort_by_key(|s| s.index);

        Ok(snapshots)
    }

    async fn snapshots_count(&self, generation: &str) -> Result<usize, Error> {
        self.snapshots(generation)
            .await
            .map(|snapshots| snapshots.len())
    }

    async fn write_snapshot(
        &self,
        generation: &str,
        index: &str,
        db_path: &str,
    ) -> Result<SnapshotInfo, Error> {
        debug!("Writing snapshot to generation: {generation}");
        let key = format!(
            "{}/generations/{generation}/snapshots/{index}.snapshot.data",
            self.prefix
        );
        put_object(&self.client, self.bucket.clone(), key, db_path.to_string()).await?;
        Ok(SnapshotInfo {
            generation: generation.to_string(),
            index: parse_index(index)?,
            updated_at: Some(SystemTime::now()),
        })
    }

    async fn write_wal_segment(&self, pos: &Pos, file_path: &str) -> Result<(), Error> {
        let key = format!(
            "{}/generations/{}/wal/{}/{}.wal.data",
            self.prefix,
            pos.generation(),
            pos.index_str(),
            pos.offset_str()
        );
        put_object(
            &self.client,
            self.bucket.clone(),
            key,
            file_path.to_string(),
        )
        .await
    }

    async fn wal_segments(&self, generation: &str) -> Result<Vec<Pos>, Error> {
        let prefix = format!("{}/generations/{generation}/wal/", self.prefix);
        // TODO: check if there is a max number of objects returned from s3
        let resp = self
            .client
            .list_objects_v2()
            .prefix(prefix.clone())
            .delimiter("/")
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| Error::S3(Box::new(err)))?;

        // TODO: segregate by index
        let mut wal_segments = vec![];

        for common_prefix in resp.common_prefixes().unwrap_or_default() {
            let resp = self
                .client
                .list_objects_v2()
                .prefix(common_prefix.prefix().unwrap_or_default())
                .delimiter("/")
                .bucket(&self.bucket)
                .send()
                .await
                .map_err(|err| Error::S3(Box::new(err)))?;

            for obj in resp.contents().unwrap_or_default() {
                let path = obj.key().unwrap_or_default();
                let path = path.replacen(&prefix, "", 1).replace(".wal.data", "");
                let path: Vec<&str> = path.split('/').collect();
                // TODO: Do better checking
                assert_eq!(path.len(), 2);
                let index = parse_index(path[0])?;
                let offset = parse_offset(path[1])?;
                wal_segments.push(Pos::new(generation.to_string(), index, offset));
            }
        }

        wal_segments.sort();

        Ok(wal_segments)
    }

    async fn wal_segment_reader(
        &self,
        pos: &Pos,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send + Sync>, Error> {
        let key = format!(
            "{}/generations/{}/wal/{}/{}.wal.data",
            self.prefix,
            pos.generation(),
            pos.index_str(),
            pos.offset_str()
        );
        get_object(&self.client, self.bucket.clone(), key).await
    }

    async fn snapshot_reader(
        &self,
        generation: &str,
        index: &str,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send + Sync>, Error> {
        let key = format!(
            "{}/generations/{generation}/snapshots/{index}.snapshot.data",
            self.prefix
        );
        get_object(&self.client, self.bucket.clone(), key).await
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn crud() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");

        let config = Config {
            bucket: Uuid::new_v4().to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            region: "eu-west-1".to_string(),
            prefix: "tmp.db".to_string(),
        };
        let s3_client = S3Replica::new(config.clone()).await.unwrap();
        s3_client.create_bucket().await.unwrap();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_file_path = tmp_dir.path().join("tmp.db");
        let tmp_file_path = tmp_file_path.to_str().unwrap_or_default();
        let mut tmp_file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(tmp_file_path)
            .await
            .unwrap();
        let snapshot_content = "hello world";
        tmp_file
            .write_all(snapshot_content.as_bytes())
            .await
            .unwrap();
        tmp_file.sync_all().await.unwrap();

        let generation = "example-generation";
        let index = 123;
        s3_client
            .write_snapshot(generation, &Pos::format(index), tmp_file_path)
            .await
            .unwrap();

        let snapshots = s3_client.snapshots(generation).await.unwrap();

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].generation, generation);
        assert_eq!(snapshots[0].index, index);

        let snapshot_count = s3_client.snapshots_count(generation).await.unwrap();
        assert_eq!(snapshot_count, 1);

        let mut snapshot = s3_client
            .snapshot_reader(generation, &Pos::format(index))
            .await
            .unwrap();
        let mut actual_snapshot_content = String::new();
        snapshot
            .read_to_string(&mut actual_snapshot_content)
            .await
            .unwrap();
        assert_eq!(actual_snapshot_content, snapshot_content);

        let generations = s3_client.generations().await.unwrap();
        assert_eq!(generations, vec![generation.to_string()]);
    }
}
