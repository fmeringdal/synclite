# Synclite

Synclite is a SQLite streaming replication Rust library inspired by [Litestream](https://github.com/benbjohnson/litestream).

**CAUTION** This library is currently in an experimental stage and contains known bugs, making it unsuitable for production use at this time.

## Get started

Synclite is a library that is meant to be used within your application.

Start by setting up minio to store the replicated data

```sh
docker run -p 9000:9000 -p 9001:9001 minio/minio:latest server /data --console-address ":9001"
```

Create a SQLite DB at path `tmp.db` and insert some dummy data

```sh
sqlite3 tmp.db
> CREATE TABLE IF NOT EXISTS test (a INT);
> INSERT INTO test VALUES(1);
> INSERT INTO test VALUES(2);
> INSERT INTO test VALUES(3);
```

Add `synclite` to your `Cargo.toml`
```toml
[dependencies]
synclite = "0.2.2"
```

Start replicating

```rust
std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");

let s3_replica_config = synclite::replica::s3::Config {
    bucket: "mybkt".to_string(),
    endpoint: Some("http://localhost:9000".to_string()),
    region: "eu-west-1".to_string(),
    prefix: "tmp.db".to_string(),
};
let config = synclite::config::ReplicateConfig {
    db_path: "tmp.db".to_string(),
    replica: synclite::replica::Config::S3(s3_replica_config.clone()),
    // This should be set if you use SQLCipher
    encryption_key: None,
};


let (stop_tx, stop_rx) = tokio::sync::broadcast::channel(1);

// Start replication in a background task
tokio::spawn({
    let config = config.clone();
    async move {
        synclite::replicate(config.clone(), stop_rx).await.unwrap();
    }
});
```

Add some more dummy data while replication is runnning
```sh
sqlite3 tmp.db
> INSERT INTO test VALUES(4);
> INSERT INTO test VALUES(5);
sleep 2 # give some time for replication to run
```

Then restore the replicated data to `restored.db`

```rust
std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");

let s3_replica_config = synclite::replica::s3::Config {
    bucket: "mybkt".to_string(),
    endpoint: Some("http://localhost:9000".to_string()),
    region: "eu-west-1".to_string(),
    prefix: "tmp.db".to_string(),
};
let restore_config = synclite::config::RestoreConfig {
    db_path: "restored.db".to_string(),
    replica: synclite::replica::Config::S3(s3_replica_config.clone()),
    if_not_exists: true,
    encryption_key: None,
};
synclite::restore(restore_config.clone()).await.unwrap();
```

Inspected the restored db
```sh
sqlite3 restored.db
> SELECT * FROM test;
1
2
3
4
5
```


## Upcoming features
- Compression
- Routine snapshots
- Retention period for snapshots
- Better documentation and more examples
- Support more replication backends
- CLI