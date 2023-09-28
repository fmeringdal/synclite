use std::time::Duration;

use synclite::{context, db, replica};
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct Row {
    count: u32,
}

// TODO: investigate issue where there is another pool active to
// the recovery DB during recovery process.
#[tokio::test]
async fn recovery() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("RUST_LOG", "synclite=debug,error");
    env_logger::init();

    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("main.db");
    let db_path = db_path
        .to_str()
        .map(|val| val.to_string())
        .unwrap_or_default();

    let (pool, config) = db::init(&db_path, None).await.unwrap();

    let ctx = context::init(pool, config).await.unwrap();

    let s3_replica_config = synclite::replica::s3::Config {
        bucket: Uuid::new_v4().to_string(),
        endpoint: Some("http://localhost:9000".to_string()),
        region: "eu-west-1".to_string(),
        prefix: "tmp.db".to_string(),
    };
    let config = synclite::config::ReplicateConfig {
        db_path: db_path.to_string(),
        replica: synclite::replica::Config::S3(s3_replica_config.clone()),
        encryption_key: None,
    };
    let s3_client = replica::s3::S3Replica::new(s3_replica_config.clone())
        .await
        .unwrap();
    s3_client.create_bucket().await.unwrap();

    let pool = ctx.db.clone();
    sqlx::query("CREATE TABLE IF NOT EXISTS test (a INT);")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO test VALUES(1);")
        .execute(&pool)
        .await
        .unwrap();

    let (stop_tx, stop_rx) = tokio::sync::broadcast::channel(1);

    // Start replication
    tokio::spawn({
        let config = config.clone();
        async move {
            synclite::replicate(config.clone(), stop_rx).await.unwrap();
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    for _ in 0..3 {
        for i in 0..100 {
            sqlx::query("INSERT INTO test VALUES(?);")
                .bind(i)
                .execute(&pool)
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    stop_tx.send(()).unwrap();

    // Restore
    let restore_db_path = tmp_dir.path().join("restore.db");
    let restore_db_path = restore_db_path
        .to_str()
        .map(|val| val.to_string())
        .unwrap_or_default();
    let restore_config = synclite::config::RestoreConfig {
        db_path: restore_db_path.to_string(),
        replica: synclite::replica::Config::S3(s3_replica_config.clone()),
        if_not_exists: true,
        encryption_key: None,
    };
    synclite::restore(restore_config.clone()).await.unwrap();

    let (pool, _config) = db::init(&restore_db_path, None).await.unwrap();
    let count: Row = sqlx::query_as("SELECT count(*) AS count FROM test")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.count, 300 + 1);
}
