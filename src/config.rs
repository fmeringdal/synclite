use crate::replica::Config as ReplicaConfig;

#[derive(Debug, Clone)]
pub struct ReplicateConfig {
    pub db_path: String,
    pub replica: ReplicaConfig,
    pub encryption_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RestoreConfig {
    pub db_path: String,
    pub replica: ReplicaConfig,
    pub if_not_exists: bool,
    pub encryption_key: Option<String>,
}
