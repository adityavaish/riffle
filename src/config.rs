//! Configuration & storage-backend abstraction.
//!
//! Riffle reads its config from CLI flags and environment variables.
//! Storage backend is auto-detected from the table URI scheme:
//!
//! - `./...`, `/abs/path`, `file://...` → local filesystem
//! - `abfss://...`, `az://...`          → Azure Data Lake Storage Gen2
//! - `s3://...`                         → AWS S3
//! - `gs://...`                         → Google Cloud Storage

use anyhow::{bail, Result};
use clap::Parser;
use std::collections::HashMap;

/// Adaptive Delta Lake CDC streaming consumer with a tunable web dashboard.
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// Delta table URI. Local path or supported cloud URI.
    /// Examples:
    ///   ./data/my-table.delta
    ///   abfss://container@account.dfs.core.windows.net/path/to/table
    ///   s3://bucket/path/to/table
    ///   gs://bucket/path/to/table
    #[arg(long, env = "RIFFLE_TABLE_URI", default_value = "./data/riffle-demo.delta")]
    pub table_uri: String,

    /// Address the dashboard HTTP server binds to.
    #[arg(long, env = "RIFFLE_BIND_ADDR", default_value = "0.0.0.0:3000")]
    pub bind_addr: String,

    /// Enable the demo data producer. Disable if you write into the table from another process.
    #[arg(long, env = "RIFFLE_PRODUCER_ENABLED", default_value_t = true)]
    pub producer_enabled: bool,

    /// Enable the CDC consumer.
    #[arg(long, env = "RIFFLE_CONSUMER_ENABLED", default_value_t = true)]
    pub consumer_enabled: bool,

    /// Producer: minimum rows per write batch.
    #[arg(long, env = "RIFFLE_BATCH_MIN_ROWS", default_value_t = 1_000)]
    pub batch_min_rows: usize,

    /// Producer: maximum rows per write batch.
    #[arg(long, env = "RIFFLE_BATCH_MAX_ROWS", default_value_t = 500_000)]
    pub batch_max_rows: usize,

    /// Producer: interval between writes (seconds).
    #[arg(long, env = "RIFFLE_WRITE_INTERVAL_SECS", default_value_t = 8)]
    pub write_interval_secs: u64,

    /// Consumer: poll interval (seconds).
    #[arg(long, env = "RIFFLE_POLL_INTERVAL_SECS", default_value_t = 3)]
    pub poll_interval_secs: u64,

    /// Consumer: path to durable checkpoint file.
    #[arg(long, env = "RIFFLE_CHECKPOINT_FILE", default_value = "riffle-checkpoint.json")]
    pub checkpoint_file: String,

    /// Consumer: rows per chunk when splitting a large version.
    #[arg(long, env = "RIFFLE_CHUNK_TARGET_ROWS", default_value_t = 50_000)]
    pub chunk_target_rows: u64,

    /// Consumer: merge consecutive small versions until total exceeds this.
    #[arg(long, env = "RIFFLE_MERGE_THRESHOLD_ROWS", default_value_t = 30_000)]
    pub merge_threshold_rows: u64,

    /// Consumer: split versions larger than this into chunks.
    #[arg(long, env = "RIFFLE_SPLIT_THRESHOLD_ROWS", default_value_t = 100_000)]
    pub split_threshold_rows: u64,

    /// Azure auth method: `cli`, `msi`, `key`, or `env`. Only used for `abfss://` / `az://` URIs.
    #[arg(long, env = "RIFFLE_AZURE_AUTH", default_value = "cli")]
    pub azure_auth: String,
}

/// The detected storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    LocalFs,
    Azure,
    S3,
    Gcs,
}

impl Backend {
    pub fn detect(uri: &str) -> Backend {
        let lower = uri.to_lowercase();
        if lower.starts_with("abfss://") || lower.starts_with("abfs://") || lower.starts_with("az://") {
            Backend::Azure
        } else if lower.starts_with("s3://") || lower.starts_with("s3a://") {
            Backend::S3
        } else if lower.starts_with("gs://") {
            Backend::Gcs
        } else {
            // Treat anything else (./..., /abs/path, file://...) as local filesystem.
            Backend::LocalFs
        }
    }
}

impl Config {
    pub fn backend(&self) -> Backend {
        Backend::detect(&self.table_uri)
    }

    /// Build the storage_options HashMap that delta-rs expects.
    /// For local filesystem this is empty; for clouds it picks up auth from env.
    pub fn storage_options(&self) -> Result<HashMap<String, String>> {
        let mut opts = HashMap::new();
        match self.backend() {
            Backend::LocalFs => {}
            Backend::Azure => {
                if let Some(account) = extract_azure_account(&self.table_uri) {
                    opts.insert("account_name".to_string(), account);
                } else if let Ok(a) = std::env::var("AZURE_STORAGE_ACCOUNT") {
                    opts.insert("account_name".to_string(), a);
                }
                match self.azure_auth.as_str() {
                    "cli" => {
                        opts.insert("use_azure_cli".to_string(), "true".to_string());
                    }
                    "msi" => {
                        opts.insert("use_azure_managed_identity".to_string(), "true".to_string());
                    }
                    "key" => {
                        if let Ok(k) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY")
                            .or_else(|_| std::env::var("AZURE_STORAGE_KEY"))
                        {
                            opts.insert("account_key".to_string(), k);
                        } else {
                            bail!(
                                "RIFFLE_AZURE_AUTH=key but neither AZURE_STORAGE_ACCOUNT_KEY \
                                 nor AZURE_STORAGE_KEY is set"
                            );
                        }
                    }
                    "env" => {
                        // delta-rs / object_store will pick up standard AZURE_* vars.
                    }
                    other => bail!(
                        "Unknown RIFFLE_AZURE_AUTH '{}'. Use cli | msi | key | env.",
                        other
                    ),
                }
            }
            Backend::S3 => {
                // object_store reads AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                // AWS_SESSION_TOKEN automatically. Surface the unsafe-rename flag explicitly
                // so users hit a useful error rather than a confusing one.
                if std::env::var("AWS_S3_ALLOW_UNSAFE_RENAME").is_ok() {
                    opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
                }
            }
            Backend::Gcs => {
                // object_store picks up GOOGLE_APPLICATION_CREDENTIALS automatically.
            }
        }
        Ok(opts)
    }

    /// Register cloud-specific delta-rs handlers based on the backend.
    pub fn register_handlers(&self) {
        match self.backend() {
            Backend::Azure => deltalake::azure::register_handlers(None),
            Backend::S3 => deltalake::aws::register_handlers(None),
            Backend::Gcs => deltalake::gcp::register_handlers(None),
            Backend::LocalFs => {}
        }
    }
}

fn extract_azure_account(uri: &str) -> Option<String> {
    // abfss://container@account.dfs.core.windows.net/...
    let after_scheme = uri.split("://").nth(1)?;
    let host = after_scheme.split('/').next()?;
    let host_only = host.split('@').nth(1).unwrap_or(host);
    host_only.split('.').next().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_backends() {
        assert_eq!(Backend::detect("./data/x.delta"), Backend::LocalFs);
        assert_eq!(Backend::detect("/var/lib/delta"), Backend::LocalFs);
        assert_eq!(Backend::detect("file:///tmp/x"), Backend::LocalFs);
        assert_eq!(
            Backend::detect("abfss://c@acct.dfs.core.windows.net/p"),
            Backend::Azure
        );
        assert_eq!(Backend::detect("s3://bucket/key"), Backend::S3);
        assert_eq!(Backend::detect("gs://bucket/key"), Backend::Gcs);
    }

    #[test]
    fn parses_azure_account() {
        assert_eq!(
            extract_azure_account("abfss://container@myacct.dfs.core.windows.net/path"),
            Some("myacct".to_string())
        );
    }
}
