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

    /// Azure auth method: `auto`, `cli`, `msi`, `key`, or `env`. Only used for `abfss://` / `az://` URIs.
    /// `auto` (default) picks the first available of: env (service principal), msi, then cli
    /// (which also picks up tokens from VS Code's Azure Account extension).
    #[arg(long, env = "RIFFLE_AZURE_AUTH", default_value = "auto")]
    pub azure_auth: String,

    /// Enable the streaming sink. When on, the consumer reads newly added rows from the
    /// source table and transfers them into a target Delta table per `--sink-mode`.
    #[arg(long, env = "RIFFLE_SINK_ENABLED", default_value_t = false)]
    pub sink_enabled: bool,

    /// Target Delta table for the sink. Same URI rules as `--table-uri`.
    #[arg(long, env = "RIFFLE_TARGET_TABLE_URI", default_value = "./data/riffle-target.delta")]
    pub target_table_uri: String,

    /// Sink transfer semantics: `append`, `overwrite`, or `merge`.
    #[arg(long, env = "RIFFLE_SINK_MODE", default_value = "merge")]
    pub sink_mode: String,

    /// Merge keys (comma-separated). Required when `--sink-mode merge`.
    #[arg(long, env = "RIFFLE_MERGE_KEYS", default_value = "event_id")]
    pub merge_keys: String,

    /// Columns updated by `WHEN MATCHED UPDATE` (comma-separated).
    /// Empty / unset = all non-key source columns.
    #[arg(long, env = "RIFFLE_MERGE_UPDATE_COLUMNS", default_value = "")]
    pub merge_update_columns: String,

    /// SQL predicate (against `source.*` / `target.*`) gating WHEN MATCHED UPDATE.
    /// Example: `"source.event_timestamp > target.event_timestamp"`.
    #[arg(long, env = "RIFFLE_MERGE_UPDATE_PREDICATE")]
    pub merge_update_predicate: Option<String>,

    /// SQL predicate gating WHEN MATCHED DELETE. If set, matched rows satisfying it are deleted.
    #[arg(long, env = "RIFFLE_MERGE_DELETE_PREDICATE")]
    pub merge_delete_predicate: Option<String>,

    /// SQL predicate gating WHEN NOT MATCHED INSERT (only insert rows where this is true).
    #[arg(long, env = "RIFFLE_MERGE_INSERT_PREDICATE")]
    pub merge_insert_predicate: Option<String>,

    /// Probability (0.0–1.0) that a producer row reuses a recent event_id, so the
    /// MERGE actually has matched rows to update. Only effective when the demo
    /// producer is enabled.
    #[arg(long, env = "RIFFLE_UPDATE_FRACTION", default_value_t = 0.2)]
    pub update_fraction: f64,
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

    pub fn target_backend(&self) -> Backend {
        Backend::detect(&self.target_table_uri)
    }

    /// Build the storage_options HashMap that delta-rs expects.
    /// For local filesystem this is empty; for clouds it picks up auth from env.
    pub fn storage_options(&self) -> Result<HashMap<String, String>> {
        build_storage_options(&self.table_uri, &self.azure_auth)
    }

    /// Storage options for the sink target table.
    pub fn target_storage_options(&self) -> Result<HashMap<String, String>> {
        build_storage_options(&self.target_table_uri, &self.azure_auth)
    }

    /// Register cloud-specific delta-rs handlers based on the backends used by both
    /// the source and (if enabled) target tables.
    pub fn register_handlers(&self) {
        let mut backends: Vec<Backend> = vec![self.backend()];
        if self.sink_enabled {
            backends.push(self.target_backend());
        }
        register_handlers_for(&backends);
    }
}

/// Build the storage_options HashMap delta-rs expects for a given URI.
/// Free function so non-Config callers (e.g. `riffle-sink` CLI) can reuse it.
pub fn build_storage_options(uri: &str, azure_auth: &str) -> Result<HashMap<String, String>> {
    let mut opts = HashMap::new();
    match Backend::detect(uri) {
        Backend::LocalFs => {}
        Backend::Azure => {
            if let Some(account) = extract_azure_account(uri) {
                opts.insert("account_name".to_string(), account);
            } else if let Ok(a) = std::env::var("AZURE_STORAGE_ACCOUNT") {
                opts.insert("account_name".to_string(), a);
            }
            let resolved = resolve_azure_auth(azure_auth)?;
            match resolved.as_str() {
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
                            "azure_auth=key but neither AZURE_STORAGE_ACCOUNT_KEY \
                             nor AZURE_STORAGE_KEY is set"
                        );
                    }
                }
                "env" => {}
                other => bail!(
                    "Unknown azure_auth '{}'. Use auto | cli | msi | key | env.",
                    other
                ),
            }
        }
        Backend::S3 => {
            if std::env::var("AWS_S3_ALLOW_UNSAFE_RENAME").is_ok() {
                opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
            }
        }
        Backend::Gcs => {}
    }
    Ok(opts)
}

/// Register cloud-specific delta-rs handlers for the listed backends.
pub fn register_handlers_for(backends: &[Backend]) {
    let mut seen_az = false;
    let mut seen_s3 = false;
    let mut seen_gcs = false;
    for b in backends {
        match b {
            Backend::Azure if !seen_az => {
                deltalake::azure::register_handlers(None);
                seen_az = true;
            }
            Backend::S3 if !seen_s3 => {
                deltalake::aws::register_handlers(None);
                seen_s3 = true;
            }
            Backend::Gcs if !seen_gcs => {
                deltalake::gcp::register_handlers(None);
                seen_gcs = true;
            }
            _ => {}
        }
    }
}

/// Resolve `auto` to a concrete Azure auth method, mimicking
/// DefaultAzureCredential's preference order without adding the heavy
/// `azure_identity` crate.
///
/// Order:
///   1. Service principal env vars present (AZURE_TENANT_ID + AZURE_CLIENT_ID + AZURE_CLIENT_SECRET) → `env`
///   2. Managed identity endpoint vars present (IDENTITY_ENDPOINT or MSI_ENDPOINT)        → `msi`
///   3. Otherwise                                                                          → `cli`
///      (which also picks up tokens cached by the VS Code Azure Account extension since
///      it shares `~/.azure` with the Azure CLI).
pub fn resolve_azure_auth(requested: &str) -> Result<String> {
    let want = requested.trim().to_lowercase();
    if want != "auto" {
        return Ok(want);
    }
    let has_sp = std::env::var("AZURE_TENANT_ID").is_ok()
        && std::env::var("AZURE_CLIENT_ID").is_ok()
        && std::env::var("AZURE_CLIENT_SECRET").is_ok();
    if has_sp {
        tracing::info!("[auth] auto → env (service principal)");
        return Ok("env".to_string());
    }
    let has_msi =
        std::env::var("IDENTITY_ENDPOINT").is_ok() || std::env::var("MSI_ENDPOINT").is_ok();
    if has_msi {
        tracing::info!("[auth] auto → msi (managed identity endpoint detected)");
        return Ok("msi".to_string());
    }
    tracing::info!("[auth] auto → cli (Azure CLI / VS Code Azure Account extension)");
    Ok("cli".to_string())
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
