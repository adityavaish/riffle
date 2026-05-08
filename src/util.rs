//! Small helpers shared by call sites that interact with delta-rs APIs.
//!
//! These exist to centralize the friction points after the 0.22 -> 0.32
//! upgrade: the URI/Url split, and `Option<u64>` table versions that
//! Riffle internally tracks as `i64`.

use anyhow::{anyhow, Result};
use url::Url;

/// Parse a Delta table URI into a `url::Url` for the new delta-rs APIs
/// (`open_table_with_storage_options`, `try_from_url_with_storage_options`).
///
/// Best-effort: tries `Url::parse` first and falls back to interpreting the
/// input as an absolute filesystem path so locally-given paths still work.
pub fn parse_table_uri(uri: &str) -> Result<Url> {
    if let Ok(u) = Url::parse(uri) {
        return Ok(u);
    }
    Url::from_file_path(uri).map_err(|_| anyhow!("invalid table URI: {}", uri))
}

/// Convert a `DeltaTable::version()` (`Option<u64>`) into the `i64` Riffle
/// tracks throughout its state model. An uninitialized table maps to `-1`.
#[inline]
pub fn version_to_i64(v: Option<u64>) -> i64 {
    v.map(|x| x as i64).unwrap_or(-1)
}
