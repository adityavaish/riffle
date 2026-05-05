# ⚡ Riffle

> **Adaptive Delta Lake CDC streaming consumer with a real-time, tunable web dashboard — written in Rust.**

Riffle reads the Change Data Feed of a [Delta Lake](https://delta.io/) table version-by-version, adaptively **splits** large commits into manageable chunks and **merges** small consecutive commits into batched work units, persists progress to a durable checkpoint, and exposes everything through a live web dashboard you can re-tune at runtime.

It works against any storage backend [delta-rs](https://github.com/delta-io/delta-rs) supports:

| Scheme                           | Backend                          | Auth                                          |
| -------------------------------- | -------------------------------- | --------------------------------------------- |
| `./...`, `/abs/path`, `file://`  | Local filesystem                 | none (default)                                |
| `abfss://`, `az://`              | Azure Data Lake Storage Gen2     | `az login` / Managed Identity / account key   |
| `s3://`                          | AWS S3                           | standard `AWS_*` env vars                     |
| `gs://`                          | Google Cloud Storage             | `GOOGLE_APPLICATION_CREDENTIALS`              |

---

## Features

- 🌊 **Adaptive consumer** — three modes selected per poll:
  - **single**  — one commit → one work unit
  - **merged**  — many small commits coalesced into one batch (reduces overhead)
  - **split**   — one large commit divided into row-bounded chunks (bounds memory & latency)
- 📍 **Durable checkpointing** — JSON checkpoint persists `last_full_version + partial_chunk_offset + rows_committed`, so the consumer resumes mid-version after a crash.
- 🎛️ **Live tunables** — change chunk size, merge threshold, and split threshold from the dashboard; consumer picks them up on its next poll without restart (atomic, lock-free).
- 📊 **Real-time dashboard** — Server-Sent Events stream pushes producer/consumer metrics, lag, and an event timeline straight to the browser.
- 🧪 **Demo producer included** — randomly-sized batches (1K–500K rows by default) for instant local experimentation; disable with one flag for real workloads.
- 🔌 **Pluggable** — swap `sample_data.rs` for your own schema, or disable the producer entirely and let Riffle just consume what someone else writes.

---

## Quick start (zero cloud setup)

```bash
git clone https://github.com/adityavaish/riffle
cd riffle
cargo run --release
```

Open <http://localhost:3000>. The defaults create a Delta table at `./data/riffle-demo.delta` and start producing/consuming infinitely.

---

## Configuration

All settings can be passed as **CLI flags** or **environment variables** (see `.env.example`). Run `riffle --help` for the full list.

| Flag / Env                            | Default                       | Description                                       |
| ------------------------------------- | ----------------------------- | ------------------------------------------------- |
| `--table-uri` / `RIFFLE_TABLE_URI`    | `./data/riffle-demo.delta`    | Delta table URI (any supported scheme)            |
| `--bind-addr` / `RIFFLE_BIND_ADDR`    | `0.0.0.0:3000`                | Dashboard bind address                            |
| `--producer-enabled`                  | `true`                        | Run the demo producer                             |
| `--consumer-enabled`                  | `true`                        | Run the CDC consumer                              |
| `--batch-min-rows`                    | `1000`                        | Producer: min rows per write                      |
| `--batch-max-rows`                    | `500000`                      | Producer: max rows per write                      |
| `--write-interval-secs`               | `8`                           | Producer: pause between writes                    |
| `--poll-interval-secs`                | `3`                           | Consumer: pause between polls                     |
| `--checkpoint-file`                   | `riffle-checkpoint.json`      | Consumer: durable checkpoint file path            |
| `--chunk-target-rows`                 | `50_000`                      | Consumer: rows per chunk when splitting           |
| `--merge-threshold-rows`              | `30_000`                      | Consumer: merge versions until total exceeds this |
| `--split-threshold-rows`              | `100_000`                     | Consumer: split versions larger than this         |
| `--azure-auth`                        | `cli`                         | Azure auth: `cli`, `msi`, `key`, `env`            |

---

## Cloud examples

### Azure (az login)

```bash
az login
RIFFLE_TABLE_URI=abfss://mycontainer@myaccount.dfs.core.windows.net/path/to/table cargo run --release
```

### Azure (Managed Identity, e.g. inside an Azure VM/AKS)

```bash
RIFFLE_AZURE_AUTH=msi \
RIFFLE_TABLE_URI=abfss://mycontainer@myaccount.dfs.core.windows.net/path/to/table \
cargo run --release
```

### AWS S3

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_S3_ALLOW_UNSAFE_RENAME=true   # required without a lock provider
RIFFLE_TABLE_URI=s3://my-bucket/path/to/table cargo run --release
```

### Google Cloud Storage

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
RIFFLE_TABLE_URI=gs://my-bucket/path/to/table cargo run --release
```

---

## Architecture

```
┌────────────┐  appends   ┌──────────────────┐  reads commits  ┌──────────────┐
│  Producer  │ ─────────▶ │  Delta table     │ ──────────────▶ │   Consumer   │
│ (optional) │            │  (any backend)   │                 │  (adaptive)  │
└────────────┘            └──────────────────┘                 └──────┬───────┘
                                                                      │
                                                            checkpoint │ split / merge / single
                                                                      ▼
                                                              ┌──────────────┐
                                                              │ checkpoint   │
                                                              │   .json      │
                                                              └──────────────┘
                                                                      │
                                                                      ▼
                                            ┌──────────────────────────────────────────┐
                                            │  axum web server (SSE + REST tunables)   │
                                            └──────────────────┬───────────────────────┘
                                                               ▼
                                                        Browser dashboard
```

- The consumer **does not** read Parquet data during planning. It opens each pending commit's small JSON file under `_delta_log/` and counts `numRecords` from the `add` action stats — cheap enough to do every poll.
- Tunables live in three `Arc<AtomicU64>`s. The HTTP `POST /api/config` handler stores new values; the consumer loads them at the start of every poll iteration with `Ordering::Relaxed`. No locks, no restart, no signal plumbing.
- The pre-launch baseline version is captured **before** spawning the producer to avoid a race where the producer's first `Overwrite` write would be treated as part of the pre-existing table.

---

## REST API

| Method | Path           | Body                                                                 | Returns           |
| ------ | -------------- | -------------------------------------------------------------------- | ----------------- |
| GET    | `/`            | —                                                                    | dashboard HTML    |
| GET    | `/events`      | —                                                                    | SSE state stream  |
| GET    | `/api/config`  | —                                                                    | current tunables  |
| POST   | `/api/config`  | `{ "chunk_target_rows":?, "merge_threshold_rows":?, "split_threshold_rows":? }` | new snapshot      |
| GET    | `/api/health`  | —                                                                    | `ok`              |

Example:

```bash
curl -X POST http://localhost:3000/api/config \
  -H 'Content-Type: application/json' \
  -d '{"chunk_target_rows":20000}'
```

---

## Adapting Riffle to your workload

1. **Replace the producer** — edit `src/sample_data.rs` to match your schema, or pass `--producer-enabled=false` and let Riffle just consume what your real pipeline writes.
2. **Replace `process_work` in `src/consumer.rs`** — currently it sleeps proportionally to row count to simulate downstream work. Plug in your actual sink (a `MERGE INTO`, an HTTP push, an S3 write, etc.).
3. **Tune defaults** — change `RIFFLE_*` env vars or pass CLI flags. Live values can also be changed from the dashboard.

---

## Build

```bash
cargo build --release
./target/release/riffle --help
```

Tested on Rust 1.74+. Built with [`delta-rs 0.22`](https://github.com/delta-io/delta-rs), [`axum 0.7`](https://github.com/tokio-rs/axum), [`tokio`](https://tokio.rs/), and [`arrow-rs 53`](https://github.com/apache/arrow-rs).

---

## License

Apache-2.0 — see [LICENSE](./LICENSE).

---

## Contributing

Issues and PRs welcome. Please make sure `cargo build` and `cargo test` pass before submitting.
