//! Generic example data generator.
//!
//! This produces synthetic transaction-like rows for the demo. Replace this
//! module (or set `producer_enabled = false` and write to the table from your
//! own pipeline) when adapting Riffle to your real schema.

use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{TimeDelta, Utc};
use rand::rngs::StdRng;
use rand::Rng;
use std::sync::Arc;
use uuid::Uuid;

pub fn build_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("account_id", DataType::Utf8, false),
        Field::new("event_timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(18, 4), false),
        Field::new("currency", DataType::Utf8, false),
        Field::new("is_eligible", DataType::Boolean, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("batch_id", DataType::Int32, false),
    ]))
}

pub fn generate_batch(
    schema: &Arc<Schema>,
    num_rows: usize,
    batch_id: i32,
    rng: &mut StdRng,
) -> Result<RecordBatch> {
    // Generic, vendor-neutral sample values.
    let event_types = ["create", "update", "delete", "refund", "credit"];
    let categories = ["compute", "storage", "network", "database", "analytics", "ml"];
    let products = ["service-a", "service-b", "service-c", "service-d", "service-e"];
    let currencies = ["USD", "EUR", "GBP", "JPY", "CAD"];
    let regions = ["us-east", "us-west", "eu-west", "ap-south", "ap-east"];
    let statuses = ["active", "completed", "pending", "processing"];

    let now = Utc::now().naive_utc();

    let event_ids: Vec<String> = (0..num_rows).map(|_| Uuid::new_v4().to_string()).collect();
    let account_ids: Vec<String> = (0..num_rows)
        .map(|_| format!("acct-{:08}", rng.gen_range(0u32..99_999_999)))
        .collect();
    let timestamps: Vec<i64> = (0..num_rows)
        .map(|_| {
            (now - TimeDelta::seconds(rng.gen_range(0..3600)))
                .and_utc()
                .timestamp_micros()
        })
        .collect();
    let event_type_vals: Vec<&str> =
        (0..num_rows).map(|_| event_types[rng.gen_range(0..event_types.len())]).collect();
    let category_vals: Vec<&str> =
        (0..num_rows).map(|_| categories[rng.gen_range(0..categories.len())]).collect();
    let product_vals: Vec<&str> =
        (0..num_rows).map(|_| products[rng.gen_range(0..products.len())]).collect();
    let amounts: Vec<i128> =
        (0..num_rows).map(|_| rng.gen_range(1000i128..50_000_000)).collect();
    let currency_vals: Vec<&str> =
        (0..num_rows).map(|_| currencies[rng.gen_range(0..currencies.len())]).collect();
    let eligible: Vec<bool> = (0..num_rows).map(|_| rng.gen_bool(0.85)).collect();
    let region_vals: Vec<&str> =
        (0..num_rows).map(|_| regions[rng.gen_range(0..regions.len())]).collect();
    let status_vals: Vec<&str> =
        (0..num_rows).map(|_| statuses[rng.gen_range(0..statuses.len())]).collect();

    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                event_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                account_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(StringArray::from(event_type_vals)),
            Arc::new(StringArray::from(category_vals)),
            Arc::new(StringArray::from(product_vals)),
            Arc::new(Decimal128Array::from(amounts).with_precision_and_scale(18, 4)?),
            Arc::new(StringArray::from(currency_vals)),
            Arc::new(BooleanArray::from(eligible)),
            Arc::new(StringArray::from(region_vals)),
            Arc::new(StringArray::from(status_vals)),
            Arc::new(Int32Array::from(vec![batch_id; num_rows])),
        ],
    )?)
}
