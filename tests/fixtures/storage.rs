//! Replay the storage door against recorded inputs, including every persisted column.

mod compare;
mod door;
mod model;
mod normalize;
mod operations;
mod received_rule;
mod seed;

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::CreateActionArgs;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::{AuthId, StorageCreateActionResult};
use bsv_wallet_toolbox_rs::StorageSqlx;
use bsv_wallet_toolbox_rs::Wallet;
use serde_json::{json, Value};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use model::Fixture;

struct Replay {
    storage: StorageSqlx,
    auth: AuthId,
    root: PrivateKey,
    seeds: BTreeMap<String, seed::Seeded>,
    drafts: BTreeMap<String, (StorageCreateActionResult, CreateActionArgs)>,
    wallet: Option<Wallet<StorageSqlx, MockWalletServices>>,
    wallet_handles: BTreeMap<String, (Option<String>, String)>,
}

// Ordered by dependency and creation id. Maps are part of the contract too.
const TABLES: &[(&str, &str)] = &[
    ("users", "user_id"),
    ("output_baskets", "basket_id"),
    ("transactions", "transaction_id"),
    ("outputs", "output_id"),
    ("proven_txs", "proven_tx_id"),
    ("proven_tx_reqs", "proven_tx_req_id"),
    ("tx_labels", "tx_label_id"),
    ("tx_labels_map", "tx_label_map_id"),
    ("output_tags", "output_tag_id"),
    ("output_tags_map", "output_tag_map_id"),
    ("commissions", "commission_id"),
    ("sync_states", "sync_state_id"),
    ("broadcast_seen", "txid, provider"),
    ("broadcast_prefs", "key"),
    ("locked_input_checks", "output_id"),
    ("proof_root_checks", "txid"),
    ("monitor_state", "key"),
];

async fn rows(storage: &StorageSqlx) -> Result<Value> {
    let mut tables = serde_json::Map::new();
    for &(table, order) in TABLES {
        let query = format!("SELECT * FROM {table} ORDER BY {order}");
        let mut records = Vec::new();
        for row in sqlx::query(&query).fetch_all(storage.pool()).await? {
            let mut record = serde_json::Map::new();
            for column in row.columns() {
                let key = column.name();
                let raw = row.try_get_raw(key)?;
                let value = if raw.is_null() {
                    Value::Null
                } else {
                    match raw.type_info().name() {
                        "INTEGER" => json!(row.try_get::<i64, _>(key)?),
                        "REAL" => json!(row.try_get::<f64, _>(key)?),
                        "BLOB" => json!(hex::encode(row.try_get::<Vec<u8>, _>(key)?)),
                        _ => json!(row.try_get::<String, _>(key)?),
                    }
                };
                record.insert(key.to_string(), value);
            }
            records.push(Value::Object(record));
        }
        tables.insert(table.to_string(), records.into());
    }
    Ok(tables.into())
}

#[tokio::test]
async fn replay_storage_fixtures() -> Result<()> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/data/storage");
    let mut paths = std::fs::read_dir(&directory)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "json"));
    paths.sort();
    anyhow::ensure!(
        !paths.is_empty(),
        "no storage fixtures in {}",
        directory.display()
    );
    let update = std::env::var("UPDATE_FIXTURES").as_deref() == Ok("1");
    let mut failures = Vec::new();
    for path in &paths {
        let source = std::fs::read_to_string(path)?;
        let fixture: Fixture =
            serde_json::from_str(&source).with_context(|| format!("reading {}", path.display()))?;
        anyhow::ensure!(
            !fixture.name.is_empty() && [53, 66, 103, 138, 165, 169].contains(&fixture.issue),
            "fixture identity"
        );
        anyhow::ensure!(!fixture.calls.is_empty(), "{} has no calls", fixture.name);
        let mut replay = seed::setup(&fixture.inputs).await?;
        let mut normalizer = normalize::Normalizer::default();
        let mut actual = Vec::new();
        for (index, call) in fixture.calls.iter().enumerate() {
            let result = replay
                .execute(call)
                .await
                .with_context(|| format!("{} call {}", fixture.name, index + 1))?;
            let snapshot = rows(&replay.storage).await?;
            normalizer.prepare(&snapshot, &replay.root)?;
            let history: Vec<_> = replay
                .services()
                .call_history()
                .iter()
                .map(|call| {
                    json!({
                        "method": call.method, "args": call.args, "success": call.success
                    })
                })
                .collect();
            actual.push(normalizer.value(json!({
                "result": result, "rows": snapshot, "serviceCalls": history
            }))?);
        }
        let actual = Value::Array(actual);
        let matches = actual == fixture.expected;
        if update {
            let mut document: Value = serde_json::from_str(&source)?;
            document["expected"] = actual;
            let rendered = compare::format(&document);
            anyhow::ensure!(
                rendered.lines().count() <= 800,
                "{} exceeds 800 lines",
                fixture.name
            );
            std::fs::write(path, rendered)?;
        } else if actual != fixture.expected {
            compare::diff(&fixture.name, &fixture.expected, &actual)?;
            failures.push(fixture.name.clone());
        }
        println!(
            "{}: {} ({} calls)",
            fixture.name,
            if update {
                "updated"
            } else if matches {
                "ok"
            } else {
                "DIFF"
            },
            fixture.calls.len()
        );
    }
    anyhow::ensure!(
        failures.is_empty(),
        "storage fixture differences: {}",
        failures.join(", ")
    );
    println!("replayed {} storage fixtures", paths.len());
    Ok(())
}

impl Replay {
    fn services(&self) -> &MockWalletServices {
        self.wallet
            .as_ref()
            .expect("fixture wallet initialized")
            .services()
    }

    fn resolve(&self, value: Value) -> Result<Value> {
        Ok(match value {
            Value::String(s) if s.starts_with("$outpoint:") => {
                let (name, vout) = s[10..].rsplit_once('.').context("outpoint name.vout")?;
                let tx = &self.seeds.get(name).context("unknown seed")?.tx;
                json!(format!("{}.{vout}", tx.id()))
            }
            Value::String(s) if s.starts_with("$txid:") => {
                json!(self.seeds.get(&s[6..]).context("unknown seed")?.tx.id())
            }
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(|item| self.resolve(item))
                    .collect::<Result<_>>()?,
            ),
            Value::Object(items) => Value::Object(
                items
                    .into_iter()
                    .map(|(key, item)| Ok((key, self.resolve(item)?)))
                    .collect::<Result<_>>()?,
            ),
            other => other,
        })
    }
}
