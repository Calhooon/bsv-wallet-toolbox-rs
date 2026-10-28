mod common;
#[path = "common/ladder.rs"]
mod ladder;
#[path = "common/measurement.rs"]
mod measurement;

use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::WalletInterface;
use bsv_wallet_toolbox_rs::benchmark_support::allocate_change_input;
use bsv_wallet_toolbox_rs::WalletStorageWriter;
use criterion::Criterion;
use tokio::runtime::Runtime;

use common::{list_args, payment_args, seed, Store, ORIGINATOR};
use measurement::measure;

fn main() {
    let directory = std::env::var_os("CRITERION_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(
                std::env::var_os("CARGO_TARGET_DIR")
                    .expect("set CARGO_TARGET_DIR outside the checkout"),
            )
            .join("criterion")
        });
    let sample_size = std::env::var("DOOR_BENCH_SAMPLES")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("DOOR_BENCH_SAMPLES is an integer >= 10")
        })
        .unwrap_or(50);
    assert!(sample_size >= 10, "DOOR_BENCH_SAMPLES must be >= 10");
    let reduced = std::env::var_os("DOOR_BENCH_SAMPLES").is_some();
    let mut criterion = Criterion::default()
        .sample_size(sample_size)
        .warm_up_time(Duration::from_millis(if reduced { 100 } else { 500 }))
        .measurement_time(Duration::from_secs(if reduced { 1 } else { 3 }))
        .nresamples(if reduced { 1_000 } else { 10_000 })
        .output_directory(&directory)
        .configure_from_args();
    let runtime = Runtime::new().expect("Tokio runtime");
    std::fs::create_dir_all(&directory).expect("Criterion output directory");
    let run_path = directory.join("door-run.json");
    std::fs::write(
        &run_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "finished": false, "benchmarks": [], "sample_size": sample_size,
            "quick": std::env::args().any(|arg| arg == "--quick"),
            "started_at": chrono::Utc::now().to_rfc3339(),
        }))
        .expect("serialize"),
    )
    .expect("start run manifest");

    measure(&mut criterion, &directory, "seed_10000", || {
        let started = Instant::now();
        let store = runtime.block_on(seed(10_000));
        let elapsed = started.elapsed();
        runtime.block_on(store.assert_clean());
        // Shut down pools before destroying the fixture, outside call time.
        runtime.block_on(close(store));
        elapsed
    });

    let small = runtime.block_on(seed(1_000));
    let large = runtime.block_on(seed(10_000));
    for (store, offered) in [(&small, 100), (&large, 100), (&large, 10_000)] {
        let name = format!(
            "create_action_offering_{}_coins_{}_offered",
            store.coins.len(),
            offered
        );
        measure(&mut criterion, &directory, &name, || {
            runtime.block_on(async {
                let args = payment_args();
                let started = Instant::now();
                let result = store
                    .wallet
                    .create_action_offering(args, Some(ORIGINATOR), &store.coins[..offered], &[])
                    .await
                    .expect("offered signed action");
                let elapsed = started.elapsed();
                verify_signed(
                    store,
                    result.tx.as_deref().expect("signed transaction"),
                    &result.txid.expect("signed txid"),
                )
                .await;
                store.reset().await;
                elapsed
            })
        });
        runtime.block_on(store.assert_clean());
    }

    measure(
        &mut criterion,
        &directory,
        "create_action_process_action_10000",
        || {
            runtime.block_on(async {
                let args = payment_args();
                let started = Instant::now();
                // Wallet's public create_action runs the real signer and the
                // process_action writes in the door's single transaction.
                let result = large
                    .wallet
                    .create_action(args, Some(ORIGINATOR))
                    .await
                    .expect("signed action");
                let elapsed = started.elapsed();
                verify_signed(
                    &large,
                    result.tx.as_deref().expect("signed transaction"),
                    &result.txid.expect("signed txid"),
                )
                .await;
                large.reset().await;
                elapsed
            })
        },
    );

    measure(
        &mut criterion,
        &directory,
        "internalize_action_stored_proof_10000",
        || {
            runtime.block_on(async {
                let args = large.internalize_args();
                let started = Instant::now();
                let result = large
                    .wallet
                    .storage()
                    .internalize_action(&large.auth, args)
                    .await
                    .expect("internalize");
                let elapsed = started.elapsed();
                assert!(result.base.accepted && !result.is_merge);
                let (status,): (String,) =
                    sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
                        .bind(result.txid)
                        .fetch_one(large.wallet.storage().pool())
                        .await
                        .expect("imported row");
                assert_eq!(status, "completed");
                assert_eq!(
                    large.wallet.services().call_count("get_header_for_height"),
                    1,
                    "intake reads the local mock header"
                );
                assert_eq!(
                    large.wallet.services().total_calls(),
                    1,
                    "no proof or raw transaction fetch"
                );
                large.reset().await;
                elapsed
            })
        },
    );

    measure(&mut criterion, &directory, "list_outputs_10000", || {
        runtime.block_on(async {
            let args = list_args();
            let started = Instant::now();
            let result = large
                .wallet
                .list_outputs(args, Some(ORIGINATOR))
                .await
                .expect("list outputs");
            let elapsed = started.elapsed();
            assert_eq!(result.total_outputs, 10_000);
            assert_eq!(result.outputs.len(), 10_000);
            black_box(result);
            elapsed
        })
    });

    measure(
        &mut criterion,
        &directory,
        "allocate_change_input_10000",
        || {
            runtime.block_on(async {
                let mut writer = large
                    .wallet
                    .storage()
                    .write_tx()
                    .await
                    .expect("allocation transaction");
                let started = Instant::now();
                let coin = allocate_change_input(
                    &mut writer,
                    large.auth.user_id.expect("user"),
                    large.basket,
                    1,
                    1_000,
                )
                .await
                .expect("allocate coin");
                let elapsed = started.elapsed();
                assert!(coin.is_some_and(|id| large.coins.contains(&id)));
                writer.rollback().await.expect("restore allocated coin");
                elapsed
            })
        },
    );
    runtime.block_on(large.assert_clean());
    runtime.block_on(close(small));
    runtime.block_on(close(large));
    ladder::benchmarks(&mut criterion, &directory, &runtime);
    criterion.final_summary();
    let mut run: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&run_path).expect("run manifest")).expect("run JSON");
    run["finished"] = serde_json::json!(true);
    std::fs::write(
        run_path,
        serde_json::to_vec_pretty(&run).expect("serialize"),
    )
    .expect("finish run manifest");
}

async fn verify_signed(store: &Store, bytes: &[u8], expected_txid: &[u8]) {
    let tx = Transaction::from_binary(bytes).expect("signed transaction parses");
    let txid = tx.id();
    assert!(!tx.inputs.is_empty());
    for input in &tx.inputs {
        assert!(
            input
                .unlocking_script
                .as_ref()
                .is_some_and(|script| script.to_binary().len() > 70),
            "funding input carries a P2PKH signature"
        );
    }
    let raw = tx.to_binary();
    assert_eq!(hex::encode(expected_txid), txid);
    let (saved, status): (Vec<u8>, String) =
        sqlx::query_as("SELECT raw_tx, status FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_one(store.wallet.storage().pool())
            .await
            .expect("processed transaction");
    assert_eq!(saved, raw);
    assert_eq!(status, "nosend");
    assert_eq!(
        store.wallet.services().total_calls(),
        0,
        "no network or mock broadcast"
    );
}

async fn close(store: Store) {
    store.wallet.storage().pool().close().await;
    store.wallet.storage().writer().close().await;
}
