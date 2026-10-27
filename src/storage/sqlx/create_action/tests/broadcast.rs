use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageProvider;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn test_the_broadcast_rebuild_has_a_network_fallback() {
    // Set up mock services that return a raw tx for a missing txid.
    // Rebuild a BEEF for broadcast where a parent is missing from local
    // storage (createAction asks no provider since bsv-wallet-cli #44;
    // the broadcast rebuild still does).
    // Verify the mock service was called and the BEEF is complete.
    use crate::services::mock::{MockResponse, MockWalletServices};
    use crate::services::traits::GetRawTxResult;
    use bsv_rs::primitives::sha256d;
    use std::sync::Arc;

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let compute_txid = |raw: &[u8]| -> String {
        let mut v = sha256d(raw).to_vec();
        v.reverse();
        hex::encode(v)
    };

    // The mock returns an unproven child of a locally proven parent.
    let chain = synth_chain(2);
    seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 100_000).await;
    let missing_raw_tx = chain[1].0.clone();
    // Compute the real txid from the raw bytes
    let missing_txid = compute_txid(&missing_raw_tx);

    // Configure mock services to return the raw tx
    let mock_services = MockWalletServices::builder()
        .get_raw_tx_response(MockResponse::Success(GetRawTxResult {
            name: "mock-woc".to_string(),
            txid: missing_txid.clone(),
            raw_tx: Some(missing_raw_tx.clone()),
            error: None,
            could_not_look: false,
        }))
        .build();
    storage.set_services(Arc::new(mock_services));

    let mut conn = storage.pool().acquire().await.unwrap();
    let beef = rebuild_beef_for_broadcast(
        &mut conn,
        storage.writer(),
        std::slice::from_ref(&missing_txid),
        Some(&storage),
    )
    .await
    .unwrap();

    // The fetched tx should be in the BEEF
    assert!(
        beef.find_txid(&missing_txid).is_some(),
        "Network-fetched tx must be in the BEEF"
    );

    // Verify the tx was also stored locally for future use
    // Note: proven_tx_reqs is written via the conn (within the same connection),
    // and we need to use the same connection to read it back.
    let stored = sqlx::query("SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?")
        .bind(&missing_txid)
        .fetch_optional(&mut *conn)
        .await
        .unwrap();
    assert!(
        stored.is_some(),
        "Network-fetched tx should be stored locally in proven_tx_reqs"
    );
}

// =========================================================================
// rebuild_beef_for_broadcast Tests
// =========================================================================

#[tokio::test]
async fn test_rebuild_beef_for_broadcast_with_proven_tx() {
    // Set up DB with a proven_txs entry, then call rebuild_beef_for_broadcast.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // Use a real coinbase tx so the raw_tx parses correctly
    let raw_tx = hex::decode(
        "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
    )
    .unwrap();
    let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    // Create a valid MerklePath for this tx and serialize it
    let bump = bsv_rs::transaction::MerklePath::from_coinbase_txid(txid, 100_000);
    let merkle_path_bytes = bump.to_binary();

    seed_proven_tx(&storage, txid, &raw_tx, &merkle_path_bytes).await;

    let mut conn = storage.pool().acquire().await.unwrap();
    let beef = rebuild_beef_for_broadcast(&mut conn, storage.writer(), &[txid.to_string()], None)
        .await
        .unwrap();

    // The tx should be in the BEEF
    assert!(
        beef.find_txid(txid).is_some(),
        "Rebuilt BEEF must contain the requested txid"
    );

    // The tx should have a bump (merkle proof)
    let beef_tx = beef.find_txid(txid).unwrap();
    assert!(
        beef_tx.bump_index().is_some(),
        "Rebuilt BEEF tx should have a merkle proof (bump_index)"
    );
}

#[tokio::test]
async fn test_rebuild_beef_for_broadcast_empty_txids() {
    // Calling with empty txids should return an empty BEEF
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let mut conn = storage.pool().acquire().await.unwrap();
    let beef = rebuild_beef_for_broadcast(&mut conn, storage.writer(), &[], None)
        .await
        .unwrap();

    assert!(
        beef.txs.is_empty(),
        "Empty input txids should produce an empty BEEF"
    );
}

#[tokio::test]
async fn test_rebuild_beef_for_broadcast_missing_txid() {
    // Calling with a txid not in storage should handle gracefully
    // (no crash, returns BEEF without the missing tx — network fallback
    // requires storage param which we pass as None here)
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = rebuild_beef_for_broadcast(
        &mut conn,
        storage.writer(),
        &["0000000000000000000000000000000000000000000000000000000000ffffff".to_string()],
        None,
    )
    .await;

    // Without network fallback (storage=None), a missing txid at depth 0
    // should error because it's a direct input
    assert!(
        result.is_err(),
        "Missing direct input txid without network fallback should error"
    );
}

/// Replay a real wallet's input BEEF build. Ignored unless `BEEF_CASE_DB`
/// points at a wallet.db copy and `BEEF_CASE_ROOTS` lists the input txids
/// (comma separated): `BEEF_CASE_DB=... BEEF_CASE_ROOTS=a,b cargo test
/// --lib -- --ignored replay_input_beef_case`. The p25 case of 2026-09-04
/// lives at ~/bsv/wallet-cases/p25-0829 (five roots, two sibling proofs).
#[tokio::test]
#[ignore = "needs BEEF_CASE_DB and BEEF_CASE_ROOTS"]
async fn replay_input_beef_case() {
    let db = std::env::var("BEEF_CASE_DB").expect("BEEF_CASE_DB");
    let roots: Vec<String> = std::env::var("BEEF_CASE_ROOTS")
        .expect("BEEF_CASE_ROOTS")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let storage = StorageSqlx::open(&db).await.unwrap();
    storage.make_available().await.unwrap();
    let mut beef = Beef::new();
    let mut pending: Vec<(String, usize)> = roots.iter().cloned().map(|t| (t, 0)).collect();
    let mut processed = HashSet::new();
    let mut conn = storage.pool().acquire().await.unwrap();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        None,
    )
    .await
    .unwrap();
    prune_beef_to_roots(&mut beef, &roots);
    let valid = beef.verify_valid(true).valid;
    println!(
        "replay: valid={valid} {} tx(s), {} bump(s), {} bytes",
        beef.txs.len(),
        beef.bumps.len(),
        beef.to_binary().len()
    );
    if !valid {
        panic!("{}", describe_invalid_beef(&mut beef, &roots));
    }
    for root in &roots {
        let tx = beef.find_txid(root).expect("every root is carried");
        assert!(
            tx.bump_index().is_some() || tx.raw_tx().is_some(),
            "root {root} is proven or carried whole"
        );
    }
}

#[tokio::test]
async fn test_the_broadcast_rebuild_skips_a_proof_lookup_for_a_young_transaction() {
    // A transaction this wallet recorded seconds ago cannot be mined yet,
    // and its ancestors answer the question anyway. No network call.
    use crate::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
    use crate::storage::traits::WalletStorageProvider;

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let chain = synth_chain(2);
    seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 965_000).await;
    seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, None).await;

    let services = std::sync::Arc::new(
        MockWalletServices::builder()
            .get_merkle_path_response(MockResponse::Error(
                MockErrorKind::ServiceError,
                "must not be called".to_string(),
            ))
            .build(),
    );
    storage.set_services(services.clone());

    let mut conn = storage.pool().acquire().await.unwrap();
    rebuild_beef_for_broadcast(
        &mut conn,
        storage.writer(),
        std::slice::from_ref(&chain[1].1),
        Some(&storage),
    )
    .await
    .unwrap();

    assert_eq!(
        services.call_count("get_merkle_path"),
        0,
        "a transaction recorded seconds ago is not worth asking about"
    );
}

#[tokio::test]
async fn test_the_broadcast_rebuild_fetches_a_missing_proof_and_stores_it_through_the_writer() {
    // The wallet holds the parent but no proof for it, exactly what a
    // wallet running without its monitor looks like. The broadcast
    // rebuild (not createAction, which fetches nothing) must ask
    // services once, with no lock held, record the proof through the
    // writer (the walk itself reads on a query-only reader), and stop the
    // chain there.
    use crate::services::mock::{MockResponse, MockWalletServices};
    use crate::services::traits::{BlockHeader, GetMerklePathResult};
    use crate::storage::traits::WalletStorageProvider;

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let chain = synth_chain(3);
    // Every hop unproven, and the parent's stored BEEF still holds the
    // whole history behind it.
    let mut stored = Beef::new();
    stored.merge_raw_tx(chain[0].0.clone(), None);
    let stored_bytes = stored.to_binary();
    seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
    seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, Some(&stored_bytes)).await;
    seed_unproven_tx(&storage, &chain[2].1, &chain[2].0, None).await;
    backdate_tx(&storage, &chain[1].1, 30).await;
    // The proof LAG gate is closed on a fresh database; this test pins
    // the fetch-and-store, so the gate is open (the deferral is pinned
    // in `reorg_tests`).
    crate::storage::MonitorStorage::set_max_acceptable_proof_height(&storage, u32::MAX)
        .await
        .unwrap();

    let bump = synth_bump(965_300, &chain[1].1);
    let services = MockWalletServices::builder()
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("MockProvider".to_string()),
            merkle_path: Some(hex::encode(bump.to_binary())),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "00".repeat(32),
                merkle_root: chain[1].1.clone(),
                time: 0,
                bits: 0,
                nonce: 0,
                hash: "ab".repeat(32),
                height: 965_300,
            }),
            error: None,
            notes: vec![],
        }))
        .build();
    storage.set_services(std::sync::Arc::new(services));
    // A fetched proof is stored only after the storage's tracker
    // confirms its root (P0-1): this one knows the served block.
    let mut tracker = bsv_rs::transaction::MockChainTracker::new(965_400);
    tracker.add_root(965_300, chain[1].1.clone());
    storage
        .set_chain_tracker(std::sync::Arc::new(tracker))
        .await;

    // A reader connection: the walk never holds the writer.
    let mut conn = storage.pool().acquire().await.unwrap();

    let beef = rebuild_beef_for_broadcast(
        &mut conn,
        storage.writer(),
        std::slice::from_ref(&chain[1].1),
        Some(&storage),
    )
    .await
    .unwrap();

    let kept = beef_txids(&beef);
    assert_eq!(kept.len(), 1, "only the proven parent is needed");
    assert!(kept.contains(&chain[1].1));
    assert_eq!(beef.bumps.len(), 1, "carrying the freshly fetched BUMP");
    assert!(
        !kept.contains(&chain[0].1),
        "the parent's stored ancestry is dropped once it is proven"
    );

    let stored_proof: Option<(String,)> =
        sqlx::query_as("SELECT txid FROM proven_txs WHERE txid = ?")
            .bind(&chain[1].1)
            .fetch_optional(storage.pool())
            .await
            .unwrap();
    assert!(
        stored_proof.is_some(),
        "the proof is recorded, so the next spend pays no network cost"
    );
}

// =========================================================================
// Live-store diagnostic (ignored by default)
//
// Runs the real BEEF assembly walk against a COPY of a wallet store and
// reports the shape of the BEEF the wallet would submit.
//
//   BEEF_DIAG_DB=/path/to/copy/wallet.db \
//   BEEF_DIAG_TXIDS=<comma separated root txids> \
//   cargo test --lib beef_diag_live_store -- --ignored --nocapture
// =========================================================================

/// Report one assembled BEEF: tx count, bytes, bumps, and whether any
/// proven ancestor dragged its own parents in.
fn beef_diag_report(label: &str, beef: &mut Beef, roots: &[String], elapsed_ms: u128) {
    let bytes = beef.to_binary();
    let all: std::collections::HashSet<String> = beef.txs.iter().map(|t| t.txid()).collect();

    let mut with_bump = 0usize;
    let mut txid_only = 0usize;
    let mut violations: Vec<String> = Vec::new();

    // A tx carrying a BUMP is self-proving; none of its parents belong in
    // the BEEF unless some other unproven tx needs them.
    let mut needed_by_unproven: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for tx in &mut beef.txs {
        if tx.bump_index().is_some() || tx.is_txid_only() {
            continue;
        }
        if let Some(raw) = tx.raw_tx().map(|r| r.to_vec()) {
            if let Ok(ins) = parse_input_txids(&raw) {
                for i in ins {
                    needed_by_unproven.insert(i);
                }
            }
        }
    }

    // A root is always legitimately present: it is an input being spent.
    let root_set: std::collections::HashSet<&String> = roots.iter().collect();

    for tx in &mut beef.txs {
        if tx.is_txid_only() {
            txid_only += 1;
            continue;
        }
        if tx.bump_index().is_none() {
            continue;
        }
        with_bump += 1;
        let txid = tx.txid();
        let raw = match tx.raw_tx().map(|r| r.to_vec()) {
            Some(r) => r,
            None => continue,
        };
        if let Ok(parents) = parse_input_txids(&raw) {
            for p in parents {
                if all.contains(&p) && !needed_by_unproven.contains(&p) && !root_set.contains(&p) {
                    violations.push(format!(
                        "{}<-{}",
                        &txid[..8.min(txid.len())],
                        &p[..8.min(p.len())]
                    ));
                }
            }
        }
    }

    let validity = beef.verify_valid(true);

    println!("--------------------------------------------------------------");
    println!("BEEF DIAG [{}]", label);
    println!("  roots            : {}", roots.len());
    for r in roots {
        println!("                     {}", r);
    }
    println!("  transactions     : {}", beef.txs.len());
    println!("  with BUMP        : {}", with_bump);
    println!("  txid-only        : {}", txid_only);
    println!("  bumps            : {}", beef.bumps.len());
    println!(
        "  total bytes      : {} ({} KB)",
        bytes.len(),
        bytes.len() / 1024
    );
    println!("  assembly ms      : {}", elapsed_ms);
    println!("  structurally valid: {}", validity.valid);
    println!(
        "  proven ancestors dragging their own parents : {}",
        if violations.is_empty() {
            "none".to_string()
        } else {
            violations.join(", ")
        }
    );
    println!("--------------------------------------------------------------");
}

#[tokio::test]
#[ignore]
async fn beef_diag_live_store() {
    let db = match std::env::var("BEEF_DIAG_DB") {
        Ok(v) => v,
        Err(_) => {
            println!("set BEEF_DIAG_DB to run this diagnostic");
            return;
        }
    };
    let roots: Vec<String> = std::env::var("BEEF_DIAG_TXIDS")
        .expect("BEEF_DIAG_TXIDS required")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let storage = StorageSqlx::new(&format!("sqlite:{}", db)).await.unwrap();
    let mut conn = storage.pool().acquire().await.unwrap();

    let mut beef = Beef::new();
    let mut pending: Vec<(String, usize)> = roots.iter().map(|t| (t.clone(), 0)).collect();
    let mut processed: HashSet<String> = HashSet::new();

    // With services wired the walk fetches and records missing proofs, which
    // is what lets the chain terminate. Set BEEF_DIAG_SERVICES=1 to include
    // that path (writes land on the copy only).
    let with_services = std::env::var("BEEF_DIAG_SERVICES").is_ok();
    if with_services {
        // Arcade is the default mode and the defaults name no endpoint
        // (#317): this opt-in diagnostic names the crate's mainnet one.
        let options = crate::services::ServicesOptions::mainnet()
            .with_arcade(crate::services::ARCADE_V2_MAINNET, None);
        let services =
            crate::services::Services::with_options(crate::Chain::Main, options).unwrap();
        crate::storage::traits::WalletStorageProvider::set_services(
            &storage,
            std::sync::Arc::new(services),
        );
    }
    let storage_ref = if with_services { Some(&storage) } else { None };

    let t0 = std::time::Instant::now();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        storage_ref,
        None,
    )
    .await
    .unwrap();
    let walk_ms = t0.elapsed().as_millis();

    beef_diag_report("after walk, before prune", &mut beef, &roots, walk_ms);

    let t1 = std::time::Instant::now();
    prune_beef_to_roots(&mut beef, &roots);
    let prune_ms = t1.elapsed().as_millis();

    beef_diag_report(
        "final (walk + prune)",
        &mut beef,
        &roots,
        walk_ms + prune_ms,
    );
}
