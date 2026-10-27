use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bsv_rs::primitives::sha256;
use bsv_rs::transaction::MerklePath;
use bsv_wallet_toolbox_rs::benchmark_support::local_explorer_services;
use bsv_wallet_toolbox_rs::{Services, ServicesOptions, UtxoVerdict, WalletServices};
use criterion::Criterion;
use mockito::{Matcher, Mock, Server};
use tokio::runtime::Runtime;

use crate::common::{funding_transaction, HEIGHT};
use crate::measurement::measure;

#[derive(Clone, Copy, PartialEq)]
enum Answer {
    Unspent,
    Spent,
    SpentHint,
    Unknown,
    Raw200,
    Raw404,
}

struct Fixture {
    services: Services,
    txid: String,
    script: Vec<u8>,
    requests: Arc<AtomicU64>,
    unexpected: Mock,
    post: Mock,
    _routes: Vec<Mock>,
    _server: Server,
}

async fn route(
    server: &mut Server,
    requests: &Arc<AtomicU64>,
    path: String,
    status: usize,
    body: String,
) -> Mock {
    let requests = Arc::clone(requests);
    server
        .mock("GET", path.as_str())
        .with_status(status)
        .with_header("content-type", "application/json")
        .with_body_from_request(move |_| {
            requests.fetch_add(1, Ordering::Relaxed);
            body.clone().into_bytes()
        })
        .expect_at_least(0)
        .create_async()
        .await
}

impl Fixture {
    async fn new(answer: Answer) -> Self {
        let mut server = Server::new_with_opts_async(mockito::ServerOpts::default()).await;
        let unexpected = server
            .mock("GET", Matcher::Any)
            .with_status(501)
            .expect(0)
            .create_async()
            .await;
        let post = server
            .mock("POST", Matcher::Any)
            .with_status(501)
            .expect(0)
            .create_async()
            .await;
        let requests = Arc::new(AtomicU64::new(0));
        let script = vec![0x51];
        let txid = "11".repeat(32);
        let mut spender = funding_transaction(123, &script);
        spender.inputs[0].source_txid = Some(txid.clone());
        spender.inputs[0].source_output_index = 0;
        let spender_id = spender.id();
        let mut hash = sha256(&script);
        hash.reverse();
        let hash = hex::encode(hash);
        let unspent = answer == Answer::Unspent;
        let mut routes = Vec::new();
        let woc_body = serde_json::json!({
            "script": hash,
            "result": if unspent { vec![serde_json::json!({
                "height": HEIGHT, "tx_pos": 0, "tx_hash": txid, "value": 100_000
            })] } else { vec![] }
        })
        .to_string();
        let bitails_body = serde_json::json!({
            "scripthash": hash,
            "unspent": if unspent { vec![serde_json::json!({
                "blockheight": HEIGHT, "vout": 0, "txid": txid, "satoshis": 100_000
            })] } else { vec![] }
        })
        .to_string();
        let bb_body = serde_json::json!({
            "value": 100_000, "index": 0, "script_pubkey": hex::encode(&script),
            "script_type": "nonstandard", "spent": !unspent,
            "spent_txid": if unspent { None } else { Some(&spender_id) },
            "spend_scope": "confirmed"
        })
        .to_string();
        let status = if answer == Answer::Unknown { 401 } else { 200 };
        for (path, body) in [
            (format!("/woc/script/{hash}/unspent/all"), woc_body),
            (format!("/bb/txo/{txid}/0"), bb_body),
            (format!("/bitails/scripthash/{hash}/unspent"), bitails_body),
        ] {
            routes.push(route(&mut server, &requests, path, status, body).await);
        }
        let has_spender = matches!(answer, Answer::Spent | Answer::SpentHint);
        routes.push(
            route(
                &mut server,
                &requests,
                format!("/woc/tx/{txid}/0/spent"),
                if has_spender { 200 } else { 404 },
                serde_json::json!({"txid": spender_id, "vin": 0}).to_string(),
            )
            .await,
        );
        for provider in ["woc", "bb", "bitails"] {
            let raw_status = if answer == Answer::Raw404 { 404 } else { 200 };
            routes.push(
                route(
                    &mut server,
                    &requests,
                    format!("/{provider}/tx/{spender_id}/hex"),
                    raw_status,
                    spender.to_hex(),
                )
                .await,
            );
        }

        let block_hash = "ab".repeat(32);
        let tsc = serde_json::json!({
            "index": 0, "txOrId": spender_id, "target": block_hash, "nodes": ["22".repeat(32)]
        })
        .to_string();
        let bump = bsv_wallet_toolbox_rs::tsc_proof::tsc_json_to_bump_hex(&tsc, HEIGHT)
            .expect("TSC fixture");
        let root = MerklePath::from_hex(&bump)
            .expect("BUMP")
            .compute_root(Some(&spender_id))
            .expect("root");
        routes.push(
            route(
                &mut server,
                &requests,
                format!("/woc/tx/{spender_id}/proof/tsc"),
                if answer == Answer::Spent { 200 } else { 404 },
                tsc,
            )
            .await,
        );
        routes.push(
            route(
                &mut server,
                &requests,
                format!("/bitails/tx/{spender_id}/proof/tsc"),
                404,
                String::new(),
            )
            .await,
        );
        routes.push(
            route(
                &mut server,
                &requests,
                format!("/bb/tx/{spender_id}"),
                404,
                String::new(),
            )
            .await,
        );
        let header = serde_json::json!({"status": "success", "value": {
            "version": 1, "previousHash": "00".repeat(32), "merkleRoot": root,
            "time": 1_700_000_000, "bits": 486_604_799, "nonce": 1,
            "height": HEIGHT, "hash": block_hash
        }})
        .to_string();
        for path in [
            format!("/headers/findHeaderHexForBlockHash?hash={block_hash}"),
            format!("/headers/findHeaderHexForHeight?height={HEIGHT}"),
        ] {
            routes.push(route(&mut server, &requests, path, 200, header.clone()).await);
        }
        let base = server.url();
        let services = local_explorer_services(
            // Arcade, the default mode (#317), at a loopback port nothing
            // listens on: `is_utxo` asks no broadcaster.
            ServicesOptions::mainnet()
                .with_arcade("http://127.0.0.1:9", None)
                .with_chaintracks_url(format!("{base}/headers")),
            &format!("{base}/woc"),
            &format!("{base}/bb"),
            &format!("{base}/bitails"),
        )
        .expect("local service ladders");
        Self {
            services,
            txid: if matches!(answer, Answer::Raw200 | Answer::Raw404) {
                spender_id
            } else {
                txid
            },
            script,
            requests,
            unexpected,
            post,
            _routes: routes,
            _server: server,
        }
    }
}

pub fn benchmarks(criterion: &mut Criterion, directory: &Path, runtime: &Runtime) {
    for (name, answer) in [
        ("is_utxo_unspent", Answer::Unspent),
        ("is_utxo_spent", Answer::Spent),
        ("is_utxo_spent_hint", Answer::SpentHint),
        ("is_utxo_could_not_look", Answer::Unknown),
        ("get_raw_tx_200", Answer::Raw200),
        ("get_raw_tx_404", Answer::Raw404),
    ] {
        let fixture = runtime.block_on(Fixture::new(answer));
        let mut counts = BTreeSet::new();
        measure(criterion, directory, name, || {
            runtime.block_on(async {
                fixture.requests.store(0, Ordering::Relaxed);
                let started = Instant::now();
                let elapsed = if matches!(answer, Answer::Raw200 | Answer::Raw404) {
                    let result = fixture
                        .services
                        .get_raw_tx(&fixture.txid, false)
                        .await
                        .expect("raw ladder");
                    let elapsed = started.elapsed();
                    if answer == Answer::Raw200 {
                        assert!(result.raw_tx.is_some());
                    } else {
                        assert!(result.is_not_found());
                    }
                    elapsed
                } else {
                    let verdict = fixture
                        .services
                        .is_utxo(&fixture.txid, 0, &fixture.script)
                        .await;
                    let elapsed = started.elapsed();
                    assert_eq!(
                        verdict,
                        match answer {
                            Answer::Unspent => UtxoVerdict::UnspentHint,
                            Answer::Spent => UtxoVerdict::Spent,
                            Answer::SpentHint => UtxoVerdict::SpentHint,
                            _ => UtxoVerdict::Unknown,
                        }
                    );
                    elapsed
                };
                let requests = fixture.requests.load(Ordering::Relaxed);
                let expected: &[u64] = match answer {
                    Answer::Unspent => &[3, 4],
                    Answer::Spent => &[6, 7],
                    Answer::SpentHint => &[7],
                    Answer::Unknown => &[5],
                    Answer::Raw200 => &[1],
                    Answer::Raw404 => &[3],
                };
                assert!(
                    expected.contains(&requests),
                    "{name}: {requests} HTTP requests"
                );
                counts.insert(requests);
                fixture
                    .services
                    .get_services_call_history(true)
                    .expect("clear history");
                elapsed
            })
        });
        runtime.block_on(async {
            fixture.unexpected.assert_async().await;
            fixture.post.assert_async().await;
        });
        if !counts.is_empty() {
            println!("{name}: HTTP requests per call {counts:?}; unexpected requests 0; POSTs 0");
        }
    }
}
