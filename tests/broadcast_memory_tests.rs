//! Broadcast acceptance memory (0.3.56): reduced sends per provider, the
//! one-shot full-package fallback, the sticky provider order and the
//! transient ARC 469. All synthetic: mockito servers, no network, no real
//! keys.

use std::collections::HashSet;
use std::sync::Arc as StdArc;

use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, Transaction, TransactionInput, TransactionOutput, BEEF_V1,
};
use bsv_wallet_toolbox_rs::services::{
    beef_to_ef_batch, beef_to_ef_batch_skipping, Arc as ArcProvider, ArcConfig, Arcade,
    ArcadeConfig, BroadcastMemory, InMemoryBroadcastMemory, Services, ServicesOptions,
    WalletServices, BROADCAST_STATUS_ACCEPTED, BROADCAST_STATUS_SEEN, PREF_LAST_ACCEPTED_PROVIDER,
    PROVIDER_GORILLAPOOL_ARC, PROVIDER_TAAL_ARC,
};
use bsv_wallet_toolbox_rs::{classify_broadcast_results, BroadcastOutcome, Chain};

/// P2PKH-style locking script used for synthetic outputs.
const LOCK_HEX: &str = "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac";

/// Hex prefix of an EF-encoded version-1 transaction (BRC-30 marker).
const EF_HEX_PREFIX: &str = "010000000000000000ef";

/// Hex prefix of a V1 BEEF.
const BEEF_HEX_PREFIX: &str = "0100beef";

fn synthetic_tx(source_txid: Option<String>, source_vout: u32, satoshis: u64) -> Transaction {
    Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some(source_txid.unwrap_or_else(|| "00".repeat(32))),
            source_output_index: source_vout,
            unlocking_script: Some(UnlockingScript::from_hex("00").unwrap()),
            unlocking_script_template: None,
            sequence: 0xFFFFFFFF,
        }],
        vec![TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: LockingScript::from_hex(LOCK_HEX).unwrap(),
            change: false,
        }],
        0,
    )
}

/// A four-level chain: `root` (mined, BUMP) -> `a` -> `b` -> `subject`, the
/// last three unproven. This is the shape of a wallet's change chain.
struct ChainBeef {
    beef: Vec<u8>,
    a: String,
    b: String,
    subject: String,
}

fn build_chain() -> ChainBeef {
    let root_tx = synthetic_tx(None, 0xFFFFFFFF, 4_000_000);
    let root = root_tx.id();
    let a_tx = synthetic_tx(Some(root.clone()), 0, 3_000_000);
    let a = a_tx.id();
    let b_tx = synthetic_tx(Some(a.clone()), 0, 2_000_000);
    let b = b_tx.id();
    let subject_tx = synthetic_tx(Some(b.clone()), 0, 1_000_000);
    let subject = subject_tx.id();

    let bump = MerklePath::from_coinbase_txid(&root, 800_000);
    let mut beef = Beef::with_version(BEEF_V1);
    let bump_idx = beef.merge_bump(bump);
    beef.merge_raw_tx(root_tx.to_binary(), Some(bump_idx));
    beef.merge_transaction(a_tx);
    beef.merge_transaction(b_tx);
    beef.merge_transaction(subject_tx);

    ChainBeef {
        beef: beef.to_binary(),
        a,
        b,
        subject,
    }
}

fn set(txids: &[&String]) -> HashSet<String> {
    txids.iter().map(|t| (*t).clone()).collect()
}

fn entry_txids(batch: &bsv_wallet_toolbox_rs::EfBatch) -> Vec<String> {
    batch.entries.iter().map(|e| e.txid.clone()).collect()
}

fn ef_of(batch: &bsv_wallet_toolbox_rs::EfBatch, txid: &str) -> Vec<u8> {
    batch
        .entries
        .iter()
        .find(|e| e.txid == txid)
        .map(|e| e.ef.clone())
        .expect("entry present")
}

fn arc_response(txid: &str, tx_status: &str) -> String {
    format!(
        r#"{{"txid":"{}","txStatus":"{}","extraInfo":""}}"#,
        txid, tx_status
    )
}

// =============================================================================
// beef_to_ef_batch_skipping
// =============================================================================

mod ef_filtering {
    use super::*;

    #[test]
    fn skip_set_filters_ancestors_but_never_the_subject() {
        let c = build_chain();

        // No skip: every unproven tx, parents first.
        let full = beef_to_ef_batch_skipping(&c.beef, &HashSet::new()).unwrap();
        assert_eq!(
            entry_txids(&full),
            vec![c.a.clone(), c.b.clone(), c.subject.clone()]
        );
        assert!(full.skipped.is_empty());
        assert_eq!(full.subject_txid, c.subject);
        // The old API is the same batch.
        let (efs, subject) = beef_to_ef_batch(&c.beef).unwrap();
        assert_eq!(subject, c.subject);
        assert_eq!(
            efs,
            full.entries
                .iter()
                .map(|e| e.ef.clone())
                .collect::<Vec<_>>()
        );

        // Skip `a`: `b` is still emitted and still links its input to `a`
        // (a's output value is embedded in b's EF), because the skip does not
        // touch the source map.
        let skip_a = beef_to_ef_batch_skipping(&c.beef, &set(&[&c.a])).unwrap();
        assert_eq!(entry_txids(&skip_a), vec![c.b.clone(), c.subject.clone()]);
        assert_eq!(skip_a.skipped, vec![c.a.clone()]);
        let a_sats = 3_000_000u64.to_le_bytes();
        assert!(
            ef_of(&skip_a, &c.b).windows(8).any(|w| w == a_sats),
            "b's EF must still embed a's output (linking survives the skip)"
        );

        // Skip both ancestors: the subject alone.
        let skip_ab = beef_to_ef_batch_skipping(&c.beef, &set(&[&c.a, &c.b])).unwrap();
        assert_eq!(entry_txids(&skip_ab), vec![c.subject.clone()]);
        assert_eq!(skip_ab.skipped, vec![c.a.clone(), c.b.clone()]);

        // The subject is emitted even when it is in the skip set.
        let skip_all = beef_to_ef_batch_skipping(&c.beef, &set(&[&c.a, &c.b, &c.subject])).unwrap();
        assert_eq!(entry_txids(&skip_all), vec![c.subject.clone()]);
        assert_eq!(skip_all.skipped, vec![c.a.clone(), c.b.clone()]);

        // A skip set naming unrelated txids changes nothing.
        let other = beef_to_ef_batch_skipping(&c.beef, &set(&[&"dd".repeat(32)])).unwrap();
        assert_eq!(entry_txids(&other), entry_txids(&full));
    }
}

// =============================================================================
// Arcade: reduced batch + full-batch fallback (mockito)
// =============================================================================

mod arcade_reduced {
    use super::*;

    #[tokio::test]
    async fn reduced_batch_refused_missing_parent_is_retried_once_with_the_full_batch() {
        let c = build_chain();
        let seen = set(&[&c.a, &c.b]);
        let reduced = beef_to_ef_batch_skipping(&c.beef, &seen).unwrap();
        assert_eq!(reduced.entries.len(), 1, "subject alone");
        let subject_ef = reduced.entries[0].ef.clone();
        let full = beef_to_ef_batch_skipping(&c.beef, &HashSet::new()).unwrap();
        let full_body: Vec<u8> = full.entries.iter().flat_map(|e| e.ef.clone()).collect();

        let mut server = mockito::Server::new_async().await;
        // The reduced send: one EF to /tx, refused as a missing parent.
        let refused = server
            .mock("POST", "/tx")
            .match_body(subject_ef.clone())
            .with_status(422)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":422,"title":"Unprocessable","detail":"failed to validate transaction: missing parent inputs"}"#)
            .expect(1)
            .create_async()
            .await;
        // The fallback: the full batch to /txs, accepted.
        let accepted = server
            .mock("POST", "/txs")
            .match_body(full_body.clone())
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":2,"submitted":1,"total":3}"#)
            .expect(1)
            .create_async()
            .await;

        let arcade = Arcade::new(
            server.url(),
            Some(ArcadeConfig::with_callback_token("tok")),
            None,
        )
        .unwrap();
        let (result, delivery) = arcade
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();

        refused.assert_async().await;
        accepted.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(delivery.reduced);
        assert!(delivery.fallback_full);
        assert_eq!(delivery.bytes_sent, subject_ef.len() + full_body.len());
        assert_eq!(
            delivery.accepted_txids,
            vec![c.a.clone(), c.b.clone(), c.subject.clone()],
            "the full batch handed Arcade every unproven tx"
        );
        assert!(result
            .notes
            .iter()
            .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefFallbackFull")));
    }

    #[tokio::test]
    async fn reduced_batch_accepted_reports_only_what_was_sent() {
        let c = build_chain();
        let seen = set(&[&c.a]);
        let reduced = beef_to_ef_batch_skipping(&c.beef, &seen).unwrap();
        assert_eq!(entry_txids(&reduced), vec![c.b.clone(), c.subject.clone()]);
        let body: Vec<u8> = reduced.entries.iter().flat_map(|e| e.ef.clone()).collect();

        let mut server = mockito::Server::new_async().await;
        let batch = server
            .mock("POST", "/txs")
            .match_body(body.clone())
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":0,"submitted":2,"total":2}"#)
            .expect(1)
            .create_async()
            .await;
        let single = server.mock("POST", "/tx").expect(0).create_async().await;

        let arcade = Arcade::new(server.url(), None, None).unwrap();
        let (result, delivery) = arcade
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();

        batch.assert_async().await;
        single.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(delivery.reduced);
        assert!(!delivery.fallback_full);
        assert_eq!(delivery.bytes_sent, body.len());
        assert_eq!(
            delivery.accepted_txids,
            vec![c.b.clone(), c.subject.clone()]
        );
    }

    #[tokio::test]
    async fn reduced_send_rejected_for_fees_is_definitive_and_not_retried() {
        let c = build_chain();
        let seen = set(&[&c.a, &c.b]);

        let mut server = mockito::Server::new_async().await;
        let single = server
            .mock("POST", "/tx")
            .with_status(465)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":465,"title":"Fee too low","detail":"The fees are too low"}"#)
            .expect(1)
            .create_async()
            .await;
        let batch = server.mock("POST", "/txs").expect(0).create_async().await;

        let arcade = Arcade::new(server.url(), None, None).unwrap();
        let (result, delivery) = arcade
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();

        single.assert_async().await;
        batch.assert_async().await;
        assert_eq!(result.status, "error");
        assert_eq!(result.txid_results[0].status, "465");
        assert!(delivery.reduced);
        assert!(
            !delivery.fallback_full,
            "a fee verdict is not a missing parent"
        );
        assert!(delivery.accepted_txids.is_empty());
    }

    #[tokio::test]
    async fn full_send_without_seen_set_is_unchanged() {
        let c = build_chain();
        let full = beef_to_ef_batch_skipping(&c.beef, &HashSet::new()).unwrap();
        let body: Vec<u8> = full.entries.iter().flat_map(|e| e.ef.clone()).collect();

        let mut server = mockito::Server::new_async().await;
        let batch = server
            .mock("POST", "/txs")
            .match_body(body.clone())
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":0,"submitted":3,"total":3}"#)
            .expect(1)
            .create_async()
            .await;

        let arcade = Arcade::new(server.url(), None, None).unwrap();
        let result = arcade
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        batch.assert_async().await;
        assert_eq!(result.status, "success");
    }
}

// =============================================================================
// Classic ARC: decision table end to end (mockito)
// =============================================================================

mod arc_reduced {
    use super::*;

    fn arc(server: &mockito::ServerGuard) -> ArcProvider {
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap()
    }

    fn has_note(result: &bsv_wallet_toolbox_rs::PostBeefResult, what: &str) -> bool {
        result
            .notes
            .iter()
            .any(|n| n.get("what").and_then(|v| v.as_str()) == Some(what))
    }

    #[tokio::test]
    async fn all_seen_sends_the_subject_as_ef_to_v1_tx() {
        let c = build_chain();
        let seen = set(&[&c.a, &c.b]);
        let subject_ef = ef_of(
            &beef_to_ef_batch_skipping(&c.beef, &seen).unwrap(),
            &c.subject,
        );

        let mut server = mockito::Server::new_async().await;
        let ef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}""#,
                hex::encode(&subject_ef)
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;
        let batch_mock = server
            .mock("POST", "/v1/txs")
            .expect(0)
            .create_async()
            .await;

        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();

        ef_mock.assert_async().await;
        batch_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(delivery.reduced);
        assert!(!delivery.fallback_full);
        assert_eq!(delivery.bytes_sent, subject_ef.len());
        assert_eq!(delivery.accepted_txids, vec![c.subject.clone()]);
        assert!(has_note(&result, "postBeefReducedEF"));
    }

    #[tokio::test]
    async fn partially_seen_sends_a_json_ef_batch_to_v1_txs() {
        let c = build_chain();
        let seen = set(&[&c.a]);
        let batch = beef_to_ef_batch_skipping(&c.beef, &seen).unwrap();
        let expected_body = serde_json::to_string(
            &batch
                .entries
                .iter()
                .map(|e| serde_json::json!({ "rawTx": hex::encode(&e.ef) }))
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let mut server = mockito::Server::new_async().await;
        let batch_mock = server
            .mock("POST", "/v1/txs")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::Exact(expected_body))
            .with_status(200)
            .with_header("content-type", "application/json")
            // ARC answers a bare array mixing responses.
            .with_body(format!(
                r#"[{{"txid":"{}","txStatus":"SEEN_ON_NETWORK","status":200,"title":"OK"}},{{"txid":"{}","txStatus":"RECEIVED","status":200,"title":"OK"}}]"#,
                c.b, c.subject
            ))
            .expect(1)
            .create_async()
            .await;
        let single_mock = server.mock("POST", "/v1/tx").expect(0).create_async().await;

        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();

        batch_mock.assert_async().await;
        single_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert_eq!(result.txid_results[0].txid, c.subject);
        assert!(delivery.reduced);
        assert!(!delivery.fallback_full);
        assert_eq!(
            delivery.bytes_sent,
            batch.entries.iter().map(|e| e.ef.len()).sum::<usize>()
        );
        assert_eq!(
            delivery.accepted_txids,
            vec![c.b.clone(), c.subject.clone()]
        );
        assert!(has_note(&result, "postBeefReducedBatch"));
    }

    #[tokio::test]
    async fn batch_answer_in_the_openapi_wrapper_shape_is_accepted_too() {
        let c = build_chain();
        let seen = set(&[&c.a]);

        let mut server = mockito::Server::new_async().await;
        let batch_mock = server
            .mock("POST", "/v1/txs")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"transactions":[{{"txid":"{}","txStatus":"SEEN_ON_NETWORK"}},{{"txid":"{}","txStatus":"SEEN_ON_NETWORK"}}]}}"#,
                c.b, c.subject
            ))
            .expect(1)
            .create_async()
            .await;

        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();
        batch_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert_eq!(
            delivery.accepted_txids,
            vec![c.b.clone(), c.subject.clone()]
        );
    }

    #[tokio::test]
    async fn none_seen_sends_the_full_beef_and_records_the_whole_package() {
        let c = build_chain();

        let mut server = mockito::Server::new_async().await;
        let beef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "ANNOUNCED_TO_NETWORK"))
            .expect(1)
            .create_async()
            .await;
        let batch_mock = server
            .mock("POST", "/v1/txs")
            .expect(0)
            .create_async()
            .await;

        // An unrelated seen txid is "none seen" too.
        let seen = set(&[&"dd".repeat(32)]);
        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();

        beef_mock.assert_async().await;
        batch_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(!delivery.reduced);
        assert!(!delivery.fallback_full);
        assert_eq!(delivery.bytes_sent, c.beef.len());
        // ARC submits every RawTx-format ancestor of a BEEF, not just the subject.
        assert_eq!(
            delivery.accepted_txids,
            vec![c.subject.clone(), c.a.clone(), c.b.clone()]
        );
        assert!(has_note(&result, "postBeefFull"));

        // And the plain post_beef API is exactly that path.
        let beef_mock2 = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;
        let result = arc(&server)
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        beef_mock2.assert_async().await;
        assert_eq!(result.status, "success");
    }

    #[tokio::test]
    async fn reduced_ef_refused_for_inputs_falls_back_to_the_full_beef_once() {
        let c = build_chain();
        let seen = set(&[&c.a, &c.b]);

        let mut server = mockito::Server::new_async().await;
        let ef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                EF_HEX_PREFIX
            )))
            .with_status(462)
            .with_header("content-type", "application/json")
            .with_body(r#"{"type":"https://arc.bitcoinsv.com/errors/462","title":"Invalid inputs","status":462,"detail":"Transaction is invalid because the inputs are non-existent or spent"}"#)
            .expect(1)
            .create_async()
            .await;
        let beef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;

        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();

        ef_mock.assert_async().await;
        beef_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(delivery.reduced);
        assert!(delivery.fallback_full);
        assert_eq!(
            delivery.accepted_txids,
            vec![c.subject.clone(), c.a.clone(), c.b.clone()]
        );
        assert!(has_note(&result, "postBeefFallbackFull"));
    }

    #[tokio::test]
    async fn reduced_ef_orphan_verdict_falls_back_to_the_full_beef() {
        let c = build_chain();
        let seen = set(&[&c.a, &c.b]);

        let mut server = mockito::Server::new_async().await;
        let ef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                EF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_IN_ORPHAN_MEMPOOL"))
            .expect(1)
            .create_async()
            .await;
        let beef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;

        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();
        ef_mock.assert_async().await;
        beef_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(delivery.fallback_full);
    }

    #[tokio::test]
    async fn batch_with_a_rejected_ancestor_falls_back_to_the_full_beef() {
        let c = build_chain();
        let seen = set(&[&c.a]);

        let mut server = mockito::Server::new_async().await;
        let batch_mock = server
            .mock("POST", "/v1/txs")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"[{{"type":"https://arc.bitcoinsv.com/errors/465","title":"Fee too low","status":465,"detail":"The fees are too low","txid":"{}"}},{{"txid":"{}","txStatus":"RECEIVED"}}]"#,
                c.b, c.subject
            ))
            .expect(1)
            .create_async()
            .await;
        let beef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;

        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();
        batch_mock.assert_async().await;
        beef_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(delivery.reduced);
        assert!(delivery.fallback_full);
    }

    #[tokio::test]
    async fn batch_endpoint_not_served_falls_back_to_the_full_beef() {
        let c = build_chain();
        let seen = set(&[&c.a]);

        let mut server = mockito::Server::new_async().await;
        let batch_mock = server
            .mock("POST", "/v1/txs")
            .with_status(404)
            .with_body("404 page not found")
            .expect(1)
            .create_async()
            .await;
        let beef_mock = server
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;

        let (result, delivery) = arc(&server)
            .post_beef_seen(&c.beef, std::slice::from_ref(&c.subject), &seen)
            .await
            .unwrap();
        batch_mock.assert_async().await;
        beef_mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(delivery.fallback_full);
    }

    #[tokio::test]
    async fn transient_469_timeout_is_a_service_error_end_to_end() {
        let c = build_chain();

        let mut server = mockito::Server::new_async().await;
        let timeout_mock = server
            .mock("POST", "/v1/tx")
            .with_status(469)
            .with_header("content-type", "application/json")
            .with_body(r#"{"type":"https://arc.bitcoinsv.com/errors/469","title":"Merkle Roots validation failed","status":469,"detail":"BEEF validation failed: couldn't verify Merkle Roots: BEEF verification timed out"}"#)
            .expect(1)
            .create_async()
            .await;

        let result = arc(&server)
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        timeout_mock.assert_async().await;
        assert_eq!(result.status, "error");
        let tr = &result.txid_results[0];
        assert!(tr.service_error, "provider-side timeout is transient");
        assert_eq!(tr.status, "error");
        let outcome = classify_broadcast_results(std::slice::from_ref(&result));
        assert!(
            matches!(outcome, BroadcastOutcome::ServiceError { .. }),
            "got {:?}",
            outcome
        );
        assert!(outcome.is_transient());

        // A real 469 verdict stays definitive.
        let mut server2 = mockito::Server::new_async().await;
        let _verdict_mock = server2
            .mock("POST", "/v1/tx")
            .with_status(469)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":469,"title":"Merkle Roots validation failed","detail":"BEEF validation failed: merkle root mismatch"}"#)
            .create_async()
            .await;
        let result = arc(&server2)
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        let tr = &result.txid_results[0];
        assert!(!tr.service_error);
        assert_eq!(tr.status, "469");
        let outcome = classify_broadcast_results(std::slice::from_ref(&result));
        assert!(
            matches!(outcome, BroadcastOutcome::InvalidTx { .. }),
            "got {:?}",
            outcome
        );
    }
}

// =============================================================================
// Services: memory wiring, recording, sticky order (mockito)
// =============================================================================

mod services_memory {
    use super::*;

    fn two_arc_services(taal_url: &str, gp_url: &str) -> Services {
        let options = ServicesOptions::mainnet()
            .with_arc(taal_url, None)
            .with_gorillapool(gp_url, None);
        Services::with_options(Chain::Main, options).unwrap()
    }

    #[tokio::test]
    async fn without_a_memory_the_full_package_goes_out_in_static_order() {
        let c = build_chain();
        let mut taal = mockito::Server::new_async().await;
        let mut gp = mockito::Server::new_async().await;
        let taal_mock = taal
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;
        let gp_mock = gp.mock("POST", "/v1/tx").expect(0).create_async().await;

        let services = two_arc_services(&taal.url(), &gp.url());
        assert!(services.broadcast_memory().is_none());
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        taal_mock.assert_async().await;
        gp_mock.assert_async().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_success());
    }

    #[tokio::test]
    async fn sticky_provider_is_tried_first_on_the_next_instance_and_seen_ancestors_are_skipped() {
        let c = build_chain();
        let memory = StdArc::new(InMemoryBroadcastMemory::new());

        let mut taal = mockito::Server::new_async().await;
        let mut gp = mockito::Server::new_async().await;
        // TAAL rejects definitively (no in-memory demotion for that), so only
        // the persisted sticky choice can keep it from being tried again.
        let taal_mock = taal
            .mock("POST", "/v1/tx")
            .with_status(465)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":465,"title":"Fee too low","detail":"The fees are too low"}"#)
            .expect(1)
            .create_async()
            .await;
        // GorillaPool accepts the full BEEF the first two times (the second
        // instance holds nothing but acceptances, which never skip)...
        let gp_full = gp
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                BEEF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(2)
            .create_async()
            .await;
        // ...and the subject alone as EF once the ancestors are SEEN.
        let gp_ef = gp
            .mock("POST", "/v1/tx")
            .match_body(mockito::Matcher::Regex(format!(
                r#""rawTx":"{}"#,
                EF_HEX_PREFIX
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(arc_response(&c.subject, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;

        // First instance: static order, TAAL first.
        let services = two_arc_services(&taal.url(), &gp.url());
        services.set_broadcast_memory(memory.clone());
        assert!(services.broadcast_memory().is_some());
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        assert_eq!(results.len(), 2, "TAAL rejected, GorillaPool accepted");
        assert_eq!(results[0].name, "arcTaal");
        assert_eq!(results[1].name, "arcGorillaPool");
        assert!(classify_broadcast_results(&results).is_success());

        // The acceptance was remembered: sticky + every tx of the package.
        assert_eq!(
            memory
                .get_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER)
                .await
                .unwrap()
                .as_deref(),
            Some(PROVIDER_GORILLAPOOL_ARC)
        );
        for txid in [&c.subject, &c.a, &c.b] {
            assert_eq!(
                memory.status_of(txid, PROVIDER_GORILLAPOOL_ARC).as_deref(),
                Some(BROADCAST_STATUS_ACCEPTED),
                "{} recorded for GorillaPool",
                txid
            );
            assert!(memory.status_of(txid, PROVIDER_TAAL_ARC).is_none());
        }

        // Second instance (a restart): same memory, fresh static order.
        // GorillaPool goes first, but an acceptance is not network
        // evidence (2026-09-02): the full package goes out again.
        let services2 = two_arc_services(&taal.url(), &gp.url());
        services2.set_broadcast_memory(memory.clone());
        let results = services2
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        assert_eq!(results.len(), 1, "the sticky provider accepted first");
        assert_eq!(results[0].name, "arcGorillaPool");
        assert!(
            results[0]
                .notes
                .iter()
                .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefFull")),
            "accepted alone never skips: {:?}",
            results[0].notes
        );

        // The network vouches for the ancestors (a push verdict, a presence
        // probe): now the subject alone goes out as EF.
        for txid in [&c.a, &c.b] {
            memory
                .record_broadcast_status(txid, PROVIDER_GORILLAPOOL_ARC, BROADCAST_STATUS_SEEN)
                .await
                .unwrap();
        }
        let services3 = two_arc_services(&taal.url(), &gp.url());
        services3.set_broadcast_memory(memory.clone());
        let results = services3
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "arcGorillaPool");
        assert!(results[0]
            .notes
            .iter()
            .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefReducedEF")));

        taal_mock.assert_async().await;
        gp_full.assert_async().await;
        gp_ef.assert_async().await;
    }

    #[tokio::test]
    async fn a_configured_arcade_stays_ahead_of_the_sticky_provider() {
        let c = build_chain();
        let memory = StdArc::new(InMemoryBroadcastMemory::new());
        memory
            .set_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_GORILLAPOOL_ARC)
            .await
            .unwrap();

        let mut arcade = mockito::Server::new_async().await;
        let mut gp = mockito::Server::new_async().await;
        let arcade_mock = arcade
            .mock("POST", "/txs")
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":0,"submitted":3,"total":3}"#)
            .expect(1)
            .create_async()
            .await;
        let gp_mock = gp.mock("POST", "/v1/tx").expect(0).create_async().await;

        let options = ServicesOptions::mainnet()
            .with_arcade(arcade.url(), None)
            .with_gorillapool(gp.url(), None);
        let services = Services::with_options(Chain::Main, options).unwrap();
        services.set_broadcast_memory(memory.clone());

        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        arcade_mock.assert_async().await;
        gp_mock.assert_async().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "ArcadeV2");
        // Arcade is the sticky provider now.
        assert_eq!(
            memory
                .get_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER)
                .await
                .unwrap()
                .as_deref(),
            Some("ArcadeV2")
        );
        for txid in [&c.a, &c.b, &c.subject] {
            assert_eq!(
                memory.status_of(txid, "ArcadeV2").as_deref(),
                Some(BROADCAST_STATUS_ACCEPTED)
            );
        }
    }
}

// =============================================================================
// Arcade: a stale `seen` is re-checked with GET /tx/{oldest} (mockito)
// =============================================================================

mod arcade_stale_probe {
    use super::*;
    use bsv_wallet_toolbox_rs::services::{
        BROADCAST_SEEN_STALE_SECS, BROADCAST_STATUS_REJECTED, BROADCAST_STATUS_UNKNOWN,
        PROVIDER_ARCADE_V2,
    };

    fn arcade_services(url: &str) -> Services {
        let options = ServicesOptions::mainnet().with_arcade(url, None);
        Services::with_options(Chain::Main, options).unwrap()
    }

    /// `a` and `b` SEEN by Arcade `age_secs` ago (`a` a minute older, so it
    /// is the oldest skipped ancestor).
    fn memory_seen(c: &ChainBeef, age_secs: i64) -> StdArc<InMemoryBroadcastMemory> {
        let memory = StdArc::new(InMemoryBroadcastMemory::new());
        let at = chrono::Utc::now() - chrono::Duration::seconds(age_secs);
        memory
            .record_broadcast_status_at(&c.a, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, at)
            .unwrap();
        memory
            .record_broadcast_status_at(
                &c.b,
                PROVIDER_ARCADE_V2,
                BROADCAST_STATUS_SEEN,
                at + chrono::Duration::seconds(60),
            )
            .unwrap();
        memory
    }

    fn tx_info(txid: &str, status: &str) -> String {
        format!(r#"{{"txid":"{}","txStatus":"{}"}}"#, txid, status)
    }

    fn submit_accepted(txid: &str) -> String {
        format!(r#"{{"txid":"{}","txStatus":"RECEIVED"}}"#, txid)
    }

    fn full_body(c: &ChainBeef) -> Vec<u8> {
        beef_to_ef_batch_skipping(&c.beef, &HashSet::new())
            .unwrap()
            .entries
            .iter()
            .flat_map(|e| e.ef.clone())
            .collect()
    }

    fn subject_ef(c: &ChainBeef) -> Vec<u8> {
        ef_of(
            &beef_to_ef_batch_skipping(&c.beef, &set(&[&c.a, &c.b])).unwrap(),
            &c.subject,
        )
    }

    #[tokio::test]
    async fn fresh_seen_skips_without_a_probe() {
        let c = build_chain();
        let memory = memory_seen(&c, 30);
        let mut arcade = mockito::Server::new_async().await;
        let probe = arcade
            .mock("GET", mockito::Matcher::Regex("^/tx/".to_string()))
            .expect(0)
            .create_async()
            .await;
        let single = arcade
            .mock("POST", "/tx")
            .match_body(subject_ef(&c))
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(submit_accepted(&c.subject))
            .expect(1)
            .create_async()
            .await;
        let batch = arcade.mock("POST", "/txs").expect(0).create_async().await;

        let services = arcade_services(&arcade.url());
        services.set_broadcast_memory(memory.clone());
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        probe.assert_async().await;
        single.assert_async().await;
        batch.assert_async().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_success());
        assert_eq!(
            memory.status_of(&c.a, PROVIDER_ARCADE_V2).as_deref(),
            Some(BROADCAST_STATUS_SEEN)
        );
    }

    #[tokio::test]
    async fn stale_seen_reconfirmed_by_the_broadcaster_keeps_the_reduced_send() {
        let c = build_chain();
        let memory = memory_seen(&c, BROADCAST_SEEN_STALE_SECS + 600);
        let before = memory.seen_at_of(&c.a, PROVIDER_ARCADE_V2).unwrap();
        let mut arcade = mockito::Server::new_async().await;
        let probe = arcade
            .mock("GET", format!("/tx/{}", c.a).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(tx_info(&c.a, "SEEN_ON_NETWORK"))
            .expect(1)
            .create_async()
            .await;
        let other_probe = arcade
            .mock("GET", format!("/tx/{}", c.b).as_str())
            .expect(0)
            .create_async()
            .await;
        let single = arcade
            .mock("POST", "/tx")
            .match_body(subject_ef(&c))
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(submit_accepted(&c.subject))
            .expect(1)
            .create_async()
            .await;
        let batch = arcade.mock("POST", "/txs").expect(0).create_async().await;

        let services = arcade_services(&arcade.url());
        services.set_broadcast_memory(memory.clone());
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        probe.assert_async().await;
        other_probe.assert_async().await;
        single.assert_async().await;
        batch.assert_async().await;
        assert!(results[0].is_success());
        // The re-confirmation refreshed the evidence.
        let after = memory.seen_at_of(&c.a, PROVIDER_ARCADE_V2).unwrap();
        assert!(after > before, "seen_at refreshed by the probe");
        assert_eq!(
            memory.status_of(&c.a, PROVIDER_ARCADE_V2).as_deref(),
            Some(BROADCAST_STATUS_SEEN)
        );
    }

    #[tokio::test]
    async fn stale_seen_rejected_by_the_broadcaster_falls_back_to_the_full_package() {
        let c = build_chain();
        let memory = memory_seen(&c, BROADCAST_SEEN_STALE_SECS + 600);
        let mut arcade = mockito::Server::new_async().await;
        let probe = arcade
            .mock("GET", format!("/tx/{}", c.a).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(tx_info(&c.a, "REJECTED"))
            .expect(1)
            .create_async()
            .await;
        let single = arcade.mock("POST", "/tx").expect(0).create_async().await;
        let batch = arcade
            .mock("POST", "/txs")
            .match_body(full_body(&c))
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":0,"submitted":3,"total":3}"#)
            .expect(1)
            .create_async()
            .await;

        let services = arcade_services(&arcade.url());
        services.set_broadcast_memory(memory.clone());
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        probe.assert_async().await;
        single.assert_async().await;
        batch.assert_async().await;
        assert!(results[0].is_success());
        // The rejected ancestor is remembered as such, and the batch's
        // acceptance does not launder it.
        assert_eq!(
            memory.status_of(&c.a, PROVIDER_ARCADE_V2).as_deref(),
            Some(BROADCAST_STATUS_REJECTED)
        );
        assert_eq!(
            memory.status_of(&c.b, PROVIDER_ARCADE_V2).as_deref(),
            Some(BROADCAST_STATUS_SEEN),
            "only the probed ancestor changes"
        );
    }

    #[tokio::test]
    async fn stale_seen_unknown_to_the_broadcaster_falls_back_to_the_full_package() {
        let c = build_chain();
        let memory = memory_seen(&c, BROADCAST_SEEN_STALE_SECS + 600);
        let mut arcade = mockito::Server::new_async().await;
        let probe = arcade
            .mock("GET", format!("/tx/{}", c.a).as_str())
            .with_status(404)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"transaction not found"}"#)
            .expect(1)
            .create_async()
            .await;
        let batch = arcade
            .mock("POST", "/txs")
            .match_body(full_body(&c))
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":0,"submitted":3,"total":3}"#)
            .expect(1)
            .create_async()
            .await;

        let services = arcade_services(&arcade.url());
        services.set_broadcast_memory(memory.clone());
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        probe.assert_async().await;
        batch.assert_async().await;
        assert!(results[0].is_success());
        assert_eq!(
            memory.status_of(&c.a, PROVIDER_ARCADE_V2).as_deref(),
            Some(BROADCAST_STATUS_UNKNOWN)
        );
        // The next broadcast keeps `a` in the package (unknown never
        // qualifies, so it is not even re-probed), while `b`, stale too, is
        // re-checked and stays skipped.
        let probe_a = arcade
            .mock("GET", format!("/tx/{}", c.a).as_str())
            .expect(0)
            .create_async()
            .await;
        let probe_b = arcade
            .mock("GET", format!("/tx/{}", c.b).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(tx_info(&c.b, "SEEN_MULTIPLE_NODES"))
            .expect(1)
            .create_async()
            .await;
        let reduced_body: Vec<u8> = beef_to_ef_batch_skipping(&c.beef, &set(&[&c.b]))
            .unwrap()
            .entries
            .iter()
            .flat_map(|e| e.ef.clone())
            .collect();
        let batch2 = arcade
            .mock("POST", "/txs")
            .match_body(reduced_body)
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":1,"submitted":1,"total":2}"#)
            .expect(1)
            .create_async()
            .await;
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        assert!(results[0].is_success());
        probe_a.assert_async().await;
        probe_b.assert_async().await;
        batch2.assert_async().await;
        assert_eq!(
            memory.status_of(&c.a, PROVIDER_ARCADE_V2).as_deref(),
            Some(BROADCAST_STATUS_UNKNOWN),
            "the batch acceptance does not launder the absence"
        );
    }

    #[tokio::test]
    async fn a_failed_probe_sends_the_full_package_and_leaves_the_memory_alone() {
        let c = build_chain();
        let memory = memory_seen(&c, BROADCAST_SEEN_STALE_SECS + 600);
        let mut arcade = mockito::Server::new_async().await;
        let probe = arcade
            .mock("GET", format!("/tx/{}", c.a).as_str())
            .with_status(503)
            .expect(1)
            .create_async()
            .await;
        let batch = arcade
            .mock("POST", "/txs")
            .match_body(full_body(&c))
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":0,"submitted":3,"total":3}"#)
            .expect(1)
            .create_async()
            .await;

        let services = arcade_services(&arcade.url());
        services.set_broadcast_memory(memory.clone());
        let results = services
            .post_beef(&c.beef, std::slice::from_ref(&c.subject))
            .await
            .unwrap();
        probe.assert_async().await;
        batch.assert_async().await;
        assert!(results[0].is_success());
        assert_eq!(
            memory.status_of(&c.a, PROVIDER_ARCADE_V2).as_deref(),
            Some(BROADCAST_STATUS_SEEN),
            "a transient probe fault is not a verdict"
        );
    }
}

// =============================================================================
// Storage wiring (sqlite)
// =============================================================================

#[cfg(feature = "sqlite")]
mod storage_wiring {
    use super::*;
    use bsv_rs::transaction::MockChainTracker;
    use bsv_wallet_toolbox_rs::storage::StorageSqlx;
    use bsv_wallet_toolbox_rs::{
        MonitorStorage, ProofIngestOutcome, Wallet, WalletStorageProvider, WalletStorageWriter,
        BROADCAST_PROVIDER_NETWORK,
    };

    async fn setup_storage() -> StorageSqlx {
        let storage = StorageSqlx::in_memory().await.expect("in_memory storage");
        let storage_key = "02".to_string() + &"ab".repeat(32);
        storage
            .migrate("test-broadcast-memory", &storage_key)
            .await
            .expect("migrate");
        storage.make_available().await.expect("make_available");
        storage
    }

    #[tokio::test]
    async fn the_wallet_hands_the_storage_memory_to_the_services() {
        let storage = setup_storage().await;
        assert!(WalletStorageProvider::broadcast_memory(&storage).is_some());
        let services = Services::mainnet().unwrap();
        assert!(services.broadcast_memory().is_none());

        let wallet = Wallet::new(None, storage, services).await.unwrap();
        assert!(
            wallet.services().broadcast_memory().is_some(),
            "Wallet::new attaches the storage's persisted memory"
        );
    }

    #[tokio::test]
    async fn set_services_on_the_storage_attaches_the_memory_too() {
        let storage = setup_storage().await;
        let services: StdArc<dyn WalletServices> = StdArc::new(Services::mainnet().unwrap());
        assert!(services.broadcast_memory().is_none());
        storage.set_services(services.clone());
        assert!(services.broadcast_memory().is_some());
    }

    #[tokio::test]
    async fn a_mined_proof_is_remembered_for_every_provider() {
        let storage = setup_storage().await;
        let txid = "a".repeat(64);
        let height = 850_000u32;

        // A req row so the proof has raw bytes to attach to.
        let now = chrono::Utc::now();
        sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at)
            VALUES (?, 'unmined', 0, '{}', 0, '{}', X'01000000', ?, ?)
            "#,
        )
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert proven_tx_req");

        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let root = bump.compute_root(Some(&txid)).unwrap();
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, root);
        storage.set_chain_tracker(StdArc::new(tracker)).await;

        let outcome = storage
            .ingest_merkle_proof(&txid, &bump.to_binary(), height, &"b".repeat(64), None)
            .await
            .expect("ingest ok");
        assert!(matches!(outcome, ProofIngestOutcome::Ingested(_)));

        let txids = vec![txid.clone()];
        assert!(storage
            .broadcast_seen_for(PROVIDER_TAAL_ARC, &txids)
            .await
            .unwrap()
            .contains(&txid));
        assert!(storage
            .broadcast_seen_for("AnyOtherProvider", &txids)
            .await
            .unwrap()
            .contains(&txid));
        let (provider, status): (String, String) =
            sqlx::query_as("SELECT provider, status FROM broadcast_seen WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(provider, BROADCAST_PROVIDER_NETWORK);
        assert_eq!(status, "mined");

        // A push "seen" for the same txid never downgrades mined.
        storage
            .mark_transaction_seen_on_network_by(&txid, "ArcadeV2")
            .await
            .unwrap();
        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM broadcast_seen WHERE txid = ? AND provider = ?")
                .bind(&txid)
                .bind(BROADCAST_PROVIDER_NETWORK)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(status, "mined");
    }
}
