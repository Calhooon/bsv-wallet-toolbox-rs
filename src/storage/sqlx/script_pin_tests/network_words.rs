//! #440, #452: each network judges by its own `ChainParams`, and an input
//! bsv-rs cannot judge is `SpendRefusal::NotJudged` with its typed cause.

use super::{internalize_on, proof, signed_payment, verify_entries, FixtureTracker};
use crate::Error;
use bsv_rs::script::{NotJudgedCause, ProtocolEra};
use bsv_rs::transaction::beef_stream::SpendRefusal;
use bsv_rs::transaction::Beef;

/// The door's not-judged refusal at input 0 (#452): bsv-rs's typed cause,
/// matched on its fields, never its text.
fn not_judged(result: crate::Result<()>, expected: NotJudgedCause) {
    match result.unwrap_err() {
        Error::SpendRefused {
            input: Some(0),
            why: SpendRefusal::NotJudged { input: 0, cause },
            ..
        } => {
            eprintln!("input not judged: {cause}");
            assert_eq!(cause, expected);
        }
        other => panic!("expected input 0 not judged, got {other:?}"),
    }
}

/// Testnet and regtest judge by their own `ChainParams` (#440): a coin
/// proven at the network's Genesis spent after its Chronicle passes. A coin
/// proven at height 1, which both accepted before #440 (the spend's era for
/// the coin), is now before their Genesis: not judged, bsv-rs's cause.
#[tokio::test]
async fn word_uses_each_networks_own_heights_for_testnet_and_regtest() {
    for (network, chain) in [
        ("test", bsv_rs::block::ChainParams::testnet()),
        ("regtest", bsv_rs::block::ChainParams::regtest()),
    ] {
        let (parent, child) = signed_payment(0x61);
        let bump = proof(&parent, chain.genesis_height);
        let result = verify_entries(
            vec![(parent, Some(0)), (child, None)],
            vec![bump],
            chain.chronicle_height,
            network,
        )
        .await;
        assert!(result.is_ok(), "{network}: {result:?}");

        let (parent, child) = signed_payment(0x61);
        let bump = proof(&parent, 1);
        not_judged(
            verify_entries(
                vec![(parent, Some(0)), (child, None)],
                vec![bump],
                chain.chronicle_height,
                network,
            )
            .await,
            NotJudgedCause::PreGenesisCoin {
                height: 1,
                genesis_height: chain.genesis_height,
            },
        );
    }
}

/// Before #440: `The mainnet coin height is before Genesis.`, a script
/// refusal; now not judged with bsv-rs's `PreGenesisCoin` (#452).
#[tokio::test]
async fn word_does_not_judge_a_pre_genesis_mainnet_coin() {
    let (parent, child) = signed_payment(0x61);
    let bump = proof(&parent, ProtocolEra::MAINNET_GENESIS_HEIGHT - 1);
    not_judged(
        verify_entries(
            vec![(parent, Some(0)), (child, None)],
            vec![bump],
            ProtocolEra::MAINNET_CHRONICLE_HEIGHT,
            "main",
        )
        .await,
        NotJudgedCause::PreGenesisCoin {
            height: ProtocolEra::MAINNET_GENESIS_HEIGHT - 1,
            genesis_height: ProtocolEra::MAINNET_GENESIS_HEIGHT,
        },
    );
}

/// The same bytes on three stores: a coin proven at 620,537 is before
/// mainnet's and testnet's Genesis (not judged, each network's height named)
/// and after regtest's (accepted), at a tip past every network's Chronicle.
/// Before #440 testnet and regtest read mainnet's heights and accepted.
#[tokio::test]
async fn internalize_reads_the_stored_network_before_writing_received_outputs() {
    let height = ProtocolEra::MAINNET_GENESIS_HEIGHT - 1;
    let (parent, child) = signed_payment(0x61);
    let child_id = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(proof(&parent, height));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_transaction(child);
    let bytes = beef.to_binary_atomic(&child_id).unwrap();
    let mut root: [u8; 32] = hex::decode(parent.id()).unwrap().try_into().unwrap();
    root.reverse();

    for (network, genesis) in [
        ("mainnet", Some(ProtocolEra::MAINNET_GENESIS_HEIGHT)),
        (
            "testnet",
            Some(bsv_rs::block::ChainParams::testnet().genesis_height),
        ),
        ("regtest", None),
    ] {
        let tracker = FixtureTracker {
            roots: vec![(u64::from(height), root)],
            tip: bsv_rs::block::ChainParams::testnet().chronicle_height,
        };
        let (storage, result) = internalize_on(network, tracker, bytes.clone()).await;
        match genesis {
            Some(genesis_height) => not_judged(
                result.map(|_| ()),
                NotJudgedCause::PreGenesisCoin {
                    height,
                    genesis_height,
                },
            ),
            None => assert!(result.unwrap().base.accepted, "{network}"),
        }
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(rows, i64::from(genesis.is_none()), "{network}");
    }
}
