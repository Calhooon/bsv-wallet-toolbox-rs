//! #396: the live internalize door over two bsv-rs 0.9.0-beta.2 reader changes,
//! and #395: the door's stack memory figure (`DOOR_STACK_MEMORY_BYTES`).
//!
//! Each door witness is a mainnet payment whose parent is proven at Chronicle's
//! activation height, the tip six blocks on, so the stream and the live word
//! both judge the spend in the post-Chronicle era.

use super::{internalize_on, payment, proof, signed_payment, FixtureTracker};
use crate::spend_verifier::DOOR_STACK_MEMORY_BYTES;
use crate::Error;
use bsv_rs::block::ChainParams;
use bsv_rs::primitives::bsv::sighash::{parse_transaction, TxSighashCache};
use bsv_rs::primitives::Writer;
use bsv_rs::script::{ProtocolEra, ScriptResource, Spend, TxSpendParams, UnlockingScript, Word};
use bsv_rs::transaction::beef_stream::SpendRefusal;
use bsv_rs::transaction::{Beef, BeefTx, Transaction};
use std::sync::Arc;

const HEIGHT: u32 = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;

/// An atomic BEEF of `txs` in the order given, its subject `subject`;
/// `Beef::to_binary_atomic` sorts, so the writer is called directly.
fn atomic(
    subject: &Transaction,
    txs: Vec<(Transaction, Option<usize>)>,
    parent: &Transaction,
) -> Vec<u8> {
    let mut beef = Beef::new();
    beef.merge_bump(proof(parent, HEIGHT));
    beef.txs = txs
        .into_iter()
        .map(|(tx, bump)| BeefTx::from_tx(tx, bump))
        .collect();
    let mut writer = Writer::new();
    writer.write_u32_le(0x0101_0101);
    let mut id = hex::decode(subject.id()).unwrap();
    id.reverse();
    writer.write_bytes(&id);
    beef.to_writer(&mut writer);
    writer.into_bytes()
}

/// The output rows the door wrote: vout, satoshis, script, spendable, type.
type Rows = Vec<(i64, i64, Vec<u8>, bool, String)>;

/// Drives the door on a fresh mainnet store with `parent`'s root known at
/// [`HEIGHT`], the tip six blocks on.
async fn internalize(bytes: Vec<u8>, parent: &Transaction) -> (crate::Result<bool>, Rows) {
    let mut root: [u8; 32] = hex::decode(parent.id()).unwrap().try_into().unwrap();
    root.reverse();
    let tracker = FixtureTracker {
        roots: vec![(u64::from(HEIGHT), root)],
        tip: HEIGHT + 6,
    };
    let (storage, result) = internalize_on("mainnet", tracker, bytes).await;
    let rows: Rows = sqlx::query_as(
        "SELECT vout, satoshis, locking_script, spendable, type FROM outputs ORDER BY vout",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();
    (result.map(|r| r.base.accepted), rows)
}

/// bsv-rs #240 (0.9.0-beta.2): a BEEF whose child precedes its parent is
/// valid; beta.1 refused it as `InputNamesNoElement`. The door accepts it
/// through the stream and the live word, and records the outputs the
/// in-order BEEF records.
#[tokio::test]
async fn internalize_accepts_a_child_written_ahead_of_its_parent() {
    let (parent, child) = signed_payment(0x61);
    let in_order = atomic(
        &child,
        vec![(parent.clone(), Some(0)), (child.clone(), None)],
        &parent,
    );
    let child_ahead = atomic(
        &child,
        vec![(child.clone(), None), (parent.clone(), Some(0))],
        &parent,
    );
    assert_ne!(in_order, child_ahead);

    let (accepted, expected) = internalize(in_order, &parent).await;
    assert!(accepted.unwrap(), "the in-order BEEF");
    let (accepted, rows) = internalize(child_ahead, &parent).await;
    assert!(accepted.unwrap(), "the BEEF with the child ahead");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows, expected);
}

/// The size of the one push the stack copies: 100,000 bytes, 100,032 on the
/// node's count (its bytes plus 32).
const PUSH: usize = 100_000;

/// A payment whose spend pushes `PUSH` bytes and then runs the parent's
/// `OP_DUP` `dups` times and `OP_DROP` as often: `dups + 1` elements of
/// `PUSH` bytes at the peak, `(dups + 1) * 100,032` bytes on the node's count.
fn copies(dups: usize) -> (Transaction, Transaction) {
    let (parent, mut child) = payment(&format!("{}{}", "76".repeat(dups), "75".repeat(dups)));
    let mut unlock = vec![0x4e];
    unlock.extend(u32::try_from(PUSH).unwrap().to_le_bytes());
    unlock.extend(std::iter::repeat_n(0x01, PUSH));
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_binary(&unlock).unwrap());
    (parent, child)
}

/// The door's verdict and rows on [`copies`]`(dups)`, the parent proven.
async fn door_on_copies(dups: usize) -> (crate::Result<bool>, Rows) {
    let (parent, child) = copies(dups);
    let bytes = atomic(
        &child,
        vec![(parent.clone(), Some(0)), (child.clone(), None)],
        &parent,
    );
    internalize(bytes, &parent).await
}

/// bsv-rs #331 (0.9.0-beta.2) undid beta.1's 32,000,000-byte decline, and the
/// door judges under the node's policy figure (#395, #539): 331 elements of
/// 100,000 bytes, 33,110,592 bytes on the node's count, sit under
/// `DOOR_STACK_MEMORY_BYTES` and are accepted. No reader witness turns to a
/// refusal under the figure (the keeper's reading of 2026-11-01 10:5xZ).
#[tokio::test]
async fn internalize_accepts_a_33_mb_stack_under_the_node_policy_figure() {
    const DUPS: usize = 330;
    const { assert!((DUPS + 1) * PUSH > 32_000_000) };
    const { assert!((DUPS + 1) * (PUSH + 32) < DOOR_STACK_MEMORY_BYTES) };
    let (accepted, rows) = door_on_copies(DUPS).await;
    assert!(accepted.unwrap(), "under the node's policy figure");
    assert_eq!(rows.len(), 1);
}

/// #395 at the line: 999 elements of 100,000 bytes, 99,931,968 bytes on the
/// node's count, are accepted at the door; 1,000, 100,032,000 bytes, are
/// refused with bsv-rs's policy verdict, which fires at the door's figure.
#[tokio::test]
async fn internalize_judges_the_stack_at_the_node_policy_figure() {
    const { assert!(999 * (PUSH + 32) == 99_931_968) };
    const { assert!(1_000 * (PUSH + 32) == 100_032_000) };
    let (accepted, rows) = door_on_copies(998).await;
    assert!(accepted.unwrap(), "999 elements, 99,931,968 bytes");
    assert_eq!(rows.len(), 1);

    let (refused, rows) = door_on_copies(999).await;
    match refused {
        Err(Error::SpendRefused {
            input: Some(0),
            why: SpendRefusal::Script(message),
            ..
        }) => assert_eq!(
            message,
            "Stack size limit exceeded: 100032000 bytes, the stack memory policy is 100000000 bytes."
        ),
        other => panic!("1,000 elements, 100,032,000 bytes: {other:?}"),
    }
    assert!(rows.is_empty());
}

/// The bite of the explicit figure, at the `Spend` the door builds: the
/// door's word (the spend at Chronicle + 7, the coin at Chronicle) with
/// bsv-rs's policy verdict lifted (`set_stack_memory_policy(0)`), so the
/// local budget alone bounds the stack. `memory_limit` is
/// `DOOR_STACK_MEMORY_BYTES`, as `judge_input` passes it: 999 elements run,
/// 1,000 stop with a stack decline at the figure. bsv-rs 0.9.0-beta.5 reads
/// `None` under a word as the same figure, so only a moved figure, not `None`,
/// turns this red today.
#[test]
fn the_door_memory_figure_declines_the_stack_past_it() {
    let word = ChainParams::mainnet()
        .word_for_input(Word::Standard, Some(HEIGHT + 7), Some(HEIGHT))
        .unwrap();
    let judge = |dups: usize| {
        let (parent, child) = copies(dups);
        let raw = parse_transaction(&child.to_binary()).unwrap();
        let mut spend = Spend::with_transaction(TxSpendParams {
            transaction: Arc::new(TxSighashCache::new(
                raw.version,
                raw.inputs,
                raw.outputs,
                raw.locktime,
            )),
            input_index: 0,
            source_satoshis: 100,
            locking_script: parent.outputs[0].locking_script.clone(),
            unlocking_script: child.inputs[0].unlocking_script.clone().unwrap(),
            memory_limit: Some(DOOR_STACK_MEMORY_BYTES),
            work_limit: Some(1 << 31),
        })
        .unwrap();
        spend.set_flags(word);
        spend.set_stack_memory_policy(0);
        assert_eq!(spend.stack_memory_policy(), None);
        spend
            .validate()
            .map_err(|error| (error.message, error.resource_limit))
    };
    assert_eq!(judge(998).map_err(|(message, _)| message), Ok(true));
    let limit = judge(999).unwrap_err().1.unwrap();
    assert!(matches!(limit.resource, ScriptResource::Stack));
    assert_eq!(limit.limit, 100_000_000);
    assert_eq!(limit.attempted, 100_032_000);
}
