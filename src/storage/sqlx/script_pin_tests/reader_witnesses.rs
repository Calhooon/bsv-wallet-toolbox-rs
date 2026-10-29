//! #396: the live internalize door over two bsv-rs 0.9.0-beta.2 reader changes.
//!
//! Each witness is a mainnet payment whose parent is proven at Chronicle's
//! activation height, the tip six blocks on, so the stream and the live word
//! both judge the spend in the post-Chronicle era.

use super::{internalize_on, proof, signed_payment, FixtureTracker};
use bsv_rs::primitives::Writer;
use bsv_rs::script::{LockingScript, ProtocolEra, UnlockingScript};
use bsv_rs::transaction::{Beef, BeefTx, Transaction, TransactionInput, TransactionOutput};

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

/// One push of `PUSH` bytes, copied by `OP_DUP` `DUPS` times and dropped
/// again: 331 elements of 100,000 bytes, 33,100,000 bytes on the stack.
const PUSH: usize = 100_000;
const DUPS: usize = 330;

/// bsv-rs #331 (0.9.0-beta.2): the streaming reader runs each input under
/// `memory_limit: None`, uncapped, where beta.1 declined a stack past
/// 32,000,000 bytes. The door passes no figure of its own yet, so this spend
/// is accepted; #395 gives the door its figure (bsv-rs #373's
/// `set_memory_limit`), and this witness then turns to its refusal.
#[tokio::test]
async fn internalize_runs_a_script_past_beta1_memory_cap_until_395() {
    const { assert!((DUPS + 1) * PUSH > 32_000_000) };
    let lock = format!("{}{}", "76".repeat(DUPS), "75".repeat(DUPS));
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new("11".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(100),
        locking_script: LockingScript::from_hex(&lock).unwrap(),
        change: false,
    });
    let mut unlock = vec![0x4e];
    unlock.extend(u32::try_from(PUSH).unwrap().to_le_bytes());
    unlock.extend(std::iter::repeat_n(0x01, PUSH));
    let mut child = Transaction::new();
    let mut input = TransactionInput::new(parent.id(), 0);
    input.unlocking_script = Some(UnlockingScript::from_binary(&unlock).unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(90),
        locking_script: LockingScript::from_hex("51").unwrap(),
        change: false,
    });
    let bytes = atomic(
        &child,
        vec![(parent.clone(), Some(0)), (child.clone(), None)],
        &parent,
    );

    let (accepted, rows) = internalize(bytes, &parent).await;
    assert!(accepted.unwrap(), "uncapped until #395");
    assert_eq!(rows.len(), 1);
}
