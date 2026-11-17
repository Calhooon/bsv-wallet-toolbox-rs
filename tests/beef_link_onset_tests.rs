//! The reach of bsv-rs's default link bound on a plain single-parent sweep,
//! pinned from below and above (GHSA-33fm-jcm5-4m3m, the completion of
//! GHSA-gvmh-f5gf-5vjj's fix).
//!
//! Since the `=0.9.0-beta.9` pin the default `BeefLinkLimits` carries two
//! bounds, the wire bound (16 MiB of re-materialized parents) and a heap
//! bound (56 MiB). A child spending every output of one proven parent of
//! plain-payment (P2PKH) outputs is linked by `Transaction::from_beef` up to
//! 586 outputs and refused from 587 with `Error::BeefResourceLimit`; at
//! `=0.9.0-beta.8` the wire bound alone took 702. No library door of this
//! crate links a BEEF with bsv-rs's doors (each reads its subject or its
//! entry unlinked), so the figure is a host's that links in its own code.

use bsv_rs::script::{LockingScript, ProtocolEra, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};

/// The largest plain sweep the default links at the pin.
const ONSET: u32 = 586;

/// An atomic BEEF of a proven parent of `spent` P2PKH outputs and a child
/// spending every one of them: the bytes and the child's txid.
fn plain_sweep(spent: u32) -> (Vec<u8>, String) {
    let p2pkh = LockingScript::from_hex(&format!("76a914{}88ac", "22".repeat(20))).unwrap();
    let mut parent = Transaction::new();
    let mut funding = TransactionInput::new("11".repeat(32), 0);
    funding.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs_mut().push(funding);
    for _ in 0..spent {
        parent.outputs_mut().push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: p2pkh.clone(),
            change: false,
        });
    }
    let parent_txid = parent.id();
    let bump =
        MerklePath::from_coinbase_txid(&parent_txid, ProtocolEra::MAINNET_GENESIS_HEIGHT + 101);
    let mut child = Transaction::new();
    child.set_version(1);
    for vout in 0..spent {
        let mut input = TransactionInput::new(parent_txid.clone(), vout);
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        child.inputs_mut().push(input);
    }
    child.outputs_mut().push(TransactionOutput {
        satoshis: Some(u64::from(spent) * 1_000 - 500),
        locking_script: p2pkh,
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(bump);
    beef.merge_raw_tx(parent.to_binary(), Some(bump)).unwrap();
    beef.merge_raw_tx(child.to_binary(), None).unwrap();
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// From below: the default links a plain sweep of 586 outputs.
#[test]
fn the_default_links_a_plain_sweep_of_586_outputs() {
    let (beef, txid) = plain_sweep(ONSET);
    let linked = Transaction::from_beef(&beef, Some(&txid)).expect("586 outputs link");
    assert_eq!(linked.inputs().len(), ONSET as usize);
}

/// From above: one output more is refused for its heap, in bsv-rs's words.
#[test]
fn the_default_refuses_a_plain_sweep_of_587_outputs_for_its_heap() {
    let (beef, txid) = plain_sweep(ONSET + 1);
    let refused = Transaction::from_beef(&beef, Some(&txid)).expect_err("587 outputs are refused");
    assert!(
        matches!(refused, bsv_rs::Error::BeefResourceLimit(_)),
        "the resource class: {refused:?}"
    );
    assert_eq!(
        refused.to_string(),
        "BEEF link resource limit: linking would hold more than 58720256 heap bytes of \
         re-materialized shared-parent stubs and proofs at once (bsv-rs #749). Raise or disable \
         the bound with Transaction::from_beef_with_limits / \
         BeefLinkLimits::with_max_link_heap_bytes / BeefLinkLimits::unbounded."
    );
}
