//! #419 item 6: the internalize verifier inherits the SDK's default mode.

use super::beef_verification::verify_beef_spends;
use crate::Error;
use bsv_rs::primitives::bsv::sighash::{
    compute_sighash_original_for_signing, parse_transaction, SighashParams,
};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::beef_stream::{ChainTip, SpendRefusal};
use bsv_rs::transaction::{
    Beef, ChainTracker, ChainTrackerError, MerklePath, Transaction, TransactionInput,
    TransactionOutput,
};

const HEIGHT: u32 = 100_001;

struct Tracker;

#[async_trait::async_trait]
impl ChainTracker for Tracker {
    async fn is_valid_root_for_height(
        &self,
        _: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        Ok(height == HEIGHT)
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(HEIGHT + 6)
    }

    async fn mining_tip(&self) -> Result<Option<ChainTip>, ChainTrackerError> {
        Ok(Some(ChainTip {
            height: HEIGHT + 6,
            median_time_past: 1_700_000_000,
            genesis_height: 620_538,
            csv_height: 419_328,
            next_block_time: None,
        }))
    }
}

fn payment(lock: &str) -> (Transaction, Transaction) {
    let mut parent = Transaction::new();
    parent.version = 1;
    let mut input = TransactionInput::new("11".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(100),
        locking_script: LockingScript::from_hex(lock).unwrap(),
        change: false,
    });
    let mut child = Transaction::new();
    child.version = 1;
    child.inputs.push(TransactionInput::new(parent.id(), 0));
    child.outputs.push(TransactionOutput {
        satoshis: Some(90),
        locking_script: LockingScript::from_hex("51").unwrap(),
        change: false,
    });
    (parent, child)
}

async fn verify(parent: Transaction, child: Transaction) -> crate::Result<()> {
    let parent_id = parent.id();
    let child_id = child.id();
    let bump = MerklePath::from_coinbase_txid(&parent_id, HEIGHT);
    let root = bump.compute_root(Some(&parent_id)).unwrap();
    let mut root_bytes: [u8; 32] = hex::decode(root).unwrap().try_into().unwrap();
    root_bytes.reverse();
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(parent.to_binary(), Some(index));
    beef.merge_transaction(child);
    verify_beef_spends(
        &beef.to_binary_atomic(&child_id).unwrap(),
        &[(u64::from(HEIGHT), root_bytes)],
        &Tracker,
    )
    .await
}

async fn signed(scope: u8) -> crate::Result<()> {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
    let lock = format!("21{}ac", key.public_key().to_hex());
    let (parent, mut child) = payment(&lock);
    let raw = parse_transaction(&child.to_binary()).unwrap();
    let script = hex::decode(lock).unwrap();
    let digest = compute_sighash_original_for_signing(&SighashParams {
        version: raw.version,
        inputs: &raw.inputs,
        outputs: &raw.outputs,
        locktime: raw.locktime,
        input_index: 0,
        subscript: &script,
        satoshis: 100,
        scope: u32::from(scope),
    });
    let mut signature = key.sign(&digest).unwrap().to_der();
    signature.push(scope);
    let mut unlock = vec![u8::try_from(signature.len()).unwrap()];
    unlock.extend(signature);
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_binary(&unlock).unwrap());
    child.invalidate_caches();
    verify(parent, child).await
}

fn script_words(result: crate::Result<()>, expected: &str) {
    match result.unwrap_err() {
        Error::SpendRefused {
            input: Some(0),
            why: SpendRefusal::Script(message),
            ..
        } => {
            assert_eq!(message, expected);
        }
        other => panic!("expected an input script refusal, got {other:?}"),
    }
}

/// #422 applies the node's word per input in a lane after the beta.3 tag;
/// when that lane lands, this assertion flips to a refusal with the node's text.
#[tokio::test]
async fn internalize_verifier_accepts_original_digest_without_forkid_at_beta3() {
    assert!(signed(0x01).await.is_ok());
}

/// #422 applies the node's word per input in a lane after the beta.3 tag;
/// when that lane lands, this assertion flips to a refusal with the node's text.
#[tokio::test]
async fn internalize_verifier_uses_the_default_version_one_signature_words() {
    for scope in [0x61, 0x44] {
        script_words(signed(scope).await, "The signature format is invalid.");
    }
}

#[tokio::test]
async fn internalize_verifier_uses_the_default_nonminimal_number_words() {
    let (parent, mut child) = payment("8b5287");
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("020100").unwrap());
    script_words(
        verify(parent, child).await,
        "non-minimally encoded script number",
    );
}
