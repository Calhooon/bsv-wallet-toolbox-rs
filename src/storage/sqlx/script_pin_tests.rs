//! #422: the live internalize verifier uses the node's word per input.

use super::beef_verification::verify_beef_spends;
use crate::Error;
use bsv_rs::primitives::bsv::sighash::{
    compute_sighash_original_for_signing, parse_transaction, SighashParams, TxSighashCache,
};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{
    LockingScript, ProtocolEra, ScriptFlags, Spend, TxSpendParams, UnlockingScript,
};
use bsv_rs::transaction::beef_stream::{ChainTip, SpendRefusal};
use bsv_rs::transaction::{
    Beef, ChainTracker, ChainTrackerError, MerklePath, Transaction, TransactionInput,
    TransactionOutput,
};
use std::sync::Arc;

const HEIGHT: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT + 100;

struct Tracker {
    height: u32,
}

#[async_trait::async_trait]
impl ChainTracker for Tracker {
    async fn is_valid_root_for_height(
        &self,
        _: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        Ok(height == self.height)
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(self.height + 6)
    }

    async fn mining_tip(&self) -> Result<Option<ChainTip>, ChainTrackerError> {
        Ok(Some(ChainTip {
            height: self.height + 6,
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
    verify_at_height(parent, child, HEIGHT).await
}

async fn verify_at_height(
    parent: Transaction,
    child: Transaction,
    height: u32,
) -> crate::Result<()> {
    let parent_id = parent.id();
    let child_id = child.id();
    let bump = MerklePath::from_coinbase_txid(&parent_id, height);
    let root = bump.compute_root(Some(&parent_id)).unwrap();
    let mut root_bytes: [u8; 32] = hex::decode(root).unwrap().try_into().unwrap();
    root_bytes.reverse();
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(parent.to_binary(), Some(index));
    beef.merge_transaction(child);
    verify_beef_spends(
        &beef.to_binary_atomic(&child_id).unwrap(),
        &[(u64::from(height), root_bytes)],
        &Tracker { height },
    )
    .await
}

fn signed_payment(scope: u8) -> (Transaction, Transaction) {
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
    (parent, child)
}

async fn signed(scope: u8) -> crate::Result<()> {
    let (parent, child) = signed_payment(scope);
    verify(parent, child).await
}

fn script_words(result: crate::Result<()>, expected: &str) {
    match result.unwrap_err() {
        Error::SpendRefused {
            input: Some(0),
            why: SpendRefusal::Script(message),
            ..
        } => {
            eprintln!("input script refusal: {message}");
            assert_eq!(message, expected);
        }
        other => panic!("expected an input script refusal, got {other:?}"),
    }
}

/// The beta.3 default accepted this original-digest signature. The live
/// door must require a fork ID under the node's word (#422).
#[tokio::test]
async fn internalize_verifier_accepts_original_digest_without_forkid_at_beta3() {
    script_words(signed(0x01).await, "The signature must use SIGHASH_FORKID.");
}

/// Before Chronicle, the live door returns the word's era text for 0x61
/// and its hash-type text for an undefined base type.
#[tokio::test]
async fn internalize_verifier_uses_standard_version_one_signature_words() {
    script_words(
        signed(0x61).await,
        "The signature must not use SIGHASH_CHRONICLE before Chronicle.",
    );
    script_words(
        signed(0x44).await,
        "The signature's hash type (0x44) has no defined base type.",
    );
}

#[tokio::test]
async fn internalize_verifier_uses_standard_nonminimal_number_words() {
    let (parent, mut child) = payment("8b5287");
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("020100").unwrap());
    script_words(
        verify(parent, child).await,
        "non-minimally encoded script number",
    );
}

// Direct SDK witnesses alongside the live door's verdicts.
fn word_verdict(
    parent: &Transaction,
    child: &Transaction,
    era: ProtocolEra,
) -> Result<bool, String> {
    let raw = parse_transaction(&child.to_binary()).unwrap();
    let transaction = Arc::new(TxSighashCache::new(
        raw.version,
        raw.inputs,
        raw.outputs,
        raw.locktime,
    ));
    let mut spend = Spend::with_transaction(TxSpendParams {
        transaction,
        input_index: 0,
        source_satoshis: parent.outputs[0].satoshis.unwrap(),
        locking_script: parent.outputs[0].locking_script.clone(),
        unlocking_script: child.inputs[0].unlocking_script.clone().unwrap(),
        memory_limit: None,
        work_limit: None,
    })
    .unwrap();
    spend.set_flags(ScriptFlags::standard(era));
    spend.validate().map_err(|error| error.message)
}

/// A valid `OP_SUBSTR` at version 1 is accepted in both modes after Chronicle.
/// It cannot witness a default-mode acceptance that the word refuses.
#[tokio::test]
async fn post_chronicle_version_one_substr_passes_both_modes() {
    let (parent, mut child) = payment("0052b302616287");
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("03616263").unwrap());
    assert_eq!(
        word_verdict(&parent, &child, ProtocolEra::PostChronicle),
        Ok(true)
    );
    assert!(
        verify_at_height(parent, child, ProtocolEra::MAINNET_CHRONICLE_HEIGHT)
            .await
            .is_ok()
    );
    eprintln!("version-1 OP_SUBSTR: post-Chronicle word and live door accepted");
}

/// The same valid signature that the default mode refuses is accepted under
/// the post-Chronicle word, as overlay #205 describes.
#[tokio::test]
async fn post_chronicle_word_accepts_version_one_chronicle_signature() {
    let (parent, child) = signed_payment(0x61);
    assert_eq!(
        word_verdict(&parent, &child, ProtocolEra::PostChronicle),
        Ok(true)
    );
    assert_eq!(
        word_verdict(&parent, &child, ProtocolEra::PostGenesis),
        Err("The signature must not use SIGHASH_CHRONICLE before Chronicle.".into())
    );
    let verdict = verify_at_height(parent, child, ProtocolEra::MAINNET_CHRONICLE_HEIGHT).await;
    eprintln!("version-1 0x61 at Chronicle: {verdict:?}");
    assert!(verdict.is_ok(), "{verdict:?}");
}

#[tokio::test]
async fn standard_word_refuses_original_digest_without_forkid() {
    let (parent, child) = signed_payment(0x01);
    for era in [ProtocolEra::PostGenesis, ProtocolEra::PostChronicle] {
        assert_eq!(
            word_verdict(&parent, &child, era),
            Err("The signature must use SIGHASH_FORKID.".into())
        );
    }
    script_words(
        verify_at_height(parent, child, ProtocolEra::MAINNET_CHRONICLE_HEIGHT).await,
        "The signature must use SIGHASH_FORKID.",
    );
}

#[test]
fn sdk_mainnet_era_mapping_declines_pre_genesis_heights() {
    for (height, expected) in [
        (620_537, None),
        (620_538, Some(ProtocolEra::PostGenesis)),
        (943_815, Some(ProtocolEra::PostGenesis)),
        (943_816, Some(ProtocolEra::PostChronicle)),
    ] {
        assert_eq!(ProtocolEra::mainnet(height), expected);
    }
    eprintln!("mainnet eras: pre-620538 unavailable; 620538..943815 post-Genesis; 943816 onward post-Chronicle");
}
