//! #422: required node-word verdicts at the live internalize door.

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

/// The `AtomicBEEF` of `child` over `parent` proven at `height`, and the root.
fn atomic_payment(
    parent: &Transaction,
    child: Transaction,
    height: u32,
) -> (Vec<u8>, (u64, [u8; 32])) {
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
    let bytes = beef.to_binary_atomic(&child_id).unwrap();
    (bytes, (u64::from(height), root_bytes))
}

async fn verify_at_height(
    parent: Transaction,
    child: Transaction,
    height: u32,
) -> crate::Result<()> {
    let (bytes, root) = atomic_payment(&parent, child, height);
    let tracker = FixtureTracker {
        roots: vec![root],
        tip: height + 6,
    };
    verify_beef_spends(&bytes, &tracker.roots, &tracker, "mainnet").await
}

/// The fixture's roots with no mining tip, or with `fault` a tip read that
/// faults.
struct NoTipTracker {
    roots: FixtureTracker,
    fault: bool,
}

#[async_trait::async_trait]
impl ChainTracker for NoTipTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        self.roots.is_valid_root_for_height(root, height).await
    }
    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        self.roots.current_height().await
    }
    async fn mining_tip(&self) -> Result<Option<ChainTip>, ChainTrackerError> {
        if self.fault {
            return Err(ChainTrackerError::NetworkError("tip unavailable".into()));
        }
        Ok(None)
    }
}

/// `child` over `parent` proven at `HEIGHT`, the tracker without a tip.
async fn verify_without_tip(
    parent: Transaction,
    child: Transaction,
    fault: bool,
) -> crate::Result<()> {
    let (bytes, root) = atomic_payment(&parent, child, HEIGHT);
    let tracker = NoTipTracker {
        roots: FixtureTracker {
            roots: vec![root],
            tip: 0,
        },
        fault,
    };
    verify_beef_spends(&bytes, &tracker.roots.roots, &tracker, "mainnet").await
}

/// #464: bsv-rs's constants name no activation height still ahead, so with
/// no mining tip (or a tip fault) the door judges the spend under the latest
/// era's transaction-level word (post-Chronicle) with the coin's UTXO bits
/// from its proof height (Genesis + 100, post-Genesis), where it held with
/// `Service error: ChainTracker verification failed: the header service
/// supplied no mining tip`. A version-1 0x61 signature, which the tip at
/// `HEIGHT + 6` refuses with the era text, is accepted; one without
/// `SIGHASH_FORKID` is refused with bsv-rs's text.
#[tokio::test]
async fn internalize_verifier_judges_at_the_latest_era_without_a_tip() {
    for fault in [false, true] {
        let (parent, child) = signed_payment(0x61);
        let verdict = verify_without_tip(parent, child, fault).await;
        eprintln!("fault={fault}: version-1 0x61 on a post-Genesis coin: {verdict:?}");
        verdict.unwrap();
        let (parent, child) = signed_payment(0x01);
        script_words(
            verify_without_tip(parent, child, fault).await,
            "The signature must use SIGHASH_FORKID.",
        );
    }
}

fn signed_payment(scope: u8) -> (Transaction, Transaction) {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
    let lock = format!("21{}ac", key.public_key().to_hex());
    let (parent, mut child) = payment(&lock);
    sign_input(&parent, &mut child, 0, scope);
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

/// Witness (a): the beta.3 default accepted this original-digest signature.
/// The live door refuses it under the node's word (#422).
#[tokio::test]
async fn internalize_verifier_refuses_the_original_digest_beta3_accepted() {
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
        "Invalid script number: script execution error: Non-minimally encoded script number",
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

// The fixtures below name mainnet or the synthetic test network explicitly.
// BUMPs are checked by the local tracker; no provider participates.
struct FixtureTracker {
    roots: Vec<(u64, [u8; 32])>,
    tip: u32,
}

#[async_trait::async_trait]
impl ChainTracker for FixtureTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        Ok(self.roots.iter().any(|(h, r)| {
            *h == u64::from(height) && bsv_rs::transaction::beef_stream::display_hex(r) == root
        }))
    }
    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(self.tip)
    }
    async fn mining_tip(&self) -> Result<Option<ChainTip>, ChainTrackerError> {
        Ok(Some(ChainTip {
            height: self.tip,
            median_time_past: 1_700_000_000,
            genesis_height: ProtocolEra::MAINNET_GENESIS_HEIGHT,
            csv_height: 419_328,
            next_block_time: None,
        }))
    }
}

fn sign_input(parent: &Transaction, child: &mut Transaction, index: usize, scope: u8) {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
    let raw = parse_transaction(&child.to_binary()).unwrap();
    let script = parent.outputs[0].locking_script.to_binary();
    let digest = compute_sighash_original_for_signing(&SighashParams {
        version: raw.version,
        inputs: &raw.inputs,
        outputs: &raw.outputs,
        locktime: raw.locktime,
        input_index: index,
        subscript: &script,
        satoshis: parent.outputs[0].satoshis.unwrap(),
        scope: u32::from(scope),
    });
    let mut sig = key.sign(&digest).unwrap().to_der();
    sig.push(scope);
    let mut unlock = vec![u8::try_from(sig.len()).unwrap()];
    unlock.extend(sig);
    child.inputs[index].unlocking_script = Some(UnlockingScript::from_binary(&unlock).unwrap());
    child.invalidate_caches();
}

async fn verify_entries(
    entries: Vec<(Transaction, Option<usize>)>,
    proofs: Vec<MerklePath>,
    tip: u32,
    network: &str,
) -> crate::Result<()> {
    use bsv_rs::primitives::Writer;
    use bsv_rs::transaction::BeefTx;
    let roots = proofs
        .iter()
        .map(|bump| {
            let mut root: [u8; 32] = hex::decode(bump.compute_root(None).unwrap())
                .unwrap()
                .try_into()
                .unwrap();
            root.reverse();
            (u64::from(bump.block_height), root)
        })
        .collect::<Vec<_>>();
    let mut beef = Beef::new();
    for bump in proofs {
        beef.merge_bump(bump);
    }
    // to_writer keeps the supplied order and duplicate entries for the reader.
    beef.txs = entries
        .into_iter()
        .map(|(tx, bump)| BeefTx::from_tx(tx, bump))
        .collect();
    let mut writer = Writer::new();
    beef.to_writer(&mut writer);
    let tracker = FixtureTracker { roots, tip };
    verify_beef_spends(&writer.into_bytes(), &tracker.roots, &tracker, network).await
}

fn proof(tx: &Transaction, height: u32) -> MerklePath {
    MerklePath::from_coinbase_txid(&tx.id(), height)
}

fn spend_refusal(result: crate::Result<()>) -> SpendRefusal {
    match result {
        Err(Error::SpendRefused { why, .. }) => why,
        other => panic!("expected a spend refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn word_checks_an_unproven_ancestor_of_the_subject() {
    let (root, bad_parent) = signed_payment(0x01);
    let (_, mut subject) = payment("51");
    subject.inputs[0].source_txid = Some(bad_parent.id());
    subject.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("").unwrap());
    let parent_id = bad_parent.id();
    let bump = proof(&root, HEIGHT);
    let result = verify_entries(
        vec![(root, Some(0)), (bad_parent, None), (subject, None)],
        vec![bump],
        HEIGHT + 6,
        "mainnet",
    )
    .await;
    match &result {
        Err(Error::SpendRefused {
            txid,
            input: Some(0),
            ..
        }) => assert_eq!(txid, &parent_id),
        other => panic!("expected the ancestor's refusal, got {other:?}"),
    }
    script_words(result, "The signature must use SIGHASH_FORKID.");
}

#[tokio::test]
async fn word_preserves_value_checks_after_default_script_refusal() {
    let height = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let (parent, mut child) = signed_payment(0x61);
    child.outputs[0].satoshis = Some(101);
    sign_input(&parent, &mut child, 0, 0x61);
    assert_eq!(
        spend_refusal(verify_at_height(parent, child, height).await),
        SpendRefusal::CreatesValue,
    );
}

#[tokio::test]
async fn word_preserves_later_transactions_after_default_script_refusal() {
    let height = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let (root, parent) = signed_payment(0x61);
    let (_, mut subject) = payment("51");
    subject.inputs[0].source_txid = Some(parent.id());
    subject.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("").unwrap());
    subject.outputs[0].satoshis = Some(91);
    let subject_id = subject.id();
    let bump = proof(&root, height);
    let result = verify_entries(
        vec![(root, Some(0)), (parent, None), (subject, None)],
        vec![bump],
        height + 6,
        "mainnet",
    )
    .await;
    assert!(
        matches!(result, Err(Error::SpendRefused { why: SpendRefusal::CreatesValue, txid, .. }) if txid == subject_id)
    );
}

#[tokio::test]
async fn word_keeps_deferred_parents_and_duplicate_raw_entries() {
    let height = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let (root, child) = signed_payment(0x61);
    let bump = proof(&root, height);
    verify_entries(
        vec![(child.clone(), None), (child, None), (root, Some(0))],
        vec![bump],
        height + 6,
        "mainnet",
    )
    .await
    .expect("the deferred child and its duplicate share one spend judgement");
}

#[tokio::test]
async fn word_keeps_structural_refusals_final() {
    let height = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let (root, child) = signed_payment(0x61);
    let (_, mut absent_parent) = payment("51");
    absent_parent.inputs[0].source_txid = Some("55".repeat(32));
    let bump = proof(&root, height);
    let result = verify_entries(
        vec![(root, Some(0)), (child, None), (absent_parent, None)],
        vec![bump],
        height + 6,
        "mainnet",
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::InvalidBeef {
            kind: bsv_rs::transaction::beef_stream::Kind::InputNamesNoElement,
            ..
        })
    ));
}

#[tokio::test]
async fn word_preserves_later_mining_refusals_after_default_script_refusal() {
    let height = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let (root, parent) = signed_payment(0x61);
    let (_, mut nonfinal) = payment("51");
    nonfinal.inputs[0].source_txid = Some(parent.id());
    nonfinal.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("").unwrap());
    nonfinal.inputs[0].sequence = u32::MAX - 1;
    nonfinal.lock_time = height + 7;
    let mut p2sh = nonfinal.clone();
    p2sh.lock_time = 0;
    p2sh.outputs[0].locking_script =
        LockingScript::from_hex(&format!("a914{}87", "22".repeat(20))).unwrap();
    for (subject, expected) in [
        (
            nonfinal,
            SpendRefusal::NonFinal {
                lock_time: height + 7,
            },
        ),
        (p2sh, SpendRefusal::P2shOutput { vout: 0 }),
    ] {
        let bump = proof(&root, height);
        let result = verify_entries(
            vec![
                (root.clone(), Some(0)),
                (parent.clone(), None),
                (subject, None),
            ],
            vec![bump],
            height + 6,
            "mainnet",
        )
        .await;
        assert_eq!(spend_refusal(result), expected);
    }
}

#[tokio::test]
async fn word_preserves_spent_outputs_after_default_script_refusal() {
    let height = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let (root, parent) = signed_payment(0x61);
    let (_, mut spend) = payment("51");
    spend.inputs[0].source_txid = Some(parent.id());
    spend.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("").unwrap());
    let mut respend = spend.clone();
    respend.outputs[0].satoshis = Some(80);
    let bump = proof(&root, height);
    let result = verify_entries(
        vec![
            (root, Some(0)),
            (parent, None),
            (spend, None),
            (respend, None),
        ],
        vec![bump],
        height + 6,
        "mainnet",
    )
    .await;
    assert_eq!(
        spend_refusal(result),
        SpendRefusal::OutputNotAvailable { vout: 0 },
    );
}

/// Witness (c): a pre-Chronicle coin spent today. The spend's word carries
/// CHRONICLE, so the version-1 0x61 signature on the older input passes.
#[tokio::test]
async fn word_accepts_a_chronicle_signature_on_a_pre_chronicle_coin() {
    let height = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let (new_coin, mut child) = signed_payment(0x61);
    let mut old_coin = new_coin.clone();
    old_coin.inputs[0].source_output_index = 1;
    old_coin.invalidate_caches();
    child.inputs.push(TransactionInput::new(old_coin.id(), 0));
    child.outputs[0].satoshis = Some(190);
    sign_input(&new_coin, &mut child, 0, 0x61);
    sign_input(&old_coin, &mut child, 1, 0x61);
    let bumps = vec![proof(&new_coin, height), proof(&old_coin, height - 1)];
    let result = verify_entries(
        vec![(new_coin, Some(0)), (old_coin, Some(1)), (child, None)],
        bumps,
        height + 6,
        "mainnet",
    )
    .await;
    eprintln!("version-1 0x61 on a pre-Chronicle coin spent today: {result:?}");
    assert!(result.is_ok(), "{result:?}");
}

async fn opcode_spend(lock: &str, unlock: &str, coin_height: u32) -> crate::Result<()> {
    let (parent, mut child) = payment(lock);
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_hex(unlock).unwrap());
    let bump = proof(&parent, coin_height);
    verify_entries(
        vec![(parent, Some(0)), (child, None)],
        vec![bump],
        ProtocolEra::MAINNET_CHRONICLE_HEIGHT + 6,
        "mainnet",
    )
    .await
}

/// Witness (c)'s opcode case: the coin's UTXO bits gate the Chronicle
/// opcodes, so a coin proven before Chronicle keeps `OP_2MUL` disabled and
/// `OP_SUBSTR` as `OP_NOP4`, which the standard word discourages.
#[tokio::test]
async fn word_judges_chronicle_opcodes_by_the_coin_era() {
    let chronicle = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    for (lock, unlock, old_coin) in [
        (
            "528d5487",
            "",
            "This opcode is currently disabled. (Opcode: OP_2MUL, PC: 1)",
        ),
        (
            "0052b302616287",
            "03616263",
            "OP_NOP4 is discouraged by verification flags.",
        ),
    ] {
        let verdict = opcode_spend(lock, unlock, chronicle).await;
        assert!(
            verdict.is_ok(),
            "{lock} on a post-Chronicle coin: {verdict:?}"
        );
        script_words(opcode_spend(lock, unlock, chronicle - 1).await, old_coin);
    }
}

/// Witness (d): regtest with its tip between Genesis and Chronicle, so the
/// spend's word lacks CHRONICLE and the 0x61 signature takes the era text.
#[tokio::test]
async fn regtest_post_genesis_tip_refuses_a_chronicle_signature() {
    let (parent, child) = signed_payment(0x61);
    let bump = proof(&parent, 1);
    script_words(
        verify_entries(
            vec![(parent, Some(0)), (child, None)],
            vec![bump],
            ProtocolEra::MAINNET_GENESIS_HEIGHT + 100,
            "regtest",
        )
        .await,
        "The signature must not use SIGHASH_CHRONICLE before Chronicle.",
    );
}

#[tokio::test]
async fn word_uses_the_next_block_for_an_unproven_coin() {
    let (parent, subject) = signed_payment(0x61);
    let (mut root, _) = payment("51");
    root.outputs[0].satoshis = Some(110);
    let mut parent = parent;
    parent.inputs[0].source_txid = Some(root.id());
    parent.inputs[0].unlocking_script = Some(UnlockingScript::from_hex("").unwrap());
    parent.invalidate_caches();
    let mut subject = subject;
    subject.inputs[0].source_txid = Some(parent.id());
    sign_input(&parent, &mut subject, 0, 0x61);
    let bump = proof(&root, HEIGHT);
    // The block after this tip is Chronicle's: the mempool parent and its
    // spender take tip+1's era.
    let result = verify_entries(
        vec![(root, Some(0)), (parent, None), (subject, None)],
        vec![bump],
        ProtocolEra::MAINNET_CHRONICLE_HEIGHT - 1,
        "mainnet",
    )
    .await;
    assert!(result.is_ok(), "{result:?}");
}

/// Regtest models a post-Chronicle tip using the SDK height constants.
#[tokio::test]
async fn word_uses_the_tip_for_testnet_and_regtest_proven_coins() {
    for network in ["testnet", "regtest"] {
        let (parent, child) = signed_payment(0x61);
        let bump = proof(&parent, 1);
        let result = verify_entries(
            vec![(parent, Some(0)), (child, None)],
            vec![bump],
            ProtocolEra::MAINNET_CHRONICLE_HEIGHT,
            network,
        )
        .await;
        assert!(result.is_ok(), "{network}: {result:?}");
    }
}

#[tokio::test]
async fn word_refuses_a_pre_genesis_mainnet_coin() {
    let (parent, child) = signed_payment(0x61);
    let bump = proof(&parent, ProtocolEra::MAINNET_GENESIS_HEIGHT - 1);
    script_words(
        verify_entries(
            vec![(parent, Some(0)), (child, None)],
            vec![bump],
            ProtocolEra::MAINNET_CHRONICLE_HEIGHT,
            "mainnet",
        )
        .await,
        "The mainnet coin height is before Genesis.",
    );
}

#[tokio::test]
async fn word_does_not_rejudge_proven_transaction_inputs() {
    let (_, child) = signed_payment(0x01);
    let bump = proof(&child, HEIGHT);
    let result = verify_entries(vec![(child, Some(0))], vec![bump], HEIGHT + 6, "mainnet").await;
    assert!(result.is_ok(), "{result:?}");
}

/// Runs `internalize_action_internal` on a fresh store of `network`, a wallet
/// payment of output 0 to key 7, `tracker` answering roots and the tip.
async fn internalize_on(
    network: &str,
    tracker: FixtureTracker,
    tx: Vec<u8>,
) -> (
    super::StorageSqlx,
    crate::Result<crate::storage::traits::StorageInternalizeActionResult>,
) {
    use crate::services::mock::MockWalletServices;
    use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
    use bsv_rs::wallet::{InternalizeActionArgs, InternalizeOutput, WalletPayment};

    let key = PrivateKey::from_hex(&format!("{:064x}", 7))
        .unwrap()
        .public_key()
        .to_hex();
    let storage = super::StorageSqlx::new_with_options(
        "sqlite::memory:",
        super::StorageSqlxOptions {
            chain: match network {
                "mainnet" | "main" => crate::services::Chain::Main,
                "testnet" | "test" => crate::services::Chain::Test,
                "regtest" => crate::services::Chain::Regtest,
                _ => panic!("unknown fixture network: {network}"),
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    storage.migrate("word-fixture", &key).await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage.find_or_insert_user(&key).await.unwrap();
    storage.set_services(Arc::new(MockWalletServices::new()));
    storage.set_chain_tracker(Arc::new(tracker)).await;
    let result = super::internalize_action::internalize_action_internal(
        &storage,
        user.user_id,
        InternalizeActionArgs {
            tx,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: "wallet payment".into(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "dGVzdA==".into(),
                    derivation_suffix: "dGVzdA==".into(),
                    sender_identity_key: key,
                }),
                insertion_remittance: None,
            }],
            description: "stored network word fixture".into(),
            labels: None,
            seek_permission: None,
        },
    )
    .await;
    (storage, result)
}

/// Regtest models a post-Chronicle tip; mainnet's proven coin predates Genesis.
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

    for network in ["mainnet", "testnet", "regtest"] {
        let tracker = FixtureTracker {
            roots: vec![(u64::from(height), root)],
            tip: ProtocolEra::MAINNET_CHRONICLE_HEIGHT,
        };
        let (storage, result) = internalize_on(network, tracker, bytes.clone()).await;
        if network == "mainnet" {
            script_words(
                result.map(|_| ()),
                "The mainnet coin height is before Genesis.",
            );
        } else {
            assert!(result.unwrap().base.accepted, "{network}");
        }
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(rows, i64::from(network != "mainnet"), "{network}");
    }
}

mod reader_witnesses;
