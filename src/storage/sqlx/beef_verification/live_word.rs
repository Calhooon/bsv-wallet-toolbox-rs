//! The port's per-input word pass under bsv-rs's word (#440): the host
//! `SpendVerifier`, the create pre-check's judged set and the door's
//! per-transaction work total need the inputs one at a time, which bsv-rs's
//! stream verifier does not expose. The SDK can stop at a default-mode script
//! refusal, so this pass also keeps its later mining, outpoint and value
//! checks. Only referenced outputs stay resident; a transaction waiting for a
//! parent is released in wire order.

use super::{stream_error, VerificationHeaders};
use crate::{Error, Result};
use crate::{SpendRequest, SpendVerificationContext, SpendVerifier};
use bsv_rs::block::ChainParams;
use bsv_rs::primitives::bsv::sighash::{TxInput, TxOutput, TxSighashCache};
use bsv_rs::script::{
    input_script_flags, LockingScript, NotJudgedCause, ProtocolEra, ScriptFlags, Spend,
    TxSpendParams, UnlockingScript,
};
use bsv_rs::transaction::beef_stream::{
    display_hex, ChainTip, Element, Hash32, SpendRefusal, TxBody,
};
use bsv_rs::transaction::BeefStream;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Why an input has no word: a storage network the script policy does not
/// read, or bsv-rs's typed not-judged cause (#452).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NoWord {
    Network,
    NotJudged(NotJudgedCause),
}

/// The text of [`NoWord::Network`], a script refusal as before #440.
pub(crate) const NO_NETWORK_POLICY: &str = "The storage network has no script era policy.";

/// The node's word for one unmined input, composed by bsv-rs's
/// `input_script_flags` from the store network's `ChainParams` (#440): the
/// spend at tip+1, the block it would be mined in, with no tip the latest
/// activation the parameters know (bsv-rs's no-tip rule, the keeper's clause
/// of 2026-10-29); the coin's UTXO bits from its verified proof height, an
/// unproven coin at the spend's. This is the one place the word is chosen.
///
/// bsv-rs keeps its typed composition crate-private (`verification_flags`,
/// `checked_input_script_flags`) and its public one answers a text, so the
/// cause of a refusal is named here in the same order from bsv-rs's own era
/// table (`ProtocolEra::at_height`), never from the text: the tip's overflow,
/// the coin height's, then a zero activation, the spend's era, the coin's.
pub(crate) fn input_word(
    network: &str,
    tip: Option<ChainTip>,
    proven_height: Option<u64>,
) -> std::result::Result<ScriptFlags, NoWord> {
    let chain = super::chain_params(network).ok_or(NoWord::Network)?;
    let not_judged = NoWord::NotJudged;
    let spend = match tip {
        Some(tip) => {
            tip.height
                .checked_add(1)
                .ok_or(not_judged(NotJudgedCause::TipHeightOverflow {
                    tip: tip.height,
                }))?
        }
        None => chain.genesis_height.max(chain.chronicle_height),
    };
    let source = proven_height
        .map(|height| {
            u32::try_from(height)
                .map_err(|_| not_judged(NotJudgedCause::SourceHeightOverflow { height }))
        })
        .transpose()?;
    input_script_flags(&chain, spend, source, false)
        .map_err(|_| not_judged(refusal_cause(&chain, spend, source)))
}

/// The cause of `input_script_flags`'s refusal of these facts.
fn refusal_cause(chain: &ChainParams, spend: u32, source: Option<u32>) -> NotJudgedCause {
    use bsv_rs::block::ActivationHeightsError;
    if chain.genesis_height == 0 {
        return NotJudgedCause::ZeroActivationHeight(ActivationHeightsError::ZeroGenesis);
    }
    if chain.chronicle_height == 0 {
        return NotJudgedCause::ZeroActivationHeight(ActivationHeightsError::ZeroChronicle);
    }
    let era = |height| ProtocolEra::at_height(height, chain.genesis_height, chain.chronicle_height);
    match era(spend) {
        None => NotJudgedCause::PreGenesisSpend {
            height: spend,
            genesis_height: chain.genesis_height,
        },
        Some(_) => NotJudgedCause::PreGenesisCoin {
            height: source.unwrap_or(spend),
            genesis_height: chain.genesis_height,
        },
    }
}

pub(super) struct Coin {
    pub(super) satoshis: u64,
    pub(super) script: Box<[u8]>,
}

#[derive(Default)]
struct Entry {
    raw: bool,
    coinbase_height: Option<u64>,
    outputs: HashMap<u32, Coin>,
}

struct Pending {
    txid: Hash32,
    body: TxBody,
    missing: usize,
}

// The source bytes stay with the caller. A deferred entry needs only its
// scripts and field references, as in the SDK's held transaction index.
fn hold_scripts(mut body: TxBody) -> TxBody {
    let mut scripts = Vec::new();
    for range in body
        .inputs
        .iter_mut()
        .map(|input| &mut input.script)
        .chain(body.outputs.iter_mut().map(|output| &mut output.script))
    {
        let start = scripts.len();
        scripts.extend_from_slice(&body.raw[range.clone()]);
        *range = start..scripts.len();
    }
    body.raw = scripts;
    body
}

struct WordPass<'a> {
    network: &'a str,
    judge: &'a (dyn Fn(&Hash32) -> bool + Sync),
    verifier: Option<&'a dyn SpendVerifier>,
    selected: Vec<(SpendRequest, SpendVerificationContext)>,
    tip: Option<ChainTip>,
    heights: HashMap<Hash32, u64>,
    bumps: Vec<u64>,
    wanted: HashSet<(Hash32, u32)>,
    entries: HashMap<Hash32, Entry>,
    held: HashMap<u64, Pending>,
    waiting: HashMap<Hash32, Vec<u64>>,
    ready: BTreeSet<u64>,
}

fn refused(offset: u64, txid: Hash32, input: Option<u32>, why: SpendRefusal) -> Error {
    Error::SpendRefused {
        offset,
        txid: display_hex(&txid),
        input,
        why,
    }
}

pub(super) fn verify(
    bytes: &[u8],
    headers: &VerificationHeaders,
    network: &str,
    verifier: Option<&dyn SpendVerifier>,
) -> Result<()> {
    verify_judging(bytes, headers, network, &|_| true, verifier)
}

/// `verify` for the unproven transactions `judge` names (#426); the others
/// are read for their outputs alone, as a proven transaction is.
pub(super) fn verify_judging(
    bytes: &[u8],
    headers: &VerificationHeaders,
    network: &str,
    judge: &(dyn Fn(&Hash32) -> bool + Sync),
    verifier: Option<&dyn SpendVerifier>,
) -> Result<()> {
    let mut pass = WordPass {
        network,
        judge,
        verifier,
        selected: Vec::new(),
        tip: headers.tip,
        heights: HashMap::new(),
        bumps: Vec::new(),
        wanted: HashSet::new(),
        entries: HashMap::new(),
        held: HashMap::new(),
        waiting: HashMap::new(),
        ready: BTreeSet::new(),
    };
    for element in BeefStream::new(bytes) {
        match element.map_err(stream_error)? {
            Element::Bump(bump) => {
                pass.bumps.push(bump.block_height);
                for txid in bump.proven() {
                    pass.heights.entry(*txid).or_insert(bump.block_height);
                }
            }
            Element::Tx {
                bump_index: None,
                body,
                ..
            } => {
                pass.wanted
                    .extend(body.inputs.iter().map(|i| (i.prev, i.vout)));
            }
            _ => {}
        }
    }
    for element in BeefStream::new(bytes) {
        pass.fold(element.map_err(stream_error)?)?;
        while let Some(offset) = pass.ready.iter().next().copied() {
            pass.ready.remove(&offset);
            let pending = pass
                .held
                .remove(&offset)
                .ok_or_else(|| Error::Internal("ready spend has no held transaction".into()))?;
            pass.accept(offset, pending.txid, None, pending.body)?;
        }
    }
    if !pass.held.is_empty() {
        return Err(Error::Internal(
            "verified structure left an unresolved spend".into(),
        ));
    }
    super::own_word::verify_selected(verifier, &pass.selected)
}

impl WordPass<'_> {
    fn wake(&mut self, txid: Hash32) {
        for offset in self.waiting.remove(&txid).unwrap_or_default() {
            if let Some(pending) = self.held.get_mut(&offset) {
                pending.missing -= 1;
                if pending.missing == 0 {
                    self.ready.insert(offset);
                }
            }
        }
    }

    fn fold(&mut self, element: Element) -> Result<()> {
        match element {
            Element::Bump(_) => {}
            Element::TxidOnly { txid, .. } => {
                if let std::collections::hash_map::Entry::Vacant(slot) = self.entries.entry(txid) {
                    slot.insert(Entry::default());
                    self.wake(txid);
                }
            }
            Element::Tx {
                offset,
                txid,
                bump_index,
                body,
            } => {
                if bump_index.is_none() {
                    let missing: HashSet<_> = body
                        .inputs
                        .iter()
                        .map(|i| i.prev)
                        .filter(|prev| !self.entries.contains_key(prev))
                        .collect();
                    if !missing.is_empty() {
                        for prev in &missing {
                            self.waiting.entry(*prev).or_default().push(offset);
                        }
                        self.held.insert(
                            offset,
                            Pending {
                                txid,
                                body: hold_scripts(body),
                                missing: missing.len(),
                            },
                        );
                        return Ok(());
                    }
                }
                self.accept(offset, txid, bump_index, body)?;
            }
        }
        Ok(())
    }

    fn accept(&mut self, offset: u64, txid: Hash32, bump: Option<u64>, body: TxBody) -> Result<()> {
        if self.entries.get(&txid).is_some_and(|entry| entry.raw) {
            return Ok(());
        }
        if bump.is_none() && (self.judge)(&txid) {
            self.mining_rules(offset, txid, &body)?;
            self.spends(offset, txid, &body)?;
        }
        for input in &body.inputs {
            self.wanted.remove(&(input.prev, input.vout));
            if let Some(parent) = self.entries.get_mut(&input.prev) {
                parent.outputs.remove(&input.vout);
            }
        }
        let outputs = self.retain_outputs(txid, &body);
        let coinbase =
            matches!(body.inputs.as_slice(), [i] if i.prev == [0; 32] && i.vout == u32::MAX);
        let coinbase_height = if coinbase {
            bump.and_then(|index| usize::try_from(index).ok())
                .and_then(|index| self.bumps.get(index).copied())
        } else {
            None
        };
        let new = self
            .entries
            .insert(
                txid,
                Entry {
                    raw: true,
                    coinbase_height,
                    outputs,
                },
            )
            .is_none();
        if new {
            self.wake(txid);
        }
        Ok(())
    }

    fn retain_outputs(&self, txid: Hash32, body: &TxBody) -> HashMap<u32, Coin> {
        body.outputs
            .iter()
            .enumerate()
            .filter_map(|(vout, output)| {
                let vout = u32::try_from(vout).ok()?;
                self.wanted.contains(&(txid, vout)).then(|| {
                    (
                        vout,
                        Coin {
                            satoshis: output.satoshis,
                            script: body.raw[output.script.clone()].into(),
                        },
                    )
                })
            })
            .collect()
    }

    fn mining_rules(&self, offset: u64, txid: Hash32, body: &TxBody) -> Result<()> {
        // Same non-script checks and order as BeefIndex::fold_tx at the pin,
        // and its reading of no tip: P2SH outputs refused, Finality asked.
        // Genesis from the network's ChainParams, which alone decide it since
        // bsv-rs 0.9.0-beta.4 (#451); an unknown network refuses at the word.
        let genesis = super::chain_params(self.network).map_or(0, |chain| chain.genesis_height);
        if self
            .tip
            .is_none_or(|tip| u64::from(tip.height) + 1 >= u64::from(genesis))
        {
            for (vout, output) in body.outputs.iter().enumerate() {
                let script = &body.raw[output.script.clone()];
                if script.len() == 23
                    && script[0] == 0xa9
                    && script[1] == 0x14
                    && script[22] == 0x87
                {
                    return Err(refused(
                        output.at,
                        txid,
                        None,
                        SpendRefusal::P2shOutput {
                            vout: u32::try_from(vout).unwrap_or(u32::MAX),
                        },
                    ));
                }
            }
        }
        if body.lock_time != 0 && body.inputs.iter().any(|i| i.sequence != u32::MAX) {
            let unanswered =
                || super::header_fault("the supplied mining context could not answer Finality");
            let tip = self.tip.ok_or_else(unanswered)?;
            let next_height = u64::from(tip.height) + 1;
            let limit = if body.lock_time < 500_000_000 {
                next_height
            } else if next_height >= u64::from(tip.csv_height) {
                u64::from(tip.median_time_past)
            } else {
                u64::from(tip.next_block_time.ok_or_else(unanswered)?)
            };
            if u64::from(body.lock_time) >= limit {
                return Err(refused(
                    offset,
                    txid,
                    None,
                    SpendRefusal::NonFinal {
                        lock_time: body.lock_time,
                    },
                ));
            }
        }
        Ok(())
    }

    fn sources(&self, txid: Hash32, body: &TxBody) -> Result<(Vec<&Coin>, u64)> {
        // BeefIndex::check_spends_of resolves every input before script execution.
        const MAX_MONEY: u64 = 2_100_000_000_000_000;
        let mut total = 0;
        let mut sources = Vec::with_capacity(body.inputs.len());
        for (vin, input) in body.inputs.iter().enumerate() {
            let refusal = |why| refused(input.at, txid, u32::try_from(vin).ok(), why);
            let parent = self
                .entries
                .get(&input.prev)
                .ok_or_else(|| Error::Internal("verified input has no indexed parent".into()))?;
            if !parent.raw {
                return Err(refusal(SpendRefusal::ParentIsTxidOnly));
            }
            let source = parent
                .outputs
                .get(&input.vout)
                .ok_or_else(|| refusal(SpendRefusal::OutputNotAvailable { vout: input.vout }))?;
            if let Some(height) = parent.coinbase_height {
                let tip = self.tip.ok_or_else(|| {
                    super::header_fault(
                        "the supplied mining context could not answer CoinbaseMaturity",
                    )
                })?;
                if (u64::from(tip.height) + 1)
                    .checked_sub(height)
                    .is_none_or(|depth| depth < 100)
                {
                    return Err(refusal(SpendRefusal::PrematureCoinbase { height }));
                }
            }
            if source.satoshis > MAX_MONEY || source.satoshis > MAX_MONEY - total {
                return Err(refusal(SpendRefusal::InputValueOutOfRange));
            }
            total += source.satoshis;
            sources.push(source);
        }
        Ok((sources, total))
    }

    fn spends(&mut self, offset: u64, txid: Hash32, body: &TxBody) -> Result<()> {
        let (sources, input_total) = self.sources(txid, body)?;
        // One sighash cache per transaction, shared by all of its inputs.
        let mut judgements = super::own_word::InputJudgements::new(txid, body, self.verifier);
        for (vin, source) in sources.into_iter().enumerate() {
            let proven = self.heights.get(&body.inputs[vin].prev).copied();
            let word = input_word(self.network, self.tip, proven);
            judgements.judge(word, vin, source, proven)?;
        }
        self.selected.extend(judgements.into_selected());
        if body.outputs.iter().map(|o| o.satoshis).sum::<u64>() > input_total {
            return Err(refused(offset, txid, None, SpendRefusal::CreatesValue));
        }
        Ok(())
    }
}

/// The refusal of an input with no word, at the input's offset: a door
/// answers not judged (`SpendRefusal::NotJudged`), never invalid (#452).
pub(super) fn no_word_refusal(txid: Hash32, body: &TxBody, vin: usize, why: NoWord) -> Error {
    let input = u32::try_from(vin).unwrap_or(u32::MAX);
    let why = match why {
        NoWord::Network => SpendRefusal::Script(NO_NETWORK_POLICY.into()),
        NoWord::NotJudged(cause) => SpendRefusal::NotJudged { input, cause },
    };
    refused(body.inputs[vin].at, txid, Some(input), why)
}

/// One input under the node's word for it (`input_word`): the per-input
/// judge every route shares. A refusal names the input's offset.
pub(super) fn judge_input(
    word: std::result::Result<ScriptFlags, NoWord>,
    txid: Hash32,
    body: &TxBody,
    vin: usize,
    source: &Coin,
    transaction: Arc<TxSighashCache>,
) -> Result<()> {
    let input = &body.inputs[vin];
    let refusal = |why| refused(input.at, txid, u32::try_from(vin).ok(), why);
    let script_error = |message| refusal(SpendRefusal::Script(message));
    let word = word.map_err(|why| no_word_refusal(txid, body, vin, why))?;
    let locking_script = LockingScript::from_binary(&source.script)
        .map_err(|error| script_error(error.to_string()))?;
    let unlocking_script = UnlockingScript::from_binary(&body.raw[input.script.clone()])
        .map_err(|error| script_error(error.to_string()))?;
    let mut spend = Spend::with_transaction(TxSpendParams {
        transaction,
        input_index: vin,
        source_satoshis: source.satoshis,
        locking_script,
        unlocking_script,
        memory_limit: None,
        work_limit: None,
    })
    .map_err(|error| script_error(error.to_string()))?;
    spend.set_flags(word);
    match spend.validate() {
        Ok(true) => Ok(()),
        Ok(false) => Err(script_error("the script did not succeed".into())),
        Err(error) => Err(match error.resource_limit {
            Some(limit) => refusal(SpendRefusal::Resource(limit)),
            None => script_error(error.message),
        }),
    }
}

pub(super) fn transaction_cache(body: &TxBody) -> Arc<TxSighashCache> {
    let inputs = body
        .inputs
        .iter()
        .map(|i| TxInput {
            txid: i.prev,
            output_index: i.vout,
            script: Vec::new(),
            sequence: i.sequence,
        })
        .collect();
    let outputs = body
        .outputs
        .iter()
        .map(|o| TxOutput {
            satoshis: o.satoshis,
            script: body.raw[o.script.clone()].to_vec(),
        })
        .collect();
    Arc::new(TxSighashCache::new(
        i32::from_le_bytes(body.version.to_le_bytes()),
        inputs,
        outputs,
        body.lock_time,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ProtocolEra::{PostChronicle, PostGenesis};
    const GENESIS: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT;
    const CHRONICLE: u32 = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;

    fn tip(height: u32) -> ChainTip {
        crate::services::chain_tip(&ChainParams::mainnet(), height, 0, 0, None)
    }

    /// The retired composition (`input_word` of `InputEras` before #440),
    /// kept here alone as the cross-check of bsv-rs's: the spend era's
    /// transaction-level bits with the coin era's UTXO bits.
    fn word(spend: ProtocolEra, coin: ProtocolEra) -> ScriptFlags {
        let utxo = ScriptFlags::UTXO_AFTER_GENESIS | ScriptFlags::UTXO_AFTER_CHRONICLE;
        let bits = ScriptFlags::standard(spend).without(utxo).bits()
            | (ScriptFlags::standard(coin).bits() & utxo.bits());
        ScriptFlags::from_bits(bits).unwrap()
    }

    fn not_judged(cause: NotJudgedCause) -> std::result::Result<ScriptFlags, NoWord> {
        Err(NoWord::NotJudged(cause))
    }

    #[test]
    fn mainnet_mined_parent_spender_takes_the_next_block_and_the_proof_height() {
        for (tip_height, height, expected) in [
            (CHRONICLE + 100, GENESIS, word(PostChronicle, PostGenesis)),
            (
                CHRONICLE + 100,
                CHRONICLE - 1,
                word(PostChronicle, PostGenesis),
            ),
            (
                CHRONICLE + 100,
                CHRONICLE,
                word(PostChronicle, PostChronicle),
            ),
            (CHRONICLE - 1, GENESIS, word(PostChronicle, PostGenesis)),
            (CHRONICLE - 2, GENESIS, word(PostGenesis, PostGenesis)),
        ] {
            assert_eq!(
                input_word("main", Some(tip(tip_height)), Some(u64::from(height))),
                Ok(expected)
            );
        }
    }

    #[test]
    fn mainnet_unproven_parent_spender_takes_the_next_block_for_both() {
        for (tip_height, expected) in [
            (GENESIS - 1, word(PostGenesis, PostGenesis)),
            (CHRONICLE - 2, word(PostGenesis, PostGenesis)),
            (CHRONICLE - 1, word(PostChronicle, PostChronicle)),
        ] {
            assert_eq!(
                input_word("main", Some(tip(tip_height)), None),
                Ok(expected)
            );
        }
    }

    /// #440: testnet and regtest take their own activation heights from
    /// `ChainParams` and the coin its own proof height's era. Before, both
    /// read mainnet's heights and gave the coin the spend's era: regtest's
    /// coin proven at 10,000 spent at 15,001 was post-Chronicle, now
    /// post-Genesis.
    #[test]
    fn testnet_and_regtest_read_their_own_heights_and_the_coin_its_own() {
        for (network, genesis, chronicle) in
            [("test", 1_344_302, 1_713_168), ("regtest", 10_000, 15_000)]
        {
            assert_eq!(
                input_word(network, Some(tip(chronicle)), Some(u64::from(genesis))),
                Ok(word(PostChronicle, PostGenesis))
            );
            assert_eq!(
                input_word(network, Some(tip(chronicle - 1)), None),
                Ok(word(PostChronicle, PostChronicle))
            );
            assert_eq!(
                input_word(network, Some(tip(chronicle - 2)), Some(u64::from(genesis))),
                Ok(word(PostGenesis, PostGenesis))
            );
            assert_eq!(
                input_word(network, Some(tip(genesis - 2)), None),
                not_judged(NotJudgedCause::PreGenesisSpend {
                    height: genesis - 1,
                    genesis_height: genesis
                })
            );
        }
    }

    /// #472: the stored words alone are read; the legacy spellings #471
    /// rewrites at `migrate()` take the unknown word's refusal.
    #[test]
    fn the_stored_words_are_read_and_the_legacy_words_refused() {
        for network in ["main", "test", "regtest"] {
            assert!(input_word(network, None, None).is_ok());
        }
        for network in ["mainnet", "testnet", "other"] {
            assert_eq!(input_word(network, None, None), Err(NoWord::Network));
        }
    }

    /// #464 inside bsv-rs's rule: without a tip the spend takes the latest
    /// activation the network's parameters know, a mined coin its proof
    /// height's era and an unproven coin the spend's.
    #[test]
    fn no_tip_gives_the_spend_the_latest_activation() {
        for (network, proven, expected) in [
            ("main", Some(GENESIS), word(PostChronicle, PostGenesis)),
            (
                "main",
                Some(CHRONICLE - 1),
                word(PostChronicle, PostGenesis),
            ),
            ("main", Some(CHRONICLE), word(PostChronicle, PostChronicle)),
            ("main", None, word(PostChronicle, PostChronicle)),
            ("test", None, word(PostChronicle, PostChronicle)),
            ("regtest", Some(10_000), word(PostChronicle, PostGenesis)),
        ] {
            assert_eq!(
                input_word(network, None, proven.map(u64::from)),
                Ok(expected),
                "{network} {proven:?}"
            );
        }
        assert_eq!(
            input_word("main", None, Some(u64::from(GENESIS - 1))),
            not_judged(NotJudgedCause::PreGenesisCoin {
                height: GENESIS - 1,
                genesis_height: GENESIS
            })
        );
    }

    /// Before #440 both were `The mainnet coin height is before Genesis.`,
    /// a script refusal; now bsv-rs's typed causes, the coin's and the spend's.
    #[test]
    fn mainnet_pre_genesis_coin_and_spend_are_not_judged() {
        assert_eq!(
            input_word("main", Some(tip(CHRONICLE)), Some(u64::from(GENESIS - 1))),
            not_judged(NotJudgedCause::PreGenesisCoin {
                height: GENESIS - 1,
                genesis_height: GENESIS
            })
        );
        assert_eq!(
            input_word("main", Some(tip(GENESIS - 2)), None),
            not_judged(NotJudgedCause::PreGenesisSpend {
                height: GENESIS - 1,
                genesis_height: GENESIS
            })
        );
    }

    /// The overflows: before #440, `The coin height has no supported script
    /// era.` for the coin's and a saturated tip for the tip's.
    #[test]
    fn overflowing_heights_are_not_judged() {
        assert_eq!(
            input_word("main", Some(tip(CHRONICLE)), Some(u64::MAX)),
            not_judged(NotJudgedCause::SourceHeightOverflow { height: u64::MAX })
        );
        assert_eq!(
            input_word("main", Some(tip(u32::MAX)), None),
            not_judged(NotJudgedCause::TipHeightOverflow { tip: u32::MAX })
        );
    }

    /// The transaction-level bits follow the spend; the UTXO bits the coin.
    #[test]
    fn the_word_combines_the_spend_and_coin_words() {
        let mixed = input_word("main", Some(tip(CHRONICLE)), Some(u64::from(GENESIS))).unwrap();
        assert!(mixed.contains(ScriptFlags::CHRONICLE | ScriptFlags::GENESIS));
        assert!(mixed.contains(ScriptFlags::UTXO_AFTER_GENESIS));
        assert!(!mixed.contains(ScriptFlags::UTXO_AFTER_CHRONICLE));
        assert_eq!(
            mixed,
            ScriptFlags::standard(PostChronicle).without(ScriptFlags::UTXO_AFTER_CHRONICLE)
        );
        assert!(mixed.check().is_ok());
        for era in [PostGenesis, PostChronicle] {
            assert_eq!(word(era, era), ScriptFlags::standard(era));
        }
    }
}
