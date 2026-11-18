//! Step 7 of the received-AtomicBEEF check: the unlocking script of every
//! input of every transaction the BEEF does not prove (the subject and
//! each unproven ancestor) is run against the locking script and satoshis
//! of the output it spends, under the node's mempool word for that input.
//!
//! The rule is the 0.14 line's (toolbox #422, `live_word::input_word` and
//! `judge_input` there), built here from what bsv-rs 0.8 offers
//! (`ProtocolEra`, `ScriptFlags::standard`, `Spend::with_transaction`):
//!
//! - the transactions are run in the order the BEEF carries them, one read
//!   before a parent held until the parent is read (the 0.14 line's
//!   `WordPass`); a transaction the BEEF proves (its own BUMP, or its txid
//!   in a BUMP, whose root step 6 confirmed) is not run: a mined
//!   transaction is the chain's word;
//! - the spend takes the era of the block after the tracker's mining tip,
//!   or with no tip the latest activation the network knows;
//! - the coin takes the era of the height its parent's BUMP proves, an
//!   unproven parent the spend's; a spend or a coin below the network's
//!   Genesis height is not judged, and refused;
//! - each input runs under 2^31 units of work and the node's stack memory
//!   policy (100,000,000 bytes); each transaction's inputs together under
//!   2^31 units (the 0.14 line bounds the BEEF as a whole by nothing more).
//!
//! Every refusal is `spend refused at offset <n>, input <i> of <txid>:
//! <why>`, the 0.14 line's sentence, as a validation error.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use bsv_rs::primitives::bsv::sighash::{TxInput, TxOutput, TxSighashCache};
use bsv_rs::script::{
    LockingScript, ProtocolEra, ScriptFlags, ScriptResource, Spend, TxSpendParams, UnlockingScript,
};
use bsv_rs::transaction::beef_stream::{display_hex, ChainTip, Element, Hash32, TxBody};
use bsv_rs::transaction::{BeefStream, ChainTracker};

use crate::error::{Error, Result};

/// The work bound of one input, in bsv-rs's `work_limit` units (one per
/// opcode reached and one per byte fed to a hash): 2^31, the 0.14 line's
/// `DOOR_WORK_BUDGET.per_input`.
pub(crate) const INPUT_WORK_LIMIT: u64 = 1 << 31;

/// The work bound of one transaction's inputs together: 2^31, the 0.14 line's
/// `DOOR_WORK_BUDGET.per_transaction`.
pub(crate) const TRANSACTION_WORK_LIMIT: u64 = 1 << 31;

/// The stack memory bound of one input, in bytes on the node's count: the
/// SV node's default `-maxstackmemoryusagepolicy`, the 0.14 line's
/// `DOOR_STACK_MEMORY_BYTES`.
pub(crate) const STACK_MEMORY_BYTES: usize = 100_000_000;

/// The refusal text of a network word with no script era policy (the 0.14
/// line's `NO_NETWORK_POLICY`).
pub(crate) const NO_NETWORK_POLICY: &str = "The storage network has no script era policy.";

#[cfg(test)]
thread_local! {
    /// A witness's work bounds `(per input, per transaction)` in place of
    /// the two above, which no test could exhaust; test builds only.
    pub(crate) static WORK_OVERRIDE: std::cell::Cell<Option<(u64, u64)>> =
        const { std::cell::Cell::new(None) };
}

fn work_limits() -> (u64, u64) {
    #[cfg(test)]
    if let Some(limits) = WORK_OVERRIDE.with(std::cell::Cell::get) {
        return limits;
    }
    (INPUT_WORK_LIMIT, TRANSACTION_WORK_LIMIT)
}

/// A network's Genesis and Chronicle activation heights, from a chain word
/// (`main`, `mainnet`, `test`, `testnet`, `regtest`); `None` for any other.
/// The figures are the SV node's chain parameters, as bsv-rs's
/// `ChainParams` carries them on the 0.14 line.
pub(crate) fn activation_heights(network: &str) -> Option<(u32, u32)> {
    match network {
        "main" | "mainnet" => Some((
            ProtocolEra::MAINNET_GENESIS_HEIGHT,
            ProtocolEra::MAINNET_CHRONICLE_HEIGHT,
        )),
        "test" | "testnet" => Some((1_344_302, 1_713_168)),
        "regtest" => Some((10_000, 15_000)),
        _ => None,
    }
}

/// Why an input has no word (the 0.14 line's `NoWord` and the causes of
/// bsv-rs's `NotJudgedCause` this rule reaches), each with its text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NoWord {
    Network,
    TipHeightOverflow { tip: u32 },
    SourceHeightOverflow { height: u64 },
    PreGenesisSpend { height: u32, genesis_height: u32 },
    PreGenesisCoin { height: u32, genesis_height: u32 },
}

impl NoWord {
    fn why(self, input: u32) -> String {
        let cause = match self {
            NoWord::Network => return NO_NETWORK_POLICY.to_string(),
            NoWord::TipHeightOverflow { tip } => format!(
                "tip height {tip} cannot produce a spend height within u32 (maximum {}).",
                u32::MAX
            ),
            NoWord::SourceHeightOverflow { height } => format!(
                "source height {height} cannot be represented as a u32 (maximum {}).",
                u32::MAX
            ),
            NoWord::PreGenesisSpend {
                height,
                genesis_height,
            } => format!("spend height {height} is before Genesis height {genesis_height}."),
            NoWord::PreGenesisCoin {
                height,
                genesis_height,
            } => format!("coin height {height} is before Genesis height {genesis_height}."),
        };
        format!("input {input} not judged: {cause}")
    }
}

/// The node's mempool word for one input: the spend at the block after
/// `tip` (with none, the latest activation the network knows), the coin at
/// `proven_height` (an unproven coin at the spend's height). The
/// transaction-level bits are the spend era's standard word, the two UTXO
/// bits the coin's (bsv-rs's `ChainParams::word_for_input(Word::Standard,
/// ..)` on the 0.14 line).
pub(crate) fn input_word(
    network: &str,
    tip: Option<ChainTip>,
    proven_height: Option<u64>,
) -> std::result::Result<ScriptFlags, NoWord> {
    let (genesis_height, chronicle_height) = activation_heights(network).ok_or(NoWord::Network)?;
    let spend_height = match tip {
        Some(tip) => tip
            .height
            .checked_add(1)
            .ok_or(NoWord::TipHeightOverflow { tip: tip.height })?,
        None => genesis_height.max(chronicle_height),
    };
    let source_height = match proven_height {
        Some(height) => {
            u32::try_from(height).map_err(|_| NoWord::SourceHeightOverflow { height })?
        }
        None => spend_height,
    };
    let era = |height| ProtocolEra::at_height(height, genesis_height, chronicle_height);
    let spend_era = era(spend_height).ok_or(NoWord::PreGenesisSpend {
        height: spend_height,
        genesis_height,
    })?;
    let coin_era = era(source_height).ok_or(NoWord::PreGenesisCoin {
        height: source_height,
        genesis_height,
    })?;
    let coin_bits = ScriptFlags::UTXO_AFTER_GENESIS.union(ScriptFlags::UTXO_AFTER_CHRONICLE);
    let mut source_bits = ScriptFlags::UTXO_AFTER_GENESIS;
    if coin_era.is_chronicle() {
        source_bits = source_bits.union(ScriptFlags::UTXO_AFTER_CHRONICLE);
    }
    Ok(ScriptFlags::standard(spend_era)
        .without(coin_bits)
        .union(source_bits))
}

/// The tracker's mining tip; with none, or a read that faults, the word
/// takes the latest activation (the 0.14 line's no-tip rule).
pub(crate) async fn mining_tip(tracker: &dyn ChainTracker) -> Option<ChainTip> {
    tracker.mining_tip().await.ok().flatten()
}

fn refused(offset: u64, txid: &Hash32, input: u32, why: impl std::fmt::Display) -> Error {
    Error::ValidationError(format!(
        "spend refused at offset {offset}, input {input} of {}: {why}",
        display_hex(txid)
    ))
}

fn stream_fault() -> Error {
    // Step 1 read these bytes whole; a second read that refuses is a fault.
    Error::Internal("the received BEEF could not be read again".into())
}

struct Coin {
    satoshis: u64,
    script: Vec<u8>,
}

/// A transaction of the BEEF as the pass holds it: whole (`raw`) or by its
/// txid alone, with the outputs a later unproven input names and no input
/// of the BEEF has spent yet.
#[derive(Default)]
struct Entry {
    raw: bool,
    outputs: HashMap<u32, Coin>,
}

/// An unproven transaction read before one of its parents: held until
/// every parent is read.
struct Pending {
    txid: Hash32,
    body: TxBody,
    missing: usize,
}

/// The pass over the BEEF's transactions in the order they are read (an
/// unproven one held until its parents are read), the 0.14 line's
/// `WordPass` without its mining and value rules.
struct Pass<'a> {
    network: &'a str,
    tip: Option<ChainTip>,
    heights: HashMap<Hash32, u64>,
    wanted: HashSet<(Hash32, u32)>,
    entries: HashMap<Hash32, Entry>,
    held: HashMap<u64, Pending>,
    waiting: HashMap<Hash32, Vec<u64>>,
    ready: BTreeSet<u64>,
}

/// Runs every input of every transaction of `bytes` that the BEEF does not
/// prove (no BUMP of its own, its txid in no BUMP) against the output it
/// spends, under its word: the subject and every unproven ancestor. A
/// transaction the BEEF proves is not run.
pub(crate) fn verify_unproven_inputs(
    bytes: &[u8],
    network: &str,
    tip: Option<ChainTip>,
) -> Result<()> {
    // One read: the heights the BUMPs prove and the outputs an unproven
    // transaction's inputs name.
    let mut heights: HashMap<Hash32, u64> = HashMap::new();
    let mut unproven: Vec<(Hash32, Vec<(Hash32, u32)>)> = Vec::new();
    for element in BeefStream::new(bytes) {
        match element.map_err(|_| stream_fault())? {
            Element::Bump(bump) => {
                for txid in bump.proven() {
                    heights.entry(*txid).or_insert(bump.block_height);
                }
            }
            Element::Tx {
                txid,
                bump_index: None,
                body,
                ..
            } => unproven.push((txid, body.inputs.iter().map(|i| (i.prev, i.vout)).collect())),
            _ => {}
        }
    }
    let wanted = unproven
        .into_iter()
        .filter(|(txid, _)| !heights.contains_key(txid))
        .flat_map(|(_, spends)| spends)
        .collect();
    let mut pass = Pass {
        network,
        tip,
        heights,
        wanted,
        entries: HashMap::new(),
        held: HashMap::new(),
        waiting: HashMap::new(),
        ready: BTreeSet::new(),
    };
    // Second read: each transaction run once its parents are read.
    for element in BeefStream::new(bytes) {
        pass.fold(element.map_err(|_| stream_fault())?)?;
        while let Some(offset) = pass.ready.pop_first() {
            let pending = pass
                .held
                .remove(&offset)
                .ok_or_else(|| Error::Internal("a ready spend has no held transaction".into()))?;
            pass.accept(pending.txid, false, &pending.body)?;
        }
    }
    if !pass.held.is_empty() {
        return Err(Error::Internal(
            "a checked structure left an unresolved spend".into(),
        ));
    }
    Ok(())
}

impl Pass<'_> {
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
                let proven = bump_index.is_some() || self.heights.contains_key(&txid);
                if !proven {
                    let missing: HashSet<Hash32> = body
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
                                body,
                                missing: missing.len(),
                            },
                        );
                        return Ok(());
                    }
                }
                self.accept(txid, proven, &body)?;
            }
        }
        Ok(())
    }

    fn accept(&mut self, txid: Hash32, proven: bool, body: &TxBody) -> Result<()> {
        if self.entries.get(&txid).is_some_and(|entry| entry.raw) {
            return Ok(());
        }
        if !proven {
            self.spends(txid, body)?;
        }
        for input in &body.inputs {
            self.wanted.remove(&(input.prev, input.vout));
            if let Some(parent) = self.entries.get_mut(&input.prev) {
                parent.outputs.remove(&input.vout);
            }
        }
        let outputs = self.retain_outputs(txid, body);
        let new = self
            .entries
            .insert(txid, Entry { raw: true, outputs })
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
                            script: body.raw[output.script.clone()].to_vec(),
                        },
                    )
                })
            })
            .collect()
    }

    /// Every input of the unproven `txid` run against the output it spends,
    /// in input order, each under the per-input bound, their work together
    /// under the transaction's.
    fn spends(&self, txid: Hash32, body: &TxBody) -> Result<()> {
        // Two inputs of one transaction naming one output: the second has
        // none unspent.
        let mut spent_once: HashSet<(Hash32, u32)> = HashSet::new();
        let mut sources = Vec::with_capacity(body.inputs.len());
        for (vin, input) in body.inputs.iter().enumerate() {
            let index = u32::try_from(vin).unwrap_or(u32::MAX);
            let refusal = |why: String| refused(input.at, &txid, index, why);
            let parent = self
                .entries
                .get(&input.prev)
                .ok_or_else(|| Error::Internal("a checked input has no indexed parent".into()))?;
            if !parent.raw {
                return Err(refusal(
                    "the parent is a txid-only entry: its output is not in the BEEF".into(),
                ));
            }
            let coin = parent
                .outputs
                .get(&input.vout)
                .filter(|_| spent_once.insert((input.prev, input.vout)))
                .ok_or_else(|| {
                    refusal(format!(
                        "the parent has no unspent output at vout {}",
                        input.vout
                    ))
                })?;
            sources.push(coin);
        }
        let transaction = transaction_cache(body);
        let (input_limit, transaction_limit) = work_limits();
        let mut total: u64 = 0;
        for (vin, (input, coin)) in body.inputs.iter().zip(sources).enumerate() {
            let index = u32::try_from(vin).unwrap_or(u32::MAX);
            let refusal = |why: String| refused(input.at, &txid, index, why);
            let proven = self.heights.get(&input.prev).copied();
            let word =
                input_word(self.network, self.tip, proven).map_err(|no| refusal(no.why(index)))?;
            let spent = judge_input(word, body, vin, coin, Arc::clone(&transaction), input_limit)
                .map_err(|why| refusal(why.text(index)))?;
            total = total.saturating_add(spent);
            if total > transaction_limit {
                return Err(refusal(format!(
                    "input {index} not judged: exceeded transaction work budget \
                     {transaction_limit} units after {total} units of work."
                )));
            }
        }
        Ok(())
    }
}

/// Why one input's run refused.
enum Why {
    Script(String),
    Work { budget: u64, work_done: u64 },
    Resource(String),
}

impl Why {
    fn text(self, input: u32) -> String {
        match self {
            Why::Script(message) | Why::Resource(message) => message,
            Why::Work { budget, work_done } => format!(
                "input {input} not judged: exceeded work budget {budget} units \
                 after {work_done} units of work."
            ),
        }
    }
}

fn judge_input(
    word: ScriptFlags,
    body: &TxBody,
    vin: usize,
    coin: &Coin,
    transaction: Arc<TxSighashCache>,
    work_limit: u64,
) -> std::result::Result<u64, Why> {
    let script_error = |message: String| Why::Script(message);
    let locking_script =
        LockingScript::from_binary(&coin.script).map_err(|e| script_error(e.to_string()))?;
    let unlocking_script = UnlockingScript::from_binary(&body.raw[body.inputs[vin].script.clone()])
        .map_err(|e| script_error(e.to_string()))?;
    let mut spend = Spend::with_transaction(TxSpendParams {
        transaction,
        input_index: vin,
        source_satoshis: coin.satoshis,
        locking_script,
        unlocking_script,
        memory_limit: Some(STACK_MEMORY_BYTES),
        work_limit: Some(work_limit),
    })
    .map_err(|e| script_error(e.to_string()))?;
    spend.set_flags(word);
    match spend.validate() {
        Ok(true) => Ok(spend.work_spent()),
        Ok(false) => Err(script_error("the script did not succeed".into())),
        Err(error) => Err(match error.resource_limit {
            Some(limit) => match (limit.resource, limit.work_spent) {
                (ScriptResource::Work, Some(work_done)) => Why::Work {
                    budget: limit.limit,
                    work_done,
                },
                (resource, _) => Why::Resource(resource_text(resource, &limit)),
            },
            None => script_error(error.message),
        }),
    }
}

/// bsv-rs's `ScriptResourceLimit` sentence on the 0.14 line.
fn resource_text(resource: ScriptResource, limit: &bsv_rs::script::ScriptResourceLimit) -> String {
    let bytes = limit.limit;
    match resource {
        ScriptResource::Stack => format!("Stack memory usage has exceeded {bytes} bytes"),
        ScriptResource::AltStack => format!("Alt stack memory usage has exceeded {bytes} bytes"),
        ScriptResource::ElementSize => {
            format!("Script element allocation has exceeded {bytes} bytes")
        }
        ScriptResource::Work => match limit.work_spent {
            Some(spent) => format!("Work budget exhausted: spent {spent} units, budget {bytes}"),
            None => format!("Work budget exhausted: budget {bytes}"),
        },
    }
}

fn transaction_cache(body: &TxBody) -> Arc<TxSighashCache> {
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
#[path = "input_scripts_tests.rs"]
mod tests;
