//! Interim second evaluation until bsv-rs #440 supplies the per-input hook.
//! The SDK can stop at a default-mode script refusal, so this pass also keeps
//! its later mining, outpoint and value checks. Only referenced outputs stay
//! resident; a transaction waiting for a parent is released in wire order.

use super::{stream_error, VerificationHeaders};
use crate::{Error, Result};
use bsv_rs::primitives::bsv::sighash::{TxInput, TxOutput, TxSighashCache};
use bsv_rs::script::{
    LockingScript, ProtocolEra, ScriptFlags, Spend, TxSpendParams, UnlockingScript,
};
use bsv_rs::transaction::beef_stream::{
    display_hex, ChainTip, Element, Hash32, SpendRefusal, TxBody,
};
use bsv_rs::transaction::BeefStream;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// The two eras the node reads for one input: the spend's, for the
/// transaction-level bits, and the coin's, for the per-input UTXO bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InputEras {
    pub(crate) spend: ProtocolEra,
    pub(crate) coin: ProtocolEra,
}

/// Every spender takes tip+1's era, the block it would be mined in; a mined
/// parent's coin takes its proven height's era for the UTXO bits and an
/// unproven parent's coin tip+1's. Other networks use the spend's era for the
/// coin until #440 exposes their activation heights.
pub(crate) fn input_eras(
    network: &str,
    tip: ChainTip,
    proven_height: Option<u64>,
) -> std::result::Result<InputEras, &'static str> {
    let mainnet = match network {
        "mainnet" | "main" => true,
        "testnet" | "test" | "regtest" => false,
        _ => return Err("The storage network has no script era policy."),
    };
    let spend_height = tip.height.saturating_add(1);
    let coin = match (mainnet, proven_height) {
        (true, Some(height)) => {
            let height = u32::try_from(height)
                .map_err(|_| "The coin height has no supported script era.")?;
            Some(ProtocolEra::mainnet(height).ok_or("The mainnet coin height is before Genesis.")?)
        }
        _ => None,
    };
    let spend = ProtocolEra::mainnet(spend_height).ok_or(if mainnet {
        "The mainnet coin height is before Genesis."
    } else {
        "The tracker tip height is before the supported Genesis era."
    })?;
    Ok(InputEras {
        spend,
        coin: coin.unwrap_or(spend),
    })
}

/// The per-input bits (`UTXO_AFTER_GENESIS`, `UTXO_AFTER_CHRONICLE`); every
/// other bit of the standard word is the transaction's.
const UTXO_BITS: u32 =
    ScriptFlags::UTXO_AFTER_GENESIS.bits() | ScriptFlags::UTXO_AFTER_CHRONICLE.bits();

/// The node's word for one input: the spend era's transaction-level bits
/// with the coin era's UTXO bits.
pub(crate) fn input_word(eras: InputEras) -> std::result::Result<ScriptFlags, String> {
    let transaction = ScriptFlags::standard(eras.spend).bits() & !UTXO_BITS;
    let input = ScriptFlags::standard(eras.coin).bits() & UTXO_BITS;
    ScriptFlags::from_bits(transaction | input).map_err(|error| error.to_string())
}

struct Coin {
    satoshis: u64,
    script: Box<[u8]>,
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
    tip: ChainTip,
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

pub(super) fn verify(bytes: &[u8], headers: &VerificationHeaders, network: &str) -> Result<()> {
    verify_judging(bytes, headers, network, &|_| true)
}

/// `verify` for the unproven transactions `judge` names (#426); the others
/// are read for their outputs alone, as a proven transaction is.
pub(super) fn verify_judging(
    bytes: &[u8],
    headers: &VerificationHeaders,
    network: &str,
    judge: &(dyn Fn(&Hash32) -> bool + Sync),
) -> Result<()> {
    let mut pass = WordPass {
        network,
        judge,
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
    Ok(())
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
        // Same non-script checks and order as BeefIndex::fold_tx at the pin.
        let next_height = u64::from(self.tip.height) + 1;
        if next_height >= u64::from(self.tip.genesis_height) {
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
            let limit = if body.lock_time < 500_000_000 {
                next_height
            } else if next_height >= u64::from(self.tip.csv_height) {
                u64::from(self.tip.median_time_past)
            } else {
                u64::from(self.tip.next_block_time.ok_or_else(|| {
                    super::header_fault("the supplied mining context could not answer Finality")
                })?)
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
                if (u64::from(self.tip.height) + 1)
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

    fn spends(&self, offset: u64, txid: Hash32, body: &TxBody) -> Result<()> {
        let (sources, input_total) = self.sources(txid, body)?;
        // One sighash cache per transaction, shared by all of its inputs.
        let shared = transaction_cache(body);
        for (vin, source) in sources.into_iter().enumerate() {
            self.input_script(txid, body, vin, source, Arc::clone(&shared))?;
        }
        if body.outputs.iter().map(|o| o.satoshis).sum::<u64>() > input_total {
            return Err(refused(offset, txid, None, SpendRefusal::CreatesValue));
        }
        Ok(())
    }

    fn input_script(
        &self,
        txid: Hash32,
        body: &TxBody,
        vin: usize,
        source: &Coin,
        transaction: Arc<TxSighashCache>,
    ) -> Result<()> {
        let input = &body.inputs[vin];
        let refusal = |why| refused(input.at, txid, u32::try_from(vin).ok(), why);
        let script_error = |message| refusal(SpendRefusal::Script(message));
        let eras = input_eras(
            self.network,
            self.tip,
            self.heights.get(&input.prev).copied(),
        )
        .map_err(|message| script_error(message.into()))?;
        let word = input_word(eras).map_err(script_error)?;
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
mod tests {
    use super::*;
    const GENESIS: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT;
    const CHRONICLE: u32 = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;

    fn tip(height: u32) -> ChainTip {
        ChainTip {
            height,
            median_time_past: 0,
            genesis_height: GENESIS,
            csv_height: 0,
            next_block_time: None,
        }
    }

    fn eras(spend: ProtocolEra, coin: ProtocolEra) -> InputEras {
        InputEras { spend, coin }
    }

    #[test]
    fn mainnet_mined_parent_spender_takes_the_next_block_and_the_proof_height() {
        use ProtocolEra::{PostChronicle, PostGenesis};
        for (tip_height, height, expected) in [
            (CHRONICLE + 100, GENESIS, eras(PostChronicle, PostGenesis)),
            (
                CHRONICLE + 100,
                CHRONICLE - 1,
                eras(PostChronicle, PostGenesis),
            ),
            (
                CHRONICLE + 100,
                CHRONICLE,
                eras(PostChronicle, PostChronicle),
            ),
            (CHRONICLE - 1, GENESIS, eras(PostChronicle, PostGenesis)),
            (CHRONICLE - 2, GENESIS, eras(PostGenesis, PostGenesis)),
        ] {
            for network in ["main", "mainnet"] {
                assert_eq!(
                    input_eras(network, tip(tip_height), Some(u64::from(height))),
                    Ok(expected)
                );
            }
        }
    }

    #[test]
    fn mainnet_unproven_parent_spender_takes_the_next_block_for_both() {
        use ProtocolEra::{PostChronicle, PostGenesis};
        for (tip_height, expected) in [
            (GENESIS - 1, eras(PostGenesis, PostGenesis)),
            (CHRONICLE - 2, eras(PostGenesis, PostGenesis)),
            (CHRONICLE - 1, eras(PostChronicle, PostChronicle)),
        ] {
            assert_eq!(input_eras("mainnet", tip(tip_height), None), Ok(expected));
        }
    }

    #[test]
    fn testnet_and_regtest_take_the_spend_era_for_the_coin() {
        use ProtocolEra::{PostChronicle, PostGenesis};
        for network in ["test", "testnet", "regtest"] {
            assert_eq!(
                input_eras(network, tip(CHRONICLE), Some(1)),
                Ok(eras(PostChronicle, PostChronicle))
            );
            assert_eq!(
                input_eras(network, tip(GENESIS), Some(u64::from(CHRONICLE))),
                Ok(eras(PostGenesis, PostGenesis))
            );
            assert_eq!(
                input_eras(network, tip(CHRONICLE - 1), None),
                Ok(eras(PostChronicle, PostChronicle))
            );
        }
    }

    #[test]
    fn mainnet_pre_genesis_coin_is_refused() {
        for (tip_height, proven) in [
            (CHRONICLE, Some(u64::from(GENESIS - 1))),
            (GENESIS - 2, None),
        ] {
            assert_eq!(
                input_eras("mainnet", tip(tip_height), proven),
                Err("The mainnet coin height is before Genesis.")
            );
        }
    }

    #[test]
    fn unsupported_tip_and_network_are_refused() {
        assert_eq!(
            input_eras("regtest", tip(1), None),
            Err("The tracker tip height is before the supported Genesis era.")
        );
        assert_eq!(
            input_eras("other", tip(CHRONICLE), None),
            Err("The storage network has no script era policy.")
        );
        assert_eq!(
            input_eras("mainnet", tip(CHRONICLE), Some(u64::MAX)),
            Err("The coin height has no supported script era.")
        );
    }

    /// The transaction-level bits follow the spend; the UTXO bits the coin.
    #[test]
    fn input_word_combines_the_spend_and_coin_words() {
        use ProtocolEra::{PostChronicle, PostGenesis};
        let mixed = input_word(eras(PostChronicle, PostGenesis)).unwrap();
        assert!(mixed.contains(ScriptFlags::CHRONICLE | ScriptFlags::GENESIS));
        assert!(mixed.contains(ScriptFlags::UTXO_AFTER_GENESIS));
        assert!(!mixed.contains(ScriptFlags::UTXO_AFTER_CHRONICLE));
        assert_eq!(
            mixed,
            ScriptFlags::standard(PostChronicle).without(ScriptFlags::UTXO_AFTER_CHRONICLE)
        );
        assert!(mixed.check().is_ok());
        for era in [PostGenesis, PostChronicle] {
            assert_eq!(input_word(eras(era, era)), Ok(ScriptFlags::standard(era)));
        }
    }
}
