//! The wallet's own newly signed transaction under the node's word (#450),
//! where the reference's `verifyUnlockScripts` judges it at its signing
//! (ts-stack@edf6e03 `signer/methods/verifyUnlockScripts.ts:297-353`): each
//! input whose source transaction's bytes are at hand, under the word for
//! it, the coin's era from the source's proof height (the spend's when it
//! has none); an input without source bytes is skipped (:316-321). Only the
//! scripts are judged: the reference runs no mining or value rule here, and
//! the signed transaction is read by bsv-rs's streaming reader alone.

use super::live_word::{input_eras, input_word, judge_input, transaction_cache, Coin, InputEras};
use super::stream_error;
use crate::{Error, Result, SpendRequest, SpendVerificationContext, SpendVerifier};
use bsv_rs::primitives::bsv::sighash::TxSighashCache;
use bsv_rs::primitives::Reader;
use bsv_rs::transaction::beef_stream::{
    display_hex, ChainTip, Element, Hash32, SpendRefusal, TxBody,
};
use bsv_rs::transaction::{Beef, BeefStream, BEEF_V1};
use std::collections::HashMap;
use std::sync::Arc;

/// A source transaction's bytes and, when it is mined, its proof height.
pub(crate) struct SourceTx {
    pub(crate) raw: Vec<u8>,
    pub(crate) proven_height: Option<u64>,
}

/// `raw` read by bsv-rs's streaming reader, its offsets rebased onto `raw`.
fn read_tx(raw: &[u8]) -> Result<(Hash32, TxBody)> {
    let mut beef = Beef::with_version(BEEF_V1);
    beef.merge_raw_tx(raw.to_vec(), None);
    let bytes = beef.to_binary();
    for element in BeefStream::new(&bytes[..]) {
        if let Element::Tx {
            offset,
            txid,
            mut body,
            ..
        } = element.map_err(stream_error)?
        {
            for input in &mut body.inputs {
                input.at -= offset;
            }
            for output in &mut body.outputs {
                output.at -= offset;
            }
            return Ok((txid, body));
        }
    }
    Err(Error::Internal(
        "a signed transaction read no element".into(),
    ))
}

/// The output at `vout` of the raw transaction `raw`: its value and its
/// script bytes as written. A source is read for one output, so no
/// transaction rule (an unproven coinbase, for one) is asked of it.
fn source_output(raw: &[u8], vout: u32) -> Result<Option<(u64, &[u8])>> {
    let mut reader = Reader::new(raw);
    reader.read_bytes(4)?;
    for _ in 0..reader.read_var_int()? {
        reader.read_bytes(36)?;
        reader.read_var_bytes()?;
        reader.read_bytes(4)?;
    }
    for index in 0..reader.read_var_int()? {
        let satoshis = reader.read_u64_le()?;
        let script = reader.read_var_bytes()?;
        if index == u64::from(vout) {
            return Ok(Some((satoshis, script)));
        }
    }
    Ok(None)
}

/// Judge `raw_tx`'s inputs whose source transaction `sources` holds (keyed
/// by display txid) under the word at `tip` (with no tip, the latest era,
/// `input_eras`); the others are skipped. An
/// input `input_eras` places in no era (a mainnet coin mined before
/// Genesis, a coin bsv-rs cannot yet express; a tip or network the interim
/// policy does not read) is skipped too, under the keeper's reading of
/// #461: the signer skips it and the node's verdict at broadcast is the
/// backstop, while the doors refuse it (a skip there could hold a phantom
/// coin), both until bsv-rs #440 adds the pre-Genesis era. A refusal is
/// [`Error::SpendRefused`], its offset within `raw_tx`.
pub(crate) fn verify_own(
    raw_tx: &[u8],
    sources: &HashMap<String, SourceTx>,
    tip: Option<ChainTip>,
    network: &str,
    verifier: Option<&dyn SpendVerifier>,
) -> Result<()> {
    let (txid, body) = read_tx(raw_tx)?;
    let mut judgements = InputJudgements::new(txid, &body, verifier);
    for (vin, input) in body.inputs.iter().enumerate() {
        let Some(source) = sources.get(&display_hex(&input.prev)) else {
            continue;
        };
        let Some((satoshis, script)) = source_output(&source.raw, input.vout)? else {
            return Err(Error::SpendRefused {
                offset: input.at,
                txid: display_hex(&txid),
                input: u32::try_from(vin).ok(),
                why: SpendRefusal::OutputNotAvailable { vout: input.vout },
            });
        };
        // The signer's skip under #461; the doors refuse the same input.
        let Ok(eras) = input_eras(network, tip, source.proven_height) else {
            continue;
        };
        let coin = Coin {
            satoshis,
            script: script.into(),
        };
        judgements.judge(Ok(eras), vin, &coin, source.proven_height)?;
    }
    judgements.finish()
}

/// The per-transaction dispatcher shared by the signing check and live doors.
/// Unselected inputs keep their interpreter verdict; selected inputs can be
/// submitted here or collected into the live door's single verification batch.
pub(super) struct InputJudgements<'a> {
    txid: Hash32,
    body: &'a TxBody,
    transaction: Arc<TxSighashCache>,
    verifier: Option<&'a dyn SpendVerifier>,
    host_body: Option<Arc<TxBody>>,
    selected: Vec<(SpendRequest, SpendVerificationContext)>,
}

impl<'a> InputJudgements<'a> {
    pub(super) fn new(
        txid: Hash32,
        body: &'a TxBody,
        verifier: Option<&'a dyn SpendVerifier>,
    ) -> Self {
        Self {
            txid,
            body,
            transaction: transaction_cache(body),
            verifier,
            host_body: None,
            selected: Vec::new(),
        }
    }

    fn refusal(&self, vin: usize, message: String) -> Error {
        Error::SpendRefused {
            offset: self.body.inputs[vin].at,
            txid: display_hex(&self.txid),
            input: u32::try_from(vin).ok(),
            why: SpendRefusal::Script(message),
        }
    }

    pub(super) fn judge(
        &mut self,
        eras: std::result::Result<InputEras, &'static str>,
        vin: usize,
        source: &Coin,
        proven_height: Option<u64>,
    ) -> Result<()> {
        if let Some(verifier) = self.verifier {
            let eras = eras.map_err(|message| self.refusal(vin, message.into()))?;
            let word = input_word(eras).map_err(|message| self.refusal(vin, message))?;
            let context = SpendVerificationContext {
                word,
                proven_height,
            };
            let body = self
                .host_body
                .get_or_insert_with(|| Arc::new(self.body.clone()));
            let spend = SpendRequest {
                txid: self.txid,
                body: Arc::clone(body),
                vin,
                satoshis: source.satoshis,
                script: source.script.clone(),
            };
            if verifier.should_verify_spend(&spend, &context) {
                self.selected.push((spend, context));
                return Ok(());
            }
        }
        judge_input(
            eras,
            self.txid,
            self.body,
            vin,
            source,
            Arc::clone(&self.transaction),
        )
    }

    pub(super) fn finish(self) -> Result<()> {
        verify_selected(self.verifier, &self.selected)
    }

    pub(super) fn into_selected(self) -> Vec<(SpendRequest, SpendVerificationContext)> {
        self.selected
    }
}

fn host_refusal(spend: &SpendRequest, message: String) -> Error {
    Error::SpendRefused {
        offset: spend.body.inputs[spend.vin].at,
        txid: display_hex(&spend.txid),
        input: u32::try_from(spend.vin).ok(),
        why: SpendRefusal::Script(message),
    }
}

/// One backend call for all selected inputs of this verification, including
/// inputs from different transactions in a live BEEF.
pub(super) fn verify_selected(
    verifier: Option<&dyn SpendVerifier>,
    selected: &[(SpendRequest, SpendVerificationContext)],
) -> Result<()> {
    let (Some(verifier), [(spend, _), ..]) = (verifier, selected) else {
        return Ok(());
    };
    let verdicts = verifier
        .verify_spends_batch(selected)
        .map_err(|error| host_refusal(spend, format!("host script verifier failed: {error}")))?;
    if verdicts.len() != selected.len() {
        return Err(host_refusal(
            spend,
            format!(
                "host script verifier returned {} verdicts for {} spends",
                verdicts.len(),
                selected.len()
            ),
        ));
    }
    for ((spend, _), valid) in selected.iter().zip(verdicts) {
        if !valid {
            return Err(host_refusal(
                spend,
                "host script verifier refused the spend".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "own_word_tests.rs"]
mod tests;
