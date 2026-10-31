//! The wallet's own newly signed transaction under the node's word (#450),
//! where the reference's `verifyUnlockScripts` judges it at its signing
//! (ts-stack@edf6e03 `signer/methods/verifyUnlockScripts.ts:297-353`): each
//! input whose source transaction's bytes are at hand, under the word for
//! it, the coin's era from the source's proof height (the spend's when it
//! has none); an input without source bytes is skipped (:316-321). Only the
//! scripts are judged: the reference runs no mining or value rule here, and
//! the signed transaction is read by bsv-rs's streaming reader alone.

use super::live_word::{
    input_word, judge_input, no_word_refusal, transaction_cache, work_budget, Coin, NoWord,
};
use super::stream_error;
use crate::{Error, Result, SpendRequest, SpendVerificationContext, SpendVerifier};
use bsv_rs::primitives::bsv::sighash::TxSighashCache;
use bsv_rs::primitives::Reader;
use bsv_rs::script::{NotJudgedCause, ScriptFlags};
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
/// by display txid) under the word at `tip` (with no tip, the latest
/// activation, `input_word`); the others are skipped. An input whose coin
/// bsv-rs places before Genesis (`NotJudgedCause::PreGenesisCoin`) is skipped
/// too, under the keeper's amended reading of #461: the signer skips it and
/// the node's verdict at broadcast is the backstop, while the doors refuse it
/// (a skip there could hold a phantom coin). A refusal is
/// [`Error::SpendRefused`], its offset within `raw_tx`; an input bsv-rs
/// cannot judge for another cause is `SpendRefusal::NotJudged`.
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
        // The signer's skip under the keeper's amended #461 rule: a coin
        // bsv-rs places before Genesis is left to the node; the doors refuse
        // the same input. Every other cause is refused here as at a door.
        let word = input_word(network, tip, source.proven_height);
        if let Err(NoWord::NotJudged(NotJudgedCause::PreGenesisCoin { .. })) = word {
            continue;
        }
        let coin = Coin {
            satoshis,
            script: script.into(),
        };
        match judgements.judge(word, vin, &coin, source.proven_height) {
            // The signer's skip of an input past its work budget, as of a
            // pre-Genesis coin (#461 as amended): the node is the backstop.
            Err(Error::SpendRefused {
                why:
                    SpendRefusal::NotJudged {
                        cause: NotJudgedCause::WorkBudgetExceeded { .. },
                        ..
                    },
                ..
            }) => {}
            other => other?,
        }
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
    /// The transaction's work bound at a door (`WorkBudget::per_transaction`)
    /// and the work its inputs spent.
    transaction_work: Option<u64>,
    work_spent: u64,
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
            transaction_work: None,
            work_spent: 0,
        }
    }

    /// Bound the transaction's total work over the inputs the interpreter
    /// judges (a door's `WorkBudget::per_transaction`; the signer has none).
    pub(super) fn with_transaction_work(mut self, limit: Option<u64>) -> Self {
        self.transaction_work = limit;
        self
    }

    pub(super) fn judge(
        &mut self,
        word: std::result::Result<ScriptFlags, NoWord>,
        vin: usize,
        source: &Coin,
        proven_height: Option<u64>,
    ) -> Result<()> {
        if let Some(verifier) = self.verifier {
            let word = word.map_err(|why| no_word_refusal(self.txid, self.body, vin, why))?;
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
        // bsv-rs's rule (`WorkBudget`, bsv-rs #519): each input runs under the
        // per-input bound alone (unbounded with a transaction bound only) and
        // its work is charged after it; a total above the transaction's bound
        // is `TransactionWorkBudgetExceeded` at the crossing input.
        let budget = work_budget();
        let limit = budget
            .per_input
            .or(self.transaction_work.map(|_| u64::MAX))
            .unwrap_or(u64::MAX);
        let spent = judge_input(
            word,
            self.txid,
            self.body,
            vin,
            source,
            Arc::clone(&self.transaction),
            limit,
        )?;
        self.charge(vin, spent)
    }

    /// The work an input spent added to the transaction's total, saturating,
    /// as bsv-rs's verifier charges it: past the door's bound the crossing
    /// input is not judged, the bound and the total named.
    fn charge(&mut self, vin: usize, spent: u64) -> Result<()> {
        let next = self.work_spent.checked_add(spent);
        self.work_spent = next.unwrap_or(u64::MAX);
        match self.transaction_work {
            Some(limit) if next.is_none() || self.work_spent > limit => {
                let cause = NotJudgedCause::TransactionWorkBudgetExceeded {
                    input: u32::try_from(vin).unwrap_or(u32::MAX),
                    limit,
                    total: self.work_spent,
                };
                Err(no_word_refusal(
                    self.txid,
                    self.body,
                    vin,
                    NoWord::NotJudged(cause),
                ))
            }
            _ => Ok(()),
        }
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

#[cfg(test)]
#[path = "own_word_signing_tests.rs"]
mod signing_tests;
