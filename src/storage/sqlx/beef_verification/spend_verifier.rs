//! Host script verification, matching ts-stack@edf6e03's
//! `SpendVerifierInterface.ts:13-43`. Methods are synchronous in this port.

use crate::Result;
use bsv_rs::script::ScriptFlags;
use bsv_rs::transaction::beef_stream::{Hash32, TxBody};
use std::sync::Arc;

/// One input and its source output, as read by the script judge.
#[derive(Clone, Debug)]
pub struct SpendRequest {
    /// The spending transaction's hash, in wire order.
    pub txid: Hash32,
    /// Parsed transaction fields and the script bytes their ranges reference.
    /// The live door may retain only scripts in `body.raw`.
    pub body: Arc<TxBody>,
    /// Input index in `body.inputs`.
    pub vin: usize,
    /// Source output value.
    pub satoshis: u64,
    /// Source output locking script bytes.
    pub script: Box<[u8]>,
}

/// The authoritative word for an input, composed by bsv-rs's
/// `ChainParams::word_for_input` from the store network's parameters (#440, bsv-rs #520).
#[derive(Clone, Copy, Debug)]
pub struct SpendVerificationContext {
    /// Spend-era transaction bits joined with coin-era UTXO bits.
    pub word: ScriptFlags,
    /// Source transaction's proven block height, when known.
    pub proven_height: Option<u64>,
}

/// Optional host backend for script judgements. Implementations that need an
/// asynchronous runtime arrange that at their boundary; these calls block.
pub trait SpendVerifier: Send + Sync {
    /// Return the authoritative verdict for this spend and word.
    ///
    /// # Errors
    /// Return an error when the backend cannot produce a verdict.
    fn verify_spend(
        &self,
        spend: &SpendRequest,
        context: &SpendVerificationContext,
    ) -> Result<bool>;

    /// Select this backend; false keeps the bsv-rs interpreter for this input.
    fn should_verify_spend(
        &self,
        _spend: &SpendRequest,
        _context: &SpendVerificationContext,
    ) -> bool {
        true
    }

    /// Informational readiness signal for hosts, as in the reference interface.
    fn is_ready(&self) -> bool {
        true
    }

    /// Verify the selected inputs in order, possibly across transactions. An override must
    /// return one verdict per item or an error; either failure refuses the batch.
    ///
    /// # Errors
    /// The default returns any error from `verify_spend`.
    fn verify_spends_batch(
        &self,
        items: &[(SpendRequest, SpendVerificationContext)],
    ) -> Result<Vec<bool>> {
        items
            .iter()
            .map(|(spend, context)| self.verify_spend(spend, context))
            .collect()
    }
}

impl std::fmt::Debug for dyn SpendVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpendVerifier")
    }
}
