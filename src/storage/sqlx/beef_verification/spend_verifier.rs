//! Host script verification, matching ts-stack@edf6e03's
//! `SpendVerifierInterface.ts:13-43`. Methods are synchronous in this port.

use crate::Result;
use bsv_rs::script::ScriptFlags;
use bsv_rs::transaction::beef_stream::{Hash32, TxBody};
use bsv_rs::transaction::WorkBudget;
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

/// The work budget at the doors, bsv-rs's one policy type (`WorkBudget`,
/// bsv-rs #519 and #527), in bsv-rs's `SpendParams::work_limit` units (one
/// per opcode reached and one per byte fed to a hash, charged only under a
/// limit): 2^31 per input, the keeper's word of 2026-10-30 on #487, anchored
/// on the node's 1 s validation limit, and the same 2^31 per transaction (the
/// keeper's amendment on #487, the node's limit being per transaction). The
/// signer reads `per_input` alone. The corpus's costliest input spends 661
/// units; a P2PKH input of a 10 MB standard transaction 10,000,235. Defined
/// here, outside the storage's feature gate, so every reader shares it.
pub(crate) const DOOR_WORK_BUDGET: WorkBudget = WorkBudget::new()
    .with_per_input(Some(1 << 31))
    .with_per_transaction(Some(1 << 31));

/// The stack memory figure at the doors, in bytes on the node's count: one
/// budget over the main and the alt stack, each element its bytes plus 32
/// (bsv-rs's `STACK_ELEMENT_OVERHEAD`). It is the SV mainnet node's default
/// `-maxstackmemoryusagepolicy` (`src/policy/policy.h:153`), a policy limit
/// of the node and never a consensus constant: a door judges an unmined spend
/// under the node's word at tip+1, the policy era, so it takes the stricter
/// mainnet node's policy (Teranode's is 104,857,600). The operator's
/// `-maxstackmemoryusageconsensus` knob judges a mined spend and stays
/// outside the door. Passed explicitly as `SpendParams::memory_limit`, where
/// bsv-rs 0.9.0-beta.5 reads `None` under a word as this same figure, so a
/// later bsv-rs reading of `None` cannot move the door; the keeper's reading
/// of 2026-11-01 10:5xZ (#395). Under the mempool word bsv-rs tests its
/// policy verdict at the same figure before it, so no verdict moved. Read by
/// the storage's doors alone, so built with them.
#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub(crate) const DOOR_STACK_MEMORY_BYTES: usize = 100_000_000;

#[cfg(test)]
thread_local! {
    /// The witnesses' budget in place of [`DOOR_WORK_BUDGET`], which no test
    /// could exhaust; test builds only.
    pub(crate) static WORK_OVERRIDE: std::cell::Cell<Option<WorkBudget>> =
        const { std::cell::Cell::new(None) };
}

/// [`DOOR_WORK_BUDGET`], or a witness's override in a test build.
pub(crate) fn work_budget() -> WorkBudget {
    #[cfg(test)]
    if let Some(budget) = WORK_OVERRIDE.with(std::cell::Cell::get) {
        return budget;
    }
    DOOR_WORK_BUDGET
}

#[cfg(test)]
mod tests {
    use super::DOOR_WORK_BUDGET;

    /// The keeper's #487 figures, 2^31 per input and 2^31 per transaction: a
    /// drift in either fails here, where no witness could exhaust them.
    #[test]
    fn the_door_work_budget_is_the_keepers_two_figures() {
        assert_eq!(DOOR_WORK_BUDGET.per_input, Some(2_147_483_648));
        assert_eq!(DOOR_WORK_BUDGET.per_transaction, Some(2_147_483_648));
    }
}
