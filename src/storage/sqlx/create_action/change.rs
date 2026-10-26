use super::allocation::{allocate_change_input_by, release_change_input, ChangePick};
use super::fees::{
    calculate_transaction_size, caller_fixed_fee_required, FixedInput, FixedOutput,
    DEFAULT_FEE_RATE_SAT_PER_KB, P2PKH_LOCKING_SCRIPT_LENGTH, P2PKH_UNLOCKING_SCRIPT_LENGTH,
};
use super::inputs::ExtendedInput;
use super::outputs::ExtendedOutput;
use super::signing::caller_inputs_permit_funding;
use super::AllocatedChangeInput;
use crate::error::{Error, Result};
use crate::rng::RandomSource;
use crate::storage::entities::TableOutputBasket;
use sqlx::sqlite::SqliteConnection;

/// Parameters for change generation.
pub(super) struct GenerateChangeParams {
    pub(super) fixed_inputs: Vec<FixedInput>,
    pub(super) fixed_outputs: Vec<FixedOutput>,
    pub(super) fee_rate: u64,
    pub(super) change_initial_satoshis: u64,
    pub(super) change_first_satoshis: u64,
    pub(super) change_locking_script_length: u32,
    pub(super) change_unlocking_script_length: u32,
    pub(super) target_net_count: Option<i32>,
}

/// Result of change generation.
pub(super) struct GenerateChangeResult {
    pub(super) allocated_change_inputs: Vec<AllocatedChangeInput>,
    pub(super) change_outputs: Vec<ChangeOutput>,
}

/// A generated change output.
#[derive(Debug, Clone)]
pub(super) struct ChangeOutput {
    pub(super) satoshis: u64,
    #[allow(dead_code)]
    pub(super) vout: u32,
    pub(super) derivation_prefix: String,
    pub(super) derivation_suffix: String,
}

/// Fees and change for the action: the caller-complete plan, else
/// `generate_change`, the coins taken as `pick` says. Returns the change and
/// the derivation prefix of its outputs.
#[allow(clippy::too_many_arguments)]
pub(super) async fn plan_change(
    conn: &mut SqliteConnection,
    user_id: i64,
    change_basket: &TableOutputBasket,
    transaction_id: i64,
    args: &bsv_rs::wallet::CreateActionArgs,
    extended_inputs: &[ExtendedInput],
    extended_outputs: &[ExtendedOutput],
    available_change_count: usize,
    is_delayed: bool,
    pick: &mut ChangePick<'_>,
    rng: &RandomSource,
) -> Result<(GenerateChangeResult, String)> {
    let fee_rate = DEFAULT_FEE_RATE_SAT_PER_KB;

    let params = GenerateChangeParams {
        fixed_inputs: extended_inputs
            .iter()
            .map(|i| FixedInput {
                satoshis: i.satoshis,
                unlocking_script_length: i.unlocking_script_length,
            })
            .collect(),
        fixed_outputs: extended_outputs
            .iter()
            .map(|o| FixedOutput {
                satoshis: o.satoshis,
                locking_script_length: o.locking_script.len() as u32,
            })
            .collect(),
        fee_rate,
        change_initial_satoshis: change_basket.minimum_desired_utxo_value as u64,
        change_first_satoshis: std::cmp::max(
            1,
            (change_basket.minimum_desired_utxo_value / 4) as u64,
        ),
        change_locking_script_length: P2PKH_LOCKING_SCRIPT_LENGTH,
        change_unlocking_script_length: P2PKH_UNLOCKING_SCRIPT_LENGTH,
        target_net_count: Some(
            change_basket.number_of_desired_utxos - available_change_count as i32,
        ),
    };

    let derivation_prefix = rng.base64(16);

    // A caller-complete transaction — every provided input already carries a full
    // unlocking script — is decided by two questions (see `caller_complete_plan`):
    //
    // 1. Is it SELF-SUFFICIENT (inputs cover outputs plus the fee the caller
    //    fixed as inputs − outputs)? Then it is byte-frozen and MUST be broadcast
    //    verbatim: no funding input, no change, no re-signing (the signer already
    //    skips inputs that carry an unlocking script). This is the b23a960 path —
    //    a SIGHASH_ALL multi-party covenant spend, an OP_PUSH_TX commit/settle —
    //    where appending anything invalidates every provided signature
    //    (consensus NULLFAIL, ARC 461) and breaks the covenant preimage.
    // 2. Otherwise, do the provided SIGNATURES permit funding? An input signed
    //    ANYONECANPAY|SINGLE (0xc3) or ANYONECANPAY|NONE (0xc2) commits only to
    //    its own input (and, for SINGLE, its same-index output) — precisely so a
    //    wallet can ADD a funding input and a change output afterwards. That is
    //    the Zanaadu V3 publish shape (one covenant input signed 0xc3, 1 sat in,
    //    1,001 sats out: the wallet pays the 1,000-sat platform fee plus the
    //    miner fee), and it is what MetaNet's TS toolbox does with it. When every
    //    provided input is such an input (or carries no signature at all), the
    //    shortfall is funded by `generate_change`, which only APPENDS: provided
    //    inputs keep their indices and order, provided outputs keep theirs, so
    //    every own-index commitment still verifies. A SIGHASH_ALL input
    //    (0x41/0x43, or ALL|ANYONECANPAY 0xc1 whose output set is sealed) can
    //    never be funded — adding an input or an output breaks it — so a
    //    shortfall there stays a hard InsufficientFunds, exactly as before.
    let caller_complete = args
        .inputs
        .as_ref()
        .map(|ins| !ins.is_empty() && ins.iter().all(|i| i.unlocking_script.is_some()))
        .unwrap_or(false);

    let change_result = if caller_complete {
        let in_sats: u64 = extended_inputs.iter().map(|i| i.satoshis).sum();
        let out_sats: u64 = extended_outputs.iter().map(|o| o.satoshis).sum();
        let fundable = caller_inputs_permit_funding(args.inputs.as_deref().unwrap_or(&[]));
        let fee_required = caller_fixed_fee_required(&params);
        match caller_complete_plan(in_sats, out_sats, fee_required, fundable) {
            CallerCompletePlan::Verbatim => {
                // Inputs - outputs is the (caller-fixed) miner fee. No change.
                tracing::debug!(
                    in_sats,
                    out_sats,
                    fundable,
                    "caller-complete tx is self-sufficient — broadcasting verbatim (no funding, no change)"
                );
                GenerateChangeResult {
                    allocated_change_inputs: Vec::new(),
                    change_outputs: Vec::new(),
                }
            }
            CallerCompletePlan::Unfundable => {
                tracing::warn!(
                    in_sats,
                    out_sats,
                    "caller-complete tx has a shortfall but every provided input is signed \
                     SIGHASH_ALL (no ANYONECANPAY): the wallet cannot add a funding input \
                     without invalidating the caller's signatures"
                );
                return Err(Error::InsufficientFunds {
                    needed: out_sats,
                    available: in_sats,
                });
            }
            CallerCompletePlan::Fund => {
                tracing::debug!(
                    in_sats,
                    out_sats,
                    fee_required,
                    "caller-complete tx signed ANYONECANPAY has a shortfall — appending a funding input and change"
                );
                generate_change(
                    &mut *conn,
                    user_id,
                    change_basket.basket_id,
                    transaction_id,
                    &params,
                    &derivation_prefix,
                    is_delayed,
                    pick,
                    rng,
                )
                .await?
            }
        }
    } else {
        generate_change(
            &mut *conn,
            user_id,
            change_basket.basket_id,
            transaction_id,
            &params,
            &derivation_prefix,
            is_delayed,
            pick,
            rng,
        )
        .await?
    };

    Ok((change_result, derivation_prefix))
}

// =============================================================================
// Caller-complete funding rule
// =============================================================================

/// What `create_action` does with a caller-complete transaction (every provided
/// input already carries a full unlocking script).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallerCompletePlan {
    /// Self-sufficient: broadcast the caller's bytes verbatim. No funding input,
    /// no change output. `inputs − outputs` is the miner fee the caller fixed.
    Verbatim,
    /// Short of funds, but every provided signature permits the wallet to append
    /// inputs and outputs: run `generate_change` to fund the shortfall (and the
    /// miner fee) and to collect change. Provided inputs and outputs keep their
    /// indices and order.
    Fund,
    /// Short of funds and at least one provided signature commits to the whole
    /// transaction: appending anything would invalidate it, so the shortfall is
    /// a hard `InsufficientFunds`.
    Unfundable,
}

/// The rule, as a pure function.
///
/// * `fee_required` is the wallet's own fee for the transaction AS PROVIDED
///   (provided inputs and outputs only), so a fundable transaction whose surplus
///   is below it gets a funding input instead of a "fee too low" rejection.
/// * A transaction whose signatures forbid funding is self-sufficient whenever
///   inputs cover outputs: the fee is the caller's to fix, not ours to judge.
fn caller_complete_plan(
    in_sats: u64,
    out_sats: u64,
    fee_required: u64,
    fundable: bool,
) -> CallerCompletePlan {
    let covers_outputs = in_sats >= out_sats;
    let self_sufficient = covers_outputs && (!fundable || in_sats - out_sats >= fee_required);
    if self_sufficient {
        CallerCompletePlan::Verbatim
    } else if fundable {
        CallerCompletePlan::Fund
    } else {
        CallerCompletePlan::Unfundable
    }
}

// =============================================================================
// Change Generation
// =============================================================================

/// Generates change outputs and allocates change inputs to fund the transaction.
///
/// This follows the TypeScript logic:
/// 1. Create change outputs based on targetNetCount first (even if underfunded)
/// 2. Allocate inputs to fund the transaction
/// 3. If can't fund, remove change outputs one at a time
/// 4. Distribute excess to change outputs
#[allow(clippy::too_many_arguments)]
async fn generate_change(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    params: &GenerateChangeParams,
    derivation_prefix: &str,
    is_delayed: bool,
    pick: &mut ChangePick<'_>,
    rng: &RandomSource,
) -> Result<GenerateChangeResult> {
    let mut allocated_inputs: Vec<AllocatedChangeInput> = Vec::new();
    let mut change_outputs: Vec<ChangeOutput> = Vec::new();

    // Calculate initial funding requirement
    let fixed_input_sats: u64 = params.fixed_inputs.iter().map(|i| i.satoshis).sum();
    let fixed_output_sats: u64 = params.fixed_outputs.iter().map(|o| o.satoshis).sum();

    // Helper closure to calculate current state
    let calculate_state = |alloc_inputs: &[AllocatedChangeInput],
                           change_outs: &[ChangeOutput]|
     -> (u64, u64, u64, i64) {
        let input_sats: u64 =
            fixed_input_sats + alloc_inputs.iter().map(|i| i.satoshis).sum::<u64>();
        let output_sats: u64 =
            fixed_output_sats + change_outs.iter().map(|o| o.satoshis).sum::<u64>();

        let input_script_lengths: Vec<u32> = params
            .fixed_inputs
            .iter()
            .map(|i| i.unlocking_script_length)
            .chain(std::iter::repeat_n(
                params.change_unlocking_script_length,
                alloc_inputs.len(),
            ))
            .collect();

        let output_script_lengths: Vec<u32> = params
            .fixed_outputs
            .iter()
            .map(|o| o.locking_script_length)
            .chain(std::iter::repeat_n(
                params.change_locking_script_length,
                change_outs.len(),
            ))
            .collect();

        let size = calculate_transaction_size(&input_script_lengths, &output_script_lengths);
        let fee_required = (size * params.fee_rate).div_ceil(1000); // Ceiling division

        let fee_excess = input_sats as i64 - output_sats as i64 - fee_required as i64;

        (input_sats, output_sats, fee_required, fee_excess)
    };

    let target_net = params.target_net_count.unwrap_or(0);
    let has_target_net_count = params.target_net_count.is_some();

    // Calculate current net change (outputs created - inputs consumed)
    let net_change_count = |outputs: &[ChangeOutput], inputs: &[AllocatedChangeInput]| -> i32 {
        outputs.len() as i32 - inputs.len() as i32
    };

    // Initial state calculation
    let (_, _, _, mut fee_excess) = calculate_state(&allocated_inputs, &change_outputs);

    // Step 1: Create change outputs based on targetNetCount first
    // "If we'd like to have more change outputs create them now.
    //  They may be removed if it turns out we can't fund them."
    while (has_target_net_count
        && target_net > net_change_count(&change_outputs, &allocated_inputs))
        || (change_outputs.is_empty() && fee_excess > 0)
    {
        let satoshis = if change_outputs.is_empty() {
            params.change_first_satoshis
        } else {
            params.change_initial_satoshis
        };

        change_outputs.push(ChangeOutput {
            satoshis,
            vout: (params.fixed_outputs.len() + change_outputs.len()) as u32,
            derivation_prefix: derivation_prefix.to_string(),
            derivation_suffix: rng.base64(16),
        });

        // Recalculate fee_excess with new output
        let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
        fee_excess = new_excess;
    }

    // Step 2: Fund the transaction (starvation loop with funding loop)
    // This is the outer "for (;;)" loop in TypeScript
    #[allow(clippy::never_loop)]
    loop {
        // Release all allocated inputs (TypeScript: releaseAllocatedChangeInputs)
        for input in allocated_inputs.drain(..) {
            release_change_input(&mut *conn, input.output_id, pick).await?;
        }

        // Recalculate after releasing
        let (_, _, _, initial_excess) = calculate_state(&allocated_inputs, &change_outputs);
        fee_excess = initial_excess;

        // Funding loop: add one change input at a time
        while fee_excess < 0 {
            // Check if we should add an output to balance a new input
            let add_output = has_target_net_count
                && (net_change_count(&change_outputs, &allocated_inputs) - 1) < target_net;

            let extra_for_output = if add_output {
                2 * params.change_initial_satoshis
            } else {
                0
            };
            let target_sats = (-fee_excess) as u64 + extra_for_output;

            let allocated = allocate_change_input_by(
                &mut *conn,
                user_id,
                basket_id,
                transaction_id,
                target_sats,
                !is_delayed,
                pick,
            )
            .await?;

            if let Some(input) = allocated {
                allocated_inputs.push(input);
                let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
                fee_excess = new_excess;

                // If we have excess and should add output (or need at least one)
                if fee_excess > 0 && (add_output || change_outputs.is_empty()) {
                    let satoshis = std::cmp::min(
                        fee_excess as u64,
                        if change_outputs.is_empty() {
                            params.change_first_satoshis
                        } else {
                            params.change_initial_satoshis
                        },
                    );
                    change_outputs.push(ChangeOutput {
                        satoshis,
                        vout: (params.fixed_outputs.len() + change_outputs.len()) as u32,
                        derivation_prefix: derivation_prefix.to_string(),
                        derivation_suffix: rng.base64(16),
                    });
                    let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
                    fee_excess = new_excess;
                }
            } else {
                // No more change inputs available
                break;
            }
        }

        // Check if we're done (balanced/overbalanced or impossible)
        if fee_excess >= 0 || change_outputs.is_empty() {
            break;
        }

        // Remove change outputs one at a time (starvation)
        while !change_outputs.is_empty() && fee_excess < 0 {
            change_outputs.pop();
            let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
            fee_excess = new_excess;
        }

        if fee_excess < 0 {
            // Not enough available funding even with no change outputs
            break;
        }

        // Starvation removed all desired change outputs but we have excess sats.
        // Try to create a single change output to capture the excess instead of
        // donating it all to miners. The change output itself costs extra bytes,
        // so verify we still have a positive excess after adding it.
        if change_outputs.is_empty() && fee_excess > 0 {
            change_outputs.push(ChangeOutput {
                satoshis: 1, // Minimum; Step 3 will distribute the real excess
                vout: params.fixed_outputs.len() as u32,
                derivation_prefix: derivation_prefix.to_string(),
                derivation_suffix: rng.base64(16),
            });
            let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
            if new_excess >= 0 {
                fee_excess = new_excess;
            } else {
                // Can't afford a change output; excess goes to miners
                change_outputs.pop();
            }
        }

        break;
    }

    // Check if we still can't fund the transaction
    if fee_excess < 0 {
        let (input_sats, output_sats, fee_required, _) =
            calculate_state(&allocated_inputs, &change_outputs);

        // Check if it's because we need more change outputs to capture excess
        if change_outputs.is_empty() && fee_excess > 0 {
            return Err(Error::InsufficientFunds {
                needed: output_sats + fee_required + params.change_first_satoshis,
                available: input_sats,
            });
        }

        return Err(Error::InsufficientFunds {
            needed: output_sats + fee_required,
            available: input_sats,
        });
    }

    // Step 3: Distribute excess fee across change outputs
    while !change_outputs.is_empty() && fee_excess > 0 {
        if change_outputs.len() == 1 {
            // Give all excess to the single change output
            change_outputs[0].satoshis += fee_excess as u64;
            fee_excess = 0;
        } else if change_outputs[0].satoshis < params.change_initial_satoshis {
            // Fill first output up to initial amount
            let add = std::cmp::min(
                fee_excess as u64,
                params.change_initial_satoshis - change_outputs[0].satoshis,
            );
            change_outputs[0].satoshis += add;
            fee_excess -= add as i64;
        } else {
            // Distribute randomly (simplified: just add to first output)
            let add = std::cmp::max(1, fee_excess / 2);
            change_outputs[0].satoshis += add as u64;
            fee_excess -= add;
        }
    }

    Ok(GenerateChangeResult {
        allocated_change_inputs: allocated_inputs,
        change_outputs,
    })
}
#[cfg(test)]
#[path = "tests/change.rs"]
mod tests;
