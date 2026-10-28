//! Create Action Implementation
//!
//! This module contains the full implementation of the `create_action` method
//! for the `StorageSqlx` wallet storage backend.

mod allocation;
mod beef;
mod beef_graph;
mod beef_storage;
mod beef_walk;
mod broadcast;
mod change;
mod fees;
mod inputs;
mod must_spend;
mod outputs;
mod signing;
mod tracker;
mod validation;

use crate::error::{Error, Result, StoreBusyReason};
use crate::storage::entities::TableOutputBasket;
use crate::storage::traits::{
    CreateActionDone, CreateActionDoor, DoorProcessed, DoorRefusal, SignAndProcess,
    StorageCreateActionResult, StorageCreateTransactionInput, StorageCreateTransactionOutput,
    StorageProcessActionArgs, StorageProvidedBy,
};
use bsv_rs::transaction::ChainTracker;
use chrono::Utc;

use super::{beef_verification, proof_root_checks, StorageSqlx};
use beef::build_input_beef;
use change::plan_change;
use fees::P2PKH_UNLOCKING_SCRIPT_LENGTH;
use inputs::{update_output_spent, validate_and_extend_inputs};
use outputs::{
    create_transaction_record, find_or_insert_output_tag, find_or_insert_output_tag_map,
    find_or_insert_tx_label, find_or_insert_tx_label_map, get_transaction_reference, insert_output,
    update_transaction_satoshis, validate_and_extend_outputs,
};
use tracker::{confirm_caller_beef_roots, CachedTracker, TrackerAnswers};
use validation::validate_create_action_args;

#[cfg(feature = "bench-support")]
pub(crate) use allocation::benchmark_allocate_change_input;
pub(super) use allocation::*;
pub(crate) use allocation::{InFlightPicks, PickReservation};
pub(super) use beef_graph::*;
pub(super) use beef_storage::*;
pub(super) use beef_walk::*;
pub(super) use broadcast::rebuild_beef_for_broadcast;
pub(super) use outputs::find_or_insert_output_basket;

#[cfg(test)]
use super::{internalize_action, reorg_tests, sibling_bump_tests};
#[cfg(test)]
use bsv_rs::transaction::{Beef, MerklePath};
#[cfg(test)]
use inputs::ExtendedInput;
#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
#[path = "tests/support.rs"]
mod test_support;
#[cfg(test)]
use test_support::{
    backdate_tx, beef_txids, build_test_beef_with_bump, caller_complete_storage, covenant_input,
    create_spending_tx, fake_der_signature, l2_23_args, l2_23_assembly, l2_23_input, l2_23_storage,
    p2pkh_output, p2pkh_unlock, seed_change_output, seed_change_output_with_status,
    seed_custom_output, seed_proven_chain_tx, seed_proven_tx, seed_raw_tx,
    seed_tagged_change_output, seed_tx_with_input_beef, seed_unproven_tx, synth_bump, synth_chain,
    txid_of,
};

// =============================================================================
// Main Implementation
// =============================================================================

/// Internal implementation of create_action.
///
/// # Arguments
/// * `storage` - The storage backend
/// * `chain_tracker` - Optional chain tracker for BEEF verification. If None, skips verification.
/// * `user_id` - The user ID
/// * `args` - The create action arguments
///
/// The door is one step (bsv-wallet-cli #44; the stack's wallet machine,
/// `Wallet.lean`: ok or refused, and on a refusal the action writes nothing;
/// the pre-check's evidence steps, a root-check record, a demotion of a
/// refuted proof and the default basket, may be written first). Before the
/// write lock, on a reader: the caller's `inputBEEF` roots are read against
/// the tracker, the change coins are picked as the allocation would pick them
/// (marking nothing), and the BEEF those coins need is assembled once so that
/// every stored proof's root is read against the tracker (stillConfirmed).
/// Then, under one `BEGIN IMMEDIATE` transaction: the action is written, the
/// allocation takes only pre-checked coins, and the BEEF is assembled from
/// stored bytes with the tracker's answers already in hand: no service and no
/// tracker is awaited under the lock. A coin taken meanwhile, or a root the
/// pre-check did not read, rolls the transaction back (nothing written) and
/// the pre-check runs once more; the second time a coin taken refuses
/// `StoreBusy` (`MovedUnderUs`): the latest pre-check covered the action,
/// so the refusal is transient, never `InsufficientFunds`; a root unread the
/// second time refuses `ServiceError` naming its height (the header service
/// was not read for it; the store did not move). The coins a
/// pre-check picks are reserved in this process until the call returns
/// (`PickReservation`), so concurrent calls here never pick the same coin;
/// the re-check is for another process's writes. No provider is asked: an
/// ancestor this wallet does not store is a refusal naming it.
///
/// Note: Change outputs are created with empty locking scripts. The locking scripts
/// are derived during transaction signing and stored via process_action when the
/// signed transaction is processed.
pub async fn create_action_internal(
    storage: &StorageSqlx,
    chain_tracker: Option<&dyn ChainTracker>,
    user_id: i64,
    args: bsv_rs::wallet::CreateActionArgs,
) -> Result<StorageCreateActionResult> {
    create_action_at_door_internal(
        storage,
        chain_tracker,
        user_id,
        args,
        CreateActionDoor::default(),
    )
    .await
    .map(|done| done.created)
    .map_err(DoorRefusal::into_error)
}

/// `create_action_internal` with the door's `sign_and_process`: the template
/// is signed by the caller's signer (local CPU, nothing awaited) and
/// processAction's writes (the signed raw tx, the status, the
/// `proven_tx_reqs` row) are made inside the create step's own
/// `BEGIN IMMEDIATE` transaction, committed once with the action's rows
/// (bsv-wallet-cli #44). A busy can arise only at the writer's acquire, at
/// `BEGIN` or at the commit, and every refusal rolls the whole step back:
/// nothing of the action is ever written half. The caller broadcasts after.
pub async fn create_action_at_door_internal(
    storage: &StorageSqlx,
    chain_tracker: Option<&dyn ChainTracker>,
    user_id: i64,
    args: bsv_rs::wallet::CreateActionArgs,
    door_with: CreateActionDoor<'_>,
) -> std::result::Result<CreateActionDone, DoorRefusal> {
    // Step 1: Validate all inputs
    validate_create_action_args(&args)?;
    must_spend::check_listed(door_with.offer, door_with.must_spend)?;

    // Step 2: Get or create default output basket
    // NOTE: This uses its own pool connection (known limitation for first pass).
    let change_basket = storage.find_or_create_default_basket(user_id).await?;

    // The caller's inputBEEF proofs are facts independent of this action:
    // their roots are read against the tracker here, before the write lock
    // (as the reference's `validateRequiredInputs` does, before any
    // allocation). A refused root is refused before anything is picked.
    let answers = TrackerAnswers::confirmed(
        confirm_caller_beef_roots(chain_tracker, args.input_beef.as_deref()).await?,
    );
    // With no caller tracker the walk reads stored proofs against the
    // storage's own, and the assembled roots are not verified.
    let storage_tracker = match chain_tracker {
        Some(_) => None,
        None => storage.get_chain_tracker().await,
    };
    let door = Door {
        storage,
        verify_roots: chain_tracker.is_some(),
        reads_proofs: chain_tracker.is_some() || storage_tracker.is_some(),
        answers: &answers,
        user_id,
        args: &args,
        change_basket: &change_basket,
        then: door_with.sign_and_process.as_ref(),
        offer: door_with.offer,
        must_spend: door_with.must_spend,
    };

    // This call's pre-check picks, reserved until it returns (review 2 F1):
    // a concurrent call of this process never pre-checks the same coin.
    let mut reserved = PickReservation::new(storage.in_flight_picks().clone());
    let mut again = None;
    loop {
        let prechecked = door
            .precheck(chain_tracker, storage_tracker.as_deref(), &mut reserved)
            .await?;
        match door.step(&prechecked).await? {
            Step::Done(result) => return Ok(*result),
            Step::Again(why) => {
                if again.is_some() {
                    return Err(DoorRefusal::Create(match why {
                        // A root the header service was not read for before
                        // the lock, twice: the store did not move, the header
                        // service's answer is missing. A service refusal,
                        // naming the height, never StoreBusy.
                        Again::NotPrechecked { height, root } => Error::ServiceError(format!(
                            "the header service was not read for the merkle root {root} at height {height} before the write lock, twice; nothing written"
                        )),
                        // The latest pre-check covered the action and the
                        // store moved under it again: transient, never a
                        // shortfall.
                        Again::Uncovered(_) => Error::StoreBusy {
                            reason: StoreBusyReason::MovedUnderUs,
                        },
                    }));
                }
                tracing::info!(
                    reason = %why,
                    marker = "create_action_precheck_again",
                    "create_action: the store moved between the pre-check and the write lock; nothing written, checking once more"
                );
                again = Some(why);
            }
        }
    }
}

/// One `createAction` call's door: its arguments and the tracker answers
/// gathered before the write lock.
struct Door<'a> {
    storage: &'a StorageSqlx,
    /// The caller passed a tracker: the assembled BEEF's roots are verified.
    verify_roots: bool,
    /// A tracker (the caller's, else the storage's own) reads stored proofs.
    reads_proofs: bool,
    answers: &'a TrackerAnswers,
    user_id: i64,
    args: &'a bsv_rs::wallet::CreateActionArgs,
    change_basket: &'a TableOutputBasket,
    /// Sign and process in the step's own transaction.
    then: Option<&'a SignAndProcess<'a>>,
    /// The host's offered change coins: nothing outside them is picked.
    offer: Option<&'a [i64]>,
    /// The offered coins the action must spend, ahead of the pick (#353).
    must_spend: &'a [i64],
}

/// What the write-locked step did.
enum Step {
    /// Committed: the response.
    Done(Box<CreateActionDone>),
    /// Rolled back, nothing written: the pre-check must run again.
    Again(Again),
}

/// Why the write-locked step rolled back.
enum Again {
    /// The pre-checked coins no longer cover the action (one was taken
    /// meanwhile, by another process): the allocation's own shortfall.
    Uncovered(Error),
    /// The assembly needed a root the pre-check did not read (a proof
    /// stored meanwhile).
    NotPrechecked { height: u32, root: String },
}

impl std::fmt::Display for Again {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Again::Uncovered(e) => write!(f, "the pre-checked coins were taken: {e}"),
            Again::NotPrechecked { height, root } => write!(
                f,
                "the merkle root {root} at height {height} was not read before the write lock"
            ),
        }
    }
}

impl Door<'_> {
    /// The received-coin rule (R1 (d)) read as every label is stored (#437).
    fn allows_received(&self) -> bool {
        self.args.labels.as_ref().is_some_and(|labels| {
            labels
                .iter()
                .any(|label| super::label::normalize_label(label) == "underreceivedrule")
        })
    }

    fn options(&self) -> Option<&bsv_rs::wallet::CreateActionOptions> {
        self.args.options.as_ref()
    }

    fn is_delayed(&self) -> bool {
        self.options()
            .and_then(|o| o.accept_delayed_broadcast)
            .unwrap_or(false)
    }

    fn return_txid_only(&self) -> bool {
        self.options()
            .and_then(|o| o.return_txid_only)
            .unwrap_or(false)
    }

    fn known_txids(&self) -> Vec<String> {
        self.options()
            .and_then(|o| o.known_txids.as_ref())
            .map(|txids| txids.iter().map(hex::encode).collect())
            .unwrap_or_default()
    }

    fn trust_self(&self) -> bool {
        self.options().and_then(|o| o.trust_self) == Some(bsv_rs::wallet::TrustSelf::Known)
    }

    /// The door's one step, under one `BEGIN IMMEDIATE` transaction: the
    /// action's rows, the allocation from the `prechecked` coins only, and
    /// the BEEF from stored bytes, the tracker answered from the pre-check's
    /// answers, and, with `then`, the signing and processAction's writes.
    /// Nothing but the database is awaited while the lock is held.
    /// Every error, and every `Again`, drops the transaction: rolled back,
    /// nothing written.
    async fn step(&self, prechecked: &[i64]) -> std::result::Result<Step, DoorRefusal> {
        let user_id = self.user_id;
        let args = self.args;
        let change_basket = self.change_basket;
        let is_no_send = self.options().and_then(|o| o.no_send).unwrap_or(false);
        let is_delayed = self.is_delayed();
        // The coins the allocation may take: the pre-checked ones, within
        // the host's offer.
        let allowed: Vec<i64> = prechecked
            .iter()
            .copied()
            .filter(|id| self.offer.is_none_or(|offer| offer.contains(id)))
            .collect();

        // `BEGIN IMMEDIATE` on the writer (bsv-wallet-cli #44). If the
        // function returns an error (via `?`), `tx` is dropped and sqlx
        // rolls back.
        let mut tx = self.storage.write_tx().await?;

        // Step 3: Process caller-provided outputs
        let extended_outputs = validate_and_extend_outputs(args)?;

        // Step 4: Process caller-provided inputs
        let extended_inputs =
            validate_and_extend_inputs(self.storage, &mut tx, user_id, args).await?;

        // Step 5: Count available change outputs for targeting
        let available_change_count = count_change_inputs_under_rule(
            &mut tx,
            user_id,
            change_basket.basket_id,
            !is_delayed,
            self.allows_received(),
        )
        .await?;

        // Step 6: Create transaction record in DB
        let transaction_id =
            create_transaction_record(&mut tx, user_id, args, &self.storage.rng).await?;

        // Step 7: Create transaction labels
        if let Some(ref labels) = args.labels {
            for label in labels {
                let tx_label = find_or_insert_tx_label(&mut tx, user_id, label).await?;
                find_or_insert_tx_label_map(&mut tx, transaction_id, tx_label.label_id).await?;
            }
        }

        // Step 8: Calculate fees and generate change, from the pre-checked
        // coins only. A shortfall here is a coin taken since the pre-check
        // (which funded the same action from the same coins).
        let (change_result, derivation_prefix) = match plan_change(
            &mut tx,
            user_id,
            change_basket,
            transaction_id,
            args,
            &extended_inputs,
            &extended_outputs,
            available_change_count,
            is_delayed,
            &mut ChangePick::Within {
                allowed: &allowed,
                allow_received: self.allows_received(),
                must_spend: self.must_spend,
            },
            &self.storage.rng,
        )
        .await
        {
            Ok(planned) => planned,
            Err(e @ Error::InsufficientFunds { .. }) => {
                return Ok(Step::Again(Again::Uncovered(e)))
            }
            Err(e) => return Err(e.into()),
        };

        // Step 9: Re-read each known input under the write lock, refuse an
        // existing spend, then reserve it (ts-stack@edf6e03 createAction.ts:399-436).
        for input in &extended_inputs {
            update_output_spent(&mut tx, input, transaction_id).await?;
        }

        // Step 10: Calculate net satoshis (change received - change spent)
        let change_out_sats: i64 = change_result
            .change_outputs
            .iter()
            .map(|o| o.satoshis as i64)
            .sum();
        let change_in_sats: i64 = change_result
            .allocated_change_inputs
            .iter()
            .map(|i| i.satoshis as i64)
            .sum();
        let satoshis = change_out_sats - change_in_sats;

        // Update transaction with calculated satoshis
        update_transaction_satoshis(&mut tx, transaction_id, satoshis).await?;

        // Step 11: Create output records
        let mut result_outputs = Vec::new();
        let mut change_vouts = Vec::new();

        // First, handle user-specified outputs
        for xo in &extended_outputs {
            let basket_id = if let Some(ref basket_name) = xo.basket {
                let basket = find_or_insert_output_basket(&mut tx, user_id, basket_name).await?;
                Some(basket.basket_id)
            } else {
                None
            };

            let output_id = insert_output(
                &mut tx,
                user_id,
                transaction_id,
                basket_id,
                xo.vout as i32,
                xo.satoshis as i64,
                &xo.locking_script,
                &xo.output_description,
                &xo.provided_by,
                xo.purpose.as_deref().unwrap_or(""),
                xo.custom_instructions.as_deref(),
                None, // derivation_prefix for user outputs
                xo.derivation_suffix.as_deref(),
                false, // not change
                // spendable = true is CORRECT and INTENTIONAL — do NOT "fix" it.
                // createAction records every output it builds, INCLUDING payments to
                // third parties, with spendable = true. This matches the reference
                // (ts-stack wallet-toolbox createAction.ts makeDefaultOutput defaults
                // spendable: true; the user-output branch does not override it).
                // `spendable = true` does NOT mean "this wallet can spend it" —
                // that is gated by the BASKET (coin selection is basket-scoped;
                // a basket_id = NULL payment is never selected). A usable balance is
                // Σ spendable in the change/default basket, never SUM(spendable=1).
                // See docs/reading-a-balance.md (bsv-wallet-cli). Changing this to a
                // basket-conditional would DIVERGE from the reference — a parity bug.
                true, // spendable (see the note above before touching this)
            )
            .await?;

            // Create tag associations
            for tag in &xo.tags {
                let output_tag = find_or_insert_output_tag(&mut tx, user_id, tag).await?;
                find_or_insert_output_tag_map(&mut tx, output_id, output_tag.tag_id).await?;
            }

            result_outputs.push(StorageCreateTransactionOutput {
                vout: xo.vout,
                satoshis: xo.satoshis,
                locking_script: hex::encode(&xo.locking_script),
                provided_by: xo.provided_by,
                purpose: xo.purpose.clone(),
                derivation_suffix: xo.derivation_suffix.clone(),
                basket: xo.basket.clone(),
                tags: xo.tags.clone(),
                output_description: Some(xo.output_description.clone()),
                custom_instructions: xo.custom_instructions.clone(),
            });
        }

        // Then, handle change outputs
        // Note: Change outputs are created with empty locking scripts here.
        // The locking scripts are derived during transaction signing and stored
        // in process_action when the signed transaction is processed.
        let base_vout = extended_outputs.len() as u32;
        for (i, co) in change_result.change_outputs.iter().enumerate() {
            let vout = base_vout + i as u32;

            let _output_id = insert_output(
                &mut tx,
                user_id,
                transaction_id,
                Some(change_basket.basket_id),
                vout as i32,
                co.satoshis as i64,
                &[], // Locking script stored in process_action after signing
                "",
                &StorageProvidedBy::Storage,
                "change",
                None,
                Some(&co.derivation_prefix),
                Some(&co.derivation_suffix),
                true,  // is change
                false, // not spendable until confirmed
            )
            .await?;

            change_vouts.push(vout);

            result_outputs.push(StorageCreateTransactionOutput {
                vout,
                satoshis: co.satoshis,
                locking_script: String::new(), // Filled in by process_action
                provided_by: StorageProvidedBy::Storage,
                purpose: Some("change".to_string()),
                derivation_suffix: Some(co.derivation_suffix.clone()),
                basket: Some("default".to_string()),
                tags: vec![],
                output_description: None,
                custom_instructions: None,
            });
        }

        // Step 12: Build result inputs
        let mut result_inputs = Vec::new();

        // First, user-specified inputs
        for xi in &extended_inputs {
            let (provided_by, d_prefix, d_suffix, sender_identity_key, input_type) =
                if let Some(ref output) = xi.output {
                    let pb = if output.change {
                        StorageProvidedBy::YouAndStorage
                    } else {
                        StorageProvidedBy::You
                    };
                    (
                        pb,
                        output.derivation_prefix.clone(),
                        output.derivation_suffix.clone(),
                        output.sender_identity_key.clone(),
                        output.output_type.clone(),
                    )
                } else {
                    (
                        StorageProvidedBy::You,
                        None,
                        None,
                        None,
                        "custom".to_string(),
                    )
                };

            result_inputs.push(StorageCreateTransactionInput {
                vin: xi.vin,
                source_txid: xi.txid.clone(),
                source_vout: xi.vout,
                source_satoshis: xi.satoshis,
                source_locking_script: hex::encode(&xi.locking_script),
                source_transaction: None,
                unlocking_script_length: xi.unlocking_script_length,
                provided_by,
                input_type,
                spending_description: xi.input_description.clone(),
                derivation_prefix: d_prefix,
                derivation_suffix: d_suffix,
                sender_identity_key,
            });
        }

        // Then, allocated change inputs
        let base_vin = extended_inputs.len() as u32;
        for (i, aci) in change_result.allocated_change_inputs.iter().enumerate() {
            result_inputs.push(StorageCreateTransactionInput {
                vin: base_vin + i as u32,
                source_txid: aci.txid.clone(),
                source_vout: aci.vout as u32,
                source_satoshis: aci.satoshis,
                source_locking_script: hex::encode(&aci.locking_script),
                source_transaction: None,
                unlocking_script_length: P2PKH_UNLOCKING_SCRIPT_LENGTH,
                provided_by: StorageProvidedBy::Storage,
                input_type: "P2PKH".to_string(),
                spending_description: None,
                derivation_prefix: aci.derivation_prefix.clone(),
                derivation_suffix: aci.derivation_suffix.clone(),
                sender_identity_key: aci.sender_identity_key.clone(),
            });
        }

        // Get the transaction reference
        let reference = get_transaction_reference(&mut tx, transaction_id).await?;

        // Build input BEEF containing all input transactions with their
        // merkle proofs, from stored bytes, on this transaction's
        // connection; the tracker answers from the pre-check, and the walk
        // writes nothing (its read checks were recorded by the pre-check).
        let cached = CachedTracker::new(self.answers);
        let tracker = Some(&cached as &dyn ChainTracker);
        let (verify, read_check) = match (self.verify_roots, self.reads_proofs) {
            (true, _) => (tracker, None),
            (false, true) => (None, tracker),
            (false, false) => (None, None),
        };
        let built = build_input_beef(
            &mut tx,
            None,
            verify,
            &extended_inputs,
            &change_result.allocated_change_inputs,
            args.input_beef.as_deref(),
            &self.known_txids(),
            self.return_txid_only(),
            self.trust_self(),
            read_check,
        )
        .await;
        if let Some((height, root)) = cached.missed() {
            return Ok(Step::Again(Again::NotPrechecked { height, root }));
        }
        let input_beef = built?;

        // Store input_beef in the transaction record (required for process_action)
        // This matches the TypeScript behavior which stores inputBEEF at transaction creation
        if let Some(ref beef_bytes) = input_beef {
            sqlx::query(
                "UPDATE transactions SET input_beef = ?, updated_at = ? WHERE transaction_id = ?",
            )
            .bind(beef_bytes)
            .bind(Utc::now())
            .bind(transaction_id)
            .execute(&mut *tx)
            .await
            .map_err(Error::from)?;
        }

        // Build final result
        let created = StorageCreateActionResult {
            reference,
            version: args.version.unwrap_or(1),
            lock_time: args.lock_time.unwrap_or(0),
            inputs: result_inputs,
            outputs: result_outputs,
            derivation_prefix,
            input_beef,
            no_send_change_output_vouts: if is_no_send { Some(change_vouts) } else { None },
        };

        // Sign and process inside this same transaction: the signer is local
        // CPU, and processAction's writes go on this connection. A refusal
        // here drops `tx`: the action's rows go with it.
        // A template with no inputs is not signed (the caller signs it).
        let processed = match self.then {
            Some(then) if !created.inputs.is_empty() => {
                let raw_tx = (then.sign)(&created).map_err(DoorRefusal::Sign)?;
                let txid = {
                    let mut hash = bsv_rs::primitives::sha256d(&raw_tx);
                    hash.reverse();
                    hex::encode(hash)
                };
                let process = StorageProcessActionArgs {
                    is_new_tx: true,
                    reference: Some(created.reference.clone()),
                    txid: Some(txid.clone()),
                    raw_tx: Some(raw_tx.clone()),
                    ..then.process.clone()
                };
                let results = super::process_action::process_action_writes(
                    self.storage,
                    &mut tx,
                    user_id,
                    &process,
                )
                .await
                .map_err(DoorRefusal::Process)?;
                Some(DoorProcessed {
                    txid,
                    raw_tx,
                    results,
                })
            }
            _ => None,
        };

        super::storage_sqlx::commit(tx).await?;

        Ok(Step::Done(Box::new(CreateActionDone {
            created,
            processed,
        })))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
#[path = "tests/door.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/seed.rs"]
mod seed_tests;

#[cfg(test)]
#[path = "tests/review.rs"]
mod review_tests;
