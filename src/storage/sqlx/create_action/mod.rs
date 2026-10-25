//! Create Action Implementation
//!
//! This module contains the full implementation of the `create_action` method
//! for the `StorageSqlx` wallet storage backend.

use crate::error::{Error, Result, StoreBusyReason};
use crate::services::traits::WalletServices;
use crate::storage::entities::{TableOutput, TableOutputBasket, TableOutputTag, TableTxLabel};
use crate::storage::traits::{
    CreateActionDone, CreateActionDoor, DoorProcessed, DoorRefusal, SignAndProcess,
    StorageCreateActionResult, StorageCreateTransactionInput, StorageCreateTransactionOutput,
    StorageProcessActionArgs, StorageProvidedBy, WalletStorageReader,
};
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath};
use chrono::Utc;
use sqlx::sqlite::{SqliteConnection, SqlitePool};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

use super::storage_sqlx::{
    bump_leaf_index, store_validated_proof_on, StoreProofOutcome, ValidatedProofRow,
};
use super::StorageSqlx;

use super::proof_root_checks;

use super::beef_verification;
#[cfg(test)]
use super::{internalize_action, reorg_tests, sibling_bump_tests};

mod fees;
use fees::*;

mod signing;
use signing::*;

mod validation;
use validation::*;

mod inputs;
use inputs::*;

mod outputs;
pub(super) use outputs::find_or_insert_output_basket;
use outputs::*;

mod change;
use change::*;

mod allocation;
#[cfg(any(test, feature = "bench-support"))]
pub(super) use allocation::allocate_change_input;
#[cfg(feature = "bench-support")]
pub(crate) use allocation::benchmark_allocate_change_input;
pub(super) use allocation::AllocatedChangeInput;
pub(super) use allocation::ChangePick;
pub(crate) use allocation::InFlightPicks;
pub(crate) use allocation::PickReservation;
use allocation::*;

mod tracker;
use tracker::*;

mod beef_graph;
pub(super) use beef_graph::compact_stored_beef;
pub(super) use beef_graph::describe_invalid_beef;
pub(super) use beef_graph::flag_txid_leaf;
pub(super) use beef_graph::link_txids_carried_by_bumps;
pub(super) use beef_graph::merge_proof_for;
pub(super) use beef_graph::prune_beef_to_roots;
use beef_graph::*;

mod beef_storage;
pub(super) use beef_storage::get_stored_beef;
pub(super) use beef_storage::get_tx_with_proof;
pub(super) use beef_storage::parse_input_txids;
pub(super) use beef_storage::read_var_int_for_beef;
pub(super) use beef_storage::BeefTxData;
use beef_storage::*;

mod broadcast;
pub(super) use broadcast::rebuild_beef_for_broadcast;
use broadcast::*;

mod beef_walk;
pub(super) use beef_walk::beef_bfs_walk;
pub(super) use beef_walk::stored_bump_refuted;
use beef_walk::*;

mod beef;
use beef::*;

#[cfg(test)]
#[path = "tests/support.rs"]
mod test_support;
#[cfg(test)]
use test_support::*;

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
        let available_change_count =
            count_change_inputs(&mut tx, user_id, change_basket.basket_id, !is_delayed).await?;

        // Step 6: Create transaction record in DB
        let transaction_id = create_transaction_record(&mut tx, user_id, args).await?;

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
            &mut ChangePick::Within { allowed: &allowed },
        )
        .await
        {
            Ok(planned) => planned,
            Err(e @ Error::InsufficientFunds { .. }) => {
                return Ok(Step::Again(Again::Uncovered(e)))
            }
            Err(e) => return Err(e.into()),
        };

        // Step 9: Mark extended inputs as spent
        for input in &extended_inputs {
            if let Some(ref output) = input.output {
                update_output_spent(
                    &mut tx,
                    output.output_id,
                    transaction_id,
                    input.input_description.as_deref(),
                )
                .await?;
            }
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
mod tests {
    use super::*;
    use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
    use bsv_rs::wallet::{CreateActionOptions, CreateActionOutput};

    // =========================================================================
    // Integration tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_action_basic() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        // Create a user
        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        // Create a simple action with one output
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: locking_script.clone(),
                satoshis: 42000,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, None, user.user_id, args).await;

        // Should fail due to insufficient funds (no change inputs available)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Insufficient funds"));
    }

    #[tokio::test]
    async fn test_create_action_no_send() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test noSend transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 1000,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: Some(CreateActionOptions {
                no_send: Some(true),
                sign_and_process: Some(false),
                accept_delayed_broadcast: None,
                trust_self: None,
                return_txid_only: None,
                known_txids: None,
                no_send_change: None,
                send_with: None,
                randomize_outputs: None,
            }),
        };

        let result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

        // For noSend, we should get change vouts
        assert!(result.no_send_change_output_vouts.is_some());
    }

    #[tokio::test]
    async fn test_create_action_with_version_and_locktime() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test with version/locktime".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 1000,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: Some(500000),
            version: Some(2),
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

        assert_eq!(result.version, 2);
        assert_eq!(result.lock_time, 500000);
    }

    #[tokio::test]
    async fn test_raw_tx_stored_at_create_time() {
        // Verify that after create_action + process_action, the raw_tx is stored
        // on the transactions table (not NULL).
        use crate::storage::traits::{AuthId, StorageProcessActionArgs, WalletStorageWriter};
        use bsv_rs::primitives::sha256d;

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test raw_tx storage".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: locking_script.clone(),
                satoshis: 1000,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let create_result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

        // Build a raw tx with proper script length varint encoding.
        // Must match the number and locking scripts of outputs from create_action.
        let total_outputs = create_result.outputs.len();
        let mut raw_tx = vec![];
        raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        raw_tx.push(0x01); // 1 input (dummy coinbase)
        raw_tx.extend_from_slice(&[0u8; 32]); // txid
        raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // vout
        raw_tx.push(0x00); // empty unlocking script
        raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        raw_tx.push(total_outputs as u8); // output count
        for output in &create_result.outputs {
            // satoshis (8 bytes LE)
            raw_tx.extend_from_slice(&output.satoshis.to_le_bytes());
            // locking script with varint length prefix
            let script_bytes = hex::decode(&output.locking_script).unwrap_or_default();
            raw_tx.push(script_bytes.len() as u8);
            raw_tx.extend_from_slice(&script_bytes);
        }
        raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        // Compute txid from the raw_tx
        let mut txid_bytes = sha256d(&raw_tx).to_vec();
        txid_bytes.reverse();
        let txid = hex::encode(&txid_bytes);

        // Inject input_beef so process_action doesn't reject us
        let dummy_beef = vec![0x00, 0x01, 0x00];
        sqlx::query("UPDATE transactions SET input_beef = ? WHERE reference = ?")
            .bind(&dummy_beef)
            .bind(&create_result.reference)
            .execute(storage.writer())
            .await
            .unwrap();

        let process_args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: true,
            is_delayed: false,
            reference: Some(create_result.reference.clone()),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx.clone()),
            send_with: vec![],
        };

        let auth_id = AuthId::with_user_id("02user_identity_key", user.user_id);
        let _process_result = storage
            .process_action(&auth_id, process_args)
            .await
            .unwrap();

        // Now verify that raw_tx is stored on the transactions table
        let row = sqlx::query("SELECT raw_tx FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();

        let stored_raw_tx: Option<Vec<u8>> = row.get("raw_tx");
        assert!(
            stored_raw_tx.is_some(),
            "raw_tx must NOT be NULL on the transactions table after process_action"
        );
        assert_eq!(
            stored_raw_tx.unwrap(),
            raw_tx,
            "stored raw_tx must match the signed transaction"
        );
    }

    // =========================================================================
    // trustSelf 'known': a caller's txid-only entry the wallet's own storage
    // holds is resolved from storage before `verify_valid`
    // (ts-stack@edf6e03 wallet-toolbox src/storage/methods/createAction.ts:876,
    // 906-911, 923-963, 996-1002; Wallet.ts:1151)
    // =========================================================================

    mod trust_self_known {
        use super::*;
        use bsv_rs::script::{LockingScript, UnlockingScript};
        use bsv_rs::transaction::{
            AlwaysValidChainTracker, Transaction, TransactionInput, TransactionOutput,
        };
        use bsv_rs::wallet::{CreateActionInput, Outpoint, TrustSelf};

        const LOCK: &str = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";

        /// A one-input, one-output transaction spending `parent:0` (or a
        /// coinbase-shaped outpoint when `parent` is `None`), unique by `tag`.
        fn tx_spending(parent: Option<&str>, tag: u8, satoshis: u64) -> Transaction {
            Transaction::with_params(
                1,
                vec![TransactionInput {
                    source_transaction: None,
                    source_txid: Some(
                        parent.map_or_else(|| format!("{:02x}", tag).repeat(32), str::to_string),
                    ),
                    source_output_index: if parent.is_some() { 0 } else { 0xffff_ffff },
                    unlocking_script: Some(
                        UnlockingScript::from_hex(&format!("01{:02x}", tag)).unwrap(),
                    ),
                    unlocking_script_template: None,
                    sequence: 0xffff_ffff,
                }],
                vec![TransactionOutput {
                    satoshis: Some(satoshis),
                    locking_script: LockingScript::from_hex(LOCK).unwrap(),
                    change: false,
                }],
                0,
            )
        }

        /// A one-leaf BUMP proving `txid` at `height`.
        fn bump_for(txid: &str, height: u32) -> MerklePath {
            MerklePath {
                block_height: height,
                path: vec![vec![bsv_rs::transaction::MerklePathLeaf {
                    offset: 0,
                    hash: Some(txid.to_string()),
                    txid: true,
                    duplicate: false,
                }]],
            }
        }

        async fn setup() -> (StorageSqlx, i64) {
            let storage = StorageSqlx::in_memory().await.unwrap();
            storage.migrate("trust-self", "02test_key").await.unwrap();
            storage.make_available().await.unwrap();
            let (user, _) = storage
                .find_or_insert_user("02trust_self_user")
                .await
                .unwrap();
            (storage, user.user_id)
        }

        /// The wallet's own record of `tx`: its bytes, its output 0 (the
        /// user's, spendable, not change) and, when `proven`, its proof.
        async fn seed_known(storage: &StorageSqlx, user_id: i64, tx: &Transaction, proven: bool) {
            let txid = tx.id();
            let raw = tx.to_binary();
            if proven {
                seed_proven_tx(storage, &txid, &raw, &bump_for(&txid, 900).to_binary()).await;
            }
            let now = Utc::now();
            let row = sqlx::query(
                r#"
                INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
                VALUES (?, 'completed', ?, 0, ?, 1, 0, 'known to the wallet', ?, ?, ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(format!("ref-{}", &txid[..8]))
            .bind(tx.outputs[0].satoshis.unwrap() as i64)
            .bind(&txid)
            .bind(&raw)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                                     txid, type, spendable, change, provided_by, purpose,
                                     output_description, created_at, updated_at)
                VALUES (?, ?, NULL, 0, ?, ?, ?, 'custom', 1, 0, 'you', '', 'known output', ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(row.last_insert_rowid())
            .bind(tx.outputs[0].satoshis.unwrap() as i64)
            .bind(hex::decode(LOCK).unwrap())
            .bind(&txid)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
        }

        fn input(txid: &str) -> CreateActionInput {
            CreateActionInput {
                outpoint: Outpoint::from_string(&format!("{}.0", txid)).unwrap(),
                input_description: "an input".to_string(),
                unlocking_script: None,
                unlocking_script_length: Some(107),
                sequence_number: None,
            }
        }

        fn args(inputs: Vec<CreateActionInput>, beef: Vec<u8>) -> bsv_rs::wallet::CreateActionArgs {
            bsv_rs::wallet::CreateActionArgs {
                description: "trust self known".to_string(),
                input_beef: Some(beef),
                inputs: Some(inputs),
                outputs: Some(vec![CreateActionOutput {
                    locking_script: hex::decode(LOCK).unwrap(),
                    satoshis: 1_000,
                    output_description: "an output".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: None,
                options: Some(CreateActionOptions {
                    trust_self: Some(TrustSelf::Known),
                    ..Default::default()
                }),
            }
        }

        /// The caller's BEEF: `proven` with its own BUMP, then a txid-only
        /// entry for each of `txid_only`.
        fn caller_beef(proven: &Transaction, txid_only: &[String]) -> Vec<u8> {
            let mut beef = Beef::new();
            let bi = beef.merge_bump(bump_for(&proven.id(), 800));
            beef.merge_raw_tx(proven.to_binary(), Some(bi));
            for t in txid_only {
                beef.merge_txid_only(t.clone());
            }
            beef.to_binary()
        }

        /// The witness: the caller's `inputBEEF` carries one BUMP-proven
        /// transaction and one txid-only entry for a transaction the wallet
        /// holds with its proof, both direct inputs, under a chain tracker.
        /// 0.7.0 on bsv-rs 0.4.1 built it; 0.7.1 at 4cd8ee7 refused it
        /// (`StubNotProven` at `verify_valid`). Storage's proof replaces the
        /// entry: the BEEF built carries no txid-only entry.
        #[tokio::test]
        async fn a_known_proven_txid_only_input_is_resolved_from_storage() {
            let (storage, user_id) = setup().await;
            let external = tx_spending(None, 0xaa, 5_000);
            let known = tx_spending(None, 0xcc, 5_000);
            seed_known(&storage, user_id, &known, true).await;

            let tracker = AlwaysValidChainTracker::new(100_000);
            let result = create_action_internal(
                &storage,
                Some(&tracker),
                user_id,
                args(
                    vec![input(&external.id()), input(&known.id())],
                    caller_beef(&external, &[known.id()]),
                ),
            )
            .await
            .expect("a txid-only entry the wallet holds is honoured under trustSelf known");

            let mut beef = Beef::from_binary(&result.input_beef.unwrap()).unwrap();
            assert!(
                beef.txs.iter().all(|t| !t.is_txid_only()),
                "no txid-only entry rides"
            );
            assert!(
                beef.find_txid(&known.id()).unwrap().bump_index().is_some(),
                "storage's proof rides"
            );
            assert!(beef.verify_valid(false).valid);
        }

        /// Storage holds the bytes but no proof: the bytes ride, and the walk
        /// carries the transaction's proven parent from storage.
        #[tokio::test]
        async fn a_known_unproven_txid_only_input_rides_with_its_stored_ancestry() {
            let (storage, user_id) = setup().await;
            let external = tx_spending(None, 0xaa, 5_000);
            let grandparent = tx_spending(None, 0xdd, 9_000);
            seed_known(&storage, user_id, &grandparent, true).await;
            let known = tx_spending(Some(&grandparent.id()), 0xcc, 5_000);
            seed_known(&storage, user_id, &known, false).await;

            let tracker = AlwaysValidChainTracker::new(100_000);
            let result = create_action_internal(
                &storage,
                Some(&tracker),
                user_id,
                args(
                    vec![input(&external.id()), input(&known.id())],
                    caller_beef(&external, &[known.id()]),
                ),
            )
            .await
            .expect("the wallet's own unproven transaction is resolved with its ancestry");

            let mut beef = Beef::from_binary(&result.input_beef.unwrap()).unwrap();
            assert!(beef.txs.iter().all(|t| !t.is_txid_only()));
            assert!(beef.find_txid(&known.id()).unwrap().raw_tx().is_some());
            assert!(beef
                .find_txid(&grandparent.id())
                .unwrap()
                .bump_index()
                .is_some());
            assert!(beef.verify_valid(false).valid);
        }

        /// Without trustSelf the same shape is refused as before: the BEEF
        /// must prove every txid-only entry it carries.
        #[tokio::test]
        async fn without_trust_self_the_shape_is_refused() {
            let (storage, user_id) = setup().await;
            let external = tx_spending(None, 0xaa, 5_000);
            let known = tx_spending(None, 0xcc, 5_000);
            seed_known(&storage, user_id, &known, true).await;

            let mut a = args(
                vec![input(&external.id()), input(&known.id())],
                caller_beef(&external, &[known.id()]),
            );
            a.options = None;
            let tracker = AlwaysValidChainTracker::new(100_000);
            let err = create_action_internal(&storage, Some(&tracker), user_id, a)
                .await
                .expect_err("no trustSelf, no vouching");
            assert!(
                err.to_string().contains("BEEF structure is invalid"),
                "{err}"
            );
        }

        /// trustSelf vouches only for what storage holds: a txid-only
        /// ancestor storage does not know is refused, and the reason says so.
        #[tokio::test]
        async fn an_unknown_txid_only_entry_is_refused() {
            let (storage, user_id) = setup().await;
            let external = tx_spending(None, 0xaa, 5_000);
            let stranger = tx_spending(None, 0xee, 9_000);
            let unproven = tx_spending(Some(&stranger.id()), 0xbb, 5_000);

            let mut beef = Beef::from_binary(&caller_beef(&external, &[stranger.id()])).unwrap();
            beef.merge_raw_tx(unproven.to_binary(), None);

            let tracker = AlwaysValidChainTracker::new(100_000);
            let err = create_action_internal(
                &storage,
                Some(&tracker),
                user_id,
                args(
                    vec![input(&external.id()), input(&unproven.id())],
                    beef.to_binary(),
                ),
            )
            .await
            .expect_err("an entry storage does not know is not vouched for");
            let msg = err.to_string();
            assert!(
                msg.contains(&stranger.id()),
                "the refusal names the entry: {msg}"
            );
            assert!(
                msg.contains("not known to this wallet"),
                "the reason: {msg}"
            );
        }

        // =====================================================================
        // No tracker await under the write lock (bsv-wallet-cli #44)
        // =====================================================================

        /// Answers every root as `refuse` says and records, for each ask,
        /// whether the storage's writer connection was held at that moment.
        struct ProbeTracker {
            writer: sqlx::SqlitePool,
            refuse_height: Option<u32>,
            asked: std::sync::atomic::AtomicUsize,
            asked_under_lock: std::sync::atomic::AtomicUsize,
        }

        impl ProbeTracker {
            fn new(storage: &StorageSqlx, refuse_height: Option<u32>) -> Self {
                Self {
                    writer: storage.writer().clone(),
                    refuse_height,
                    asked: Default::default(),
                    asked_under_lock: Default::default(),
                }
            }
            fn asked(&self) -> usize {
                self.asked.load(std::sync::atomic::Ordering::SeqCst)
            }
            fn asked_under_lock(&self) -> usize {
                self.asked_under_lock
                    .load(std::sync::atomic::Ordering::SeqCst)
            }
        }

        #[async_trait::async_trait]
        impl ChainTracker for ProbeTracker {
            async fn is_valid_root_for_height(
                &self,
                _root: &str,
                height: u32,
            ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
                use std::sync::atomic::Ordering::SeqCst;
                self.asked.fetch_add(1, SeqCst);
                // The writer is one connection. Held by the caller that is
                // awaiting this answer, it can never be acquired here; a
                // connection on its way back to the pool is, at once.
                let probe =
                    tokio::time::timeout(std::time::Duration::from_secs(2), self.writer.acquire())
                        .await;
                if !matches!(probe, Ok(Ok(_))) {
                    self.asked_under_lock.fetch_add(1, SeqCst);
                }
                Ok(self.refuse_height != Some(height))
            }

            async fn current_height(
                &self,
            ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
                Ok(100_000)
            }
        }

        async fn count(storage: &StorageSqlx, sql: &str) -> i64 {
            sqlx::query_scalar(sql)
                .fetch_one(storage.pool())
                .await
                .unwrap()
        }

        /// Control: the probe sees a writer held by an open write transaction.
        #[tokio::test]
        async fn control_the_probe_sees_a_held_writer() {
            let (storage, _) = setup().await;
            let tracker = ProbeTracker::new(&storage, None);
            let tx = storage.write_tx().await.unwrap();
            tracker.is_valid_root_for_height("00", 1).await.unwrap();
            assert_eq!(tracker.asked_under_lock(), 1);
            tx.commit().await.unwrap();
            tracker.is_valid_root_for_height("00", 1).await.unwrap();
            assert_eq!(tracker.asked_under_lock(), 1, "free again after the commit");
        }

        /// The caller's root (height 800) and the stored proof's (900) are
        /// both read against the tracker, and never while the writer is held.
        #[tokio::test]
        async fn the_tracker_is_never_asked_while_the_writer_is_held() {
            let (storage, user_id) = setup().await;
            let external = tx_spending(None, 0xaa, 5_000);
            let known = tx_spending(None, 0xcc, 5_000);
            seed_known(&storage, user_id, &known, true).await;

            let tracker = ProbeTracker::new(&storage, None);
            create_action_internal(
                &storage,
                Some(&tracker),
                user_id,
                args(
                    vec![input(&external.id()), input(&known.id())],
                    caller_beef(&external, &[known.id()]),
                ),
            )
            .await
            .expect("the action is created");
            assert!(tracker.asked() >= 2, "asked {}", tracker.asked());
            assert_eq!(tracker.asked_under_lock(), 0);
        }

        /// A caller's root the tracker refuses is refused before the write
        /// transaction opens: nothing is allocated, nothing is written.
        #[tokio::test]
        async fn a_caller_root_the_tracker_refuses_is_refused_before_any_allocation() {
            let (storage, user_id) = setup().await;
            let external = tx_spending(None, 0xaa, 5_000);
            let known = tx_spending(None, 0xcc, 5_000);
            seed_known(&storage, user_id, &known, true).await;

            let tracker = ProbeTracker::new(&storage, Some(800));
            let err = create_action_internal(
                &storage,
                Some(&tracker),
                user_id,
                args(
                    vec![input(&external.id()), input(&known.id())],
                    caller_beef(&external, &[known.id()]),
                ),
            )
            .await
            .expect_err("the caller's root is refused");
            assert!(err.to_string().contains("invalid merkle root"), "{err}");
            assert_eq!(tracker.asked(), 1, "only the caller's root was asked");
            assert_eq!(tracker.asked_under_lock(), 0);
            assert_eq!(
                count(&storage, "SELECT COUNT(*) FROM transactions").await,
                1
            );
            assert_eq!(
                count(
                    &storage,
                    "SELECT COUNT(*) FROM outputs WHERE spent_by IS NOT NULL"
                )
                .await,
                0
            );
        }

        /// A change coin of the default basket on `funding`'s output 0
        /// (`funding` recorded `completed`, with its proof when `proven`).
        async fn seed_change_coin(
            storage: &StorageSqlx,
            user_id: i64,
            funding: &Transaction,
            proven: bool,
        ) -> i64 {
            let basket = storage
                .find_or_create_default_basket(user_id)
                .await
                .unwrap();
            let txid = funding.id();
            let raw = funding.to_binary();
            if proven {
                seed_proven_tx(storage, &txid, &raw, &bump_for(&txid, 900).to_binary()).await;
            }
            let now = Utc::now();
            let row = sqlx::query(
                r#"
                INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
                VALUES (?, 'completed', ?, 0, ?, 1, 0, 'funding', ?, ?, ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(format!("fund-{}", &txid[..8]))
            .bind(funding.outputs[0].satoshis.unwrap() as i64)
            .bind(&txid)
            .bind(&raw)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                                     txid, type, spendable, change, derivation_prefix, derivation_suffix,
                                     provided_by, purpose, output_description, created_at, updated_at)
                VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, 'prefix', 'suffix', 'storage', 'change', 'change', ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(row.last_insert_rowid())
            .bind(basket.basket_id)
            .bind(funding.outputs[0].satoshis.unwrap() as i64)
            .bind(hex::decode(LOCK).unwrap())
            .bind(&txid)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
        }

        /// One change coin is enough: the default basket wants one.
        async fn want_one_change_coin(storage: &StorageSqlx) {
            sqlx::query("UPDATE output_baskets SET number_of_desired_utxos = 1")
                .execute(storage.writer())
                .await
                .unwrap();
        }

        /// A wallet-funded action: one output to a new basket, with a label
        /// and a tag, so a written action would leave rows in every table.
        fn funded_args() -> bsv_rs::wallet::CreateActionArgs {
            bsv_rs::wallet::CreateActionArgs {
                description: "the door in one step".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: hex::decode(LOCK).unwrap(),
                    satoshis: 1_000,
                    output_description: "an output".to_string(),
                    basket: Some("a-new-basket".to_string()),
                    custom_instructions: None,
                    tags: Some(vec!["a-new-tag".to_string()]),
                }]),
                lock_time: None,
                version: None,
                labels: Some(vec!["a-new-label".to_string()]),
                options: None,
            }
        }

        /// Everything an action writes, as text: the coins, the rows, the
        /// labels, tags, baskets and their links, and the sequence counter.
        async fn the_store(storage: &StorageSqlx) -> Vec<Option<String>> {
            let mut seen = Vec::new();
            for sql in [
                "SELECT group_concat(output_id || ':' || spendable || ':' || ifnull(spent_by, '-'), ',') FROM outputs",
                "SELECT group_concat(transaction_id || ':' || status, ',') FROM transactions",
                "SELECT group_concat(label, ',') FROM tx_labels",
                "SELECT group_concat(transaction_id || ':' || tx_label_id, ',') FROM tx_labels_map",
                "SELECT group_concat(tag, ',') FROM output_tags",
                "SELECT group_concat(output_id || ':' || output_tag_id, ',') FROM output_tags_map",
                "SELECT group_concat(name, ',') FROM output_baskets",
                "SELECT group_concat(name || ':' || seq, ',') FROM sqlite_sequence",
            ] {
                seen.push(
                    sqlx::query_scalar::<_, Option<String>>(sql)
                        .fetch_one(storage.pool())
                        .await
                        .unwrap(),
                );
            }
            seen
        }

        /// Option B (the captain's ruling of 2026-10-21): a BEEF failure is a
        /// refusal, and the action writes nothing. The only change coin's
        /// funding transaction is unproven and its parent is not stored;
        /// createAction asks no provider (the services here would answer),
        /// skips the coin, and refuses `Unspendable coin:`, naming the coin
        /// and the parent. The coins, the action's row, its label, tag and
        /// basket, their links and the sequence counter are as they were.
        #[tokio::test]
        async fn a_beef_failure_refuses_with_nothing_written() {
            use crate::services::mock::{MockResponse, MockWalletServices};
            use crate::services::traits::GetRawTxResult;
            use crate::storage::traits::WalletStorageProvider;

            let (storage, user_id) = setup().await;
            let parent = tx_spending(None, 0x11, 60_000);
            let funding = tx_spending(Some(&parent.id()), 0x22, 50_000);
            seed_change_coin(&storage, user_id, &funding, false).await;
            let services = std::sync::Arc::new(
                MockWalletServices::builder()
                    .get_raw_tx_response(MockResponse::Success(GetRawTxResult {
                        name: "mock".to_string(),
                        txid: parent.id(),
                        raw_tx: Some(parent.to_binary()),
                        error: None,
                        could_not_look: false,
                    }))
                    .build(),
            );
            storage.set_services(services.clone());
            let before = the_store(&storage).await;

            let tracker = ProbeTracker::new(&storage, None);
            let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect_err("the parent is not stored");
            let msg = err.to_string();
            assert!(
                msg.starts_with(&format!("Unspendable coin: {}.0: ", funding.id())),
                "the refusal names the wallet's coin: {msg}"
            );
            assert!(
                msg.contains(&format!("ancestor(s) not stored: [{}]", parent.id())),
                "the refusal names the missing ancestor: {msg}"
            );
            assert_eq!(services.call_count("get_raw_tx"), 0, "no provider asked");
            assert_eq!(
                services.call_count("get_merkle_path"),
                0,
                "no provider asked"
            );
            assert_eq!(tracker.asked_under_lock(), 0);
            assert_eq!(the_store(&storage).await, before, "nothing written");
        }

        /// Takes `coin` for another action the first time the root `when` is
        /// asked (the pre-check, no lock held): a concurrent allocation
        /// between the pre-check and the write lock. Records every root asked.
        struct TakingTracker {
            writer: sqlx::SqlitePool,
            when: String,
            coin: i64,
            by: i64,
            taken: std::sync::atomic::AtomicBool,
            asked: std::sync::Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl ChainTracker for TakingTracker {
            async fn is_valid_root_for_height(
                &self,
                root: &str,
                _height: u32,
            ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
                self.asked.lock().unwrap().push(root.to_string());
                if root == self.when && !self.taken.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    let taken = tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        sqlx::query(
                            "UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?",
                        )
                        .bind(self.by)
                        .bind(self.coin)
                        .execute(&self.writer),
                    )
                    .await;
                    assert!(
                        matches!(taken, Ok(Ok(_))),
                        "the writer is free during the pre-check"
                    );
                }
                Ok(true)
            }

            async fn current_height(
                &self,
            ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
                Ok(100_000)
            }
        }

        /// The pre-checked coin is taken before the write lock: the step
        /// rolls back (nothing written), the pre-check runs once more and
        /// picks the other coin, and the action is created from it.
        #[tokio::test]
        async fn a_coin_taken_after_the_precheck_is_skipped_and_the_precheck_runs_again() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let near = tx_spending(None, 0x31, 50_000);
            let far = tx_spending(None, 0x32, 900_000);
            let near_coin = seed_change_coin(&storage, user_id, &near, true).await;
            let far_coin = seed_change_coin(&storage, user_id, &far, true).await;
            let outsider: i64 =
                sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE txid = ?")
                    .bind(near.id())
                    .fetch_one(storage.pool())
                    .await
                    .unwrap();

            let tracker = TakingTracker {
                writer: storage.writer().clone(),
                when: near.id(),
                coin: near_coin,
                by: outsider,
                taken: Default::default(),
                asked: Default::default(),
            };
            let result = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect("the action is created from the other coin");

            let asked = tracker.asked.lock().unwrap().clone();
            assert_eq!(
                asked.first(),
                Some(&near.id()),
                "the first pre-check read the near coin"
            );
            assert!(
                asked.contains(&far.id()),
                "the second pre-check read the far coin: {asked:?}"
            );
            let sources: Vec<&str> = result
                .inputs
                .iter()
                .map(|i| i.source_txid.as_str())
                .collect();
            assert_eq!(sources, vec![far.id().as_str()]);
            let spent_by = |coin: i64| {
                let pool = storage.pool().clone();
                async move {
                    sqlx::query_scalar::<_, Option<i64>>(
                        "SELECT spent_by FROM outputs WHERE output_id = ?",
                    )
                    .bind(coin)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                }
            };
            assert_eq!(
                spent_by(near_coin).await,
                Some(outsider),
                "the taker keeps it"
            );
            let action: i64 = sqlx::query_scalar(
                "SELECT transaction_id FROM transactions WHERE description = 'the door in one step'",
            )
            .fetch_one(storage.pool())
            .await
            .unwrap();
            assert_eq!(spent_by(far_coin).await, Some(action));
            assert_eq!(
                count(
                    &storage,
                    "SELECT COUNT(*) FROM transactions WHERE description = 'the door in one step'"
                )
                .await,
                1,
                "one action row: the rolled-back step left none"
            );
        }

        /// Stores `proof` for `txid` the first time the root `when` is asked
        /// (the pre-check, no lock held): the monitor storing a proof between
        /// the pre-check and the write lock. Records every root asked.
        struct ProvingTracker {
            writer: sqlx::SqlitePool,
            when: String,
            txid: String,
            raw: Vec<u8>,
            proof: Vec<u8>,
            stored: std::sync::atomic::AtomicBool,
            asked: std::sync::Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl ChainTracker for ProvingTracker {
            async fn is_valid_root_for_height(
                &self,
                root: &str,
                _height: u32,
            ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
                self.asked.lock().unwrap().push(root.to_string());
                if root == self.when && !self.stored.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    let now = Utc::now();
                    sqlx::query(
                        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
                         VALUES (?, 901, 0, ?, ?, ?, ?, ?, ?)",
                    )
                    .bind(&self.txid)
                    .bind("ab".repeat(32))
                    .bind(&self.txid)
                    .bind(&self.proof)
                    .bind(&self.raw)
                    .bind(now)
                    .bind(now)
                    .execute(&self.writer)
                    .await
                    .expect("the writer is free during the pre-check");
                }
                Ok(true)
            }

            async fn current_height(
                &self,
            ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
                Ok(100_000)
            }
        }

        /// A proof stored between the pre-check and the write lock is a root
        /// the pre-check did not read: the step rolls back (nothing written),
        /// the pre-check runs once more and reads it, and the action's BEEF
        /// carries the new proof, its parent no longer needed.
        #[tokio::test]
        async fn a_proof_stored_after_the_precheck_is_read_before_the_lock_on_the_second_try() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let parent = tx_spending(None, 0x51, 60_000);
            seed_known(&storage, user_id, &parent, true).await;
            let funding = tx_spending(Some(&parent.id()), 0x52, 50_000);
            seed_change_coin(&storage, user_id, &funding, false).await;

            let tracker = ProvingTracker {
                writer: storage.writer().clone(),
                when: parent.id(),
                txid: funding.id(),
                raw: funding.to_binary(),
                proof: bump_for(&funding.id(), 901).to_binary(),
                stored: Default::default(),
                asked: Default::default(),
            };
            let result = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect("the action is created on the second try");

            let asked = tracker.asked.lock().unwrap().clone();
            assert_eq!(
                asked.first(),
                Some(&parent.id()),
                "the first pre-check read the parent"
            );
            assert!(
                asked.contains(&funding.id()),
                "the second read the new proof: {asked:?}"
            );
            let beef = Beef::from_binary(result.input_beef.as_ref().expect("a BEEF")).unwrap();
            assert!(
                beef.find_txid(&funding.id())
                    .and_then(|t| t.bump_index())
                    .is_some(),
                "the funding transaction rides with its new proof"
            );
            assert!(
                beef.find_txid(&parent.id()).is_none(),
                "its parent is not needed"
            );
            assert_eq!(
                count(
                    &storage,
                    "SELECT COUNT(*) FROM transactions WHERE description = 'the door in one step'"
                )
                .await,
                1
            );
        }

        /// One proof a `StagedProvingTracker` stores: (trigger root, txid, raw
        /// tx, proof, height).
        type ProofStage = (String, String, Vec<u8>, Vec<u8>, u32);

        /// Stores the next proof of `stages` each time its trigger root is
        /// asked (the pre-check, no lock held): the monitor storing a proof
        /// between each pre-check and its write lock.
        struct StagedProvingTracker {
            writer: sqlx::SqlitePool,
            stages: Vec<ProofStage>,
            stored: std::sync::Mutex<HashSet<String>>,
        }

        #[async_trait::async_trait]
        impl ChainTracker for StagedProvingTracker {
            async fn is_valid_root_for_height(
                &self,
                root: &str,
                _height: u32,
            ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
                for (when, txid, raw, proof, height) in &self.stages {
                    if when == root && self.stored.lock().unwrap().insert(txid.clone()) {
                        let now = Utc::now();
                        sqlx::query(
                            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
                             VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)",
                        )
                        .bind(txid)
                        .bind(*height as i64)
                        .bind("ab".repeat(32))
                        .bind(txid)
                        .bind(proof)
                        .bind(raw)
                        .bind(now)
                        .bind(now)
                        .execute(&self.writer)
                        .await
                        .expect("the writer is free during the pre-check");
                    }
                }
                Ok(true)
            }

            async fn current_height(
                &self,
            ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
                Ok(100_000)
            }
        }

        /// A root left unread by the pre-check twice (a proof stored after
        /// each pre-check): the store did not move under the lock, the header
        /// service was not read for that root. The call refuses with the
        /// service-class error naming the height (a host's 502), never
        /// `StoreBusy`, and the action wrote nothing.
        #[tokio::test]
        async fn a_root_unread_twice_refuses_as_a_service_error_naming_its_height() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let grandparent = tx_spending(None, 0x61, 70_000);
            seed_known(&storage, user_id, &grandparent, true).await;
            let parent = tx_spending(Some(&grandparent.id()), 0x62, 60_000);
            seed_known(&storage, user_id, &parent, false).await;
            let funding = tx_spending(Some(&parent.id()), 0x63, 50_000);
            seed_change_coin(&storage, user_id, &funding, false).await;

            let tracker = StagedProvingTracker {
                writer: storage.writer().clone(),
                stages: vec![
                    (
                        grandparent.id(),
                        parent.id(),
                        parent.to_binary(),
                        bump_for(&parent.id(), 901).to_binary(),
                        901,
                    ),
                    (
                        parent.id(),
                        funding.id(),
                        funding.to_binary(),
                        bump_for(&funding.id(), 902).to_binary(),
                        902,
                    ),
                ],
                stored: Default::default(),
            };
            let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect_err("the second pre-check left a root unread");
            match &err {
                Error::ServiceError(text) => assert!(
                    text.contains("at height 902") && text.contains(&funding.id()),
                    "{text}"
                ),
                other => panic!("expected the service-class refusal, got {other:?}"),
            }
            assert!(err.to_string().starts_with("Service error: "), "{err}");
            assert_eq!(
                count(
                    &storage,
                    "SELECT COUNT(*) FROM transactions WHERE description = 'the door in one step'"
                )
                .await,
                0,
                "the action wrote nothing"
            );
        }

        /// The only coin is taken before the write lock: the step rolls
        /// back, the pre-check finds nothing to fund the action, and the call
        /// refuses with insufficient funds, nothing written by it.
        #[tokio::test]
        async fn a_coin_taken_after_the_precheck_with_none_left_refuses_with_nothing_written() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let only = tx_spending(None, 0x41, 50_000);
            let coin = seed_change_coin(&storage, user_id, &only, true).await;
            let outsider: i64 =
                sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE txid = ?")
                    .bind(only.id())
                    .fetch_one(storage.pool())
                    .await
                    .unwrap();
            let tracker = TakingTracker {
                writer: storage.writer().clone(),
                when: only.id(),
                coin,
                by: outsider,
                taken: Default::default(),
                asked: Default::default(),
            };
            let mut before = the_store(&storage).await;

            let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect_err("no coin is left");
            assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");

            // Only the taker's write moved anything: the coin it took.
            before[0] = Some(format!("{coin}:0:{outsider}"));
            assert_eq!(the_store(&storage).await, before);
        }

        // =====================================================================
        // Concurrent createActions in one process (review 2 F1 of #44)
        // =====================================================================

        /// Waits 30 ms on every root (the reviewer's scratch tracker), so
        /// concurrent pre-checks overlap, and records the coins reserved by
        /// every call at each ask.
        struct SlowTracker {
            in_flight: std::sync::Arc<InFlightPicks>,
            seen_reserved: std::sync::Mutex<Vec<usize>>,
        }

        impl SlowTracker {
            fn new(storage: &StorageSqlx) -> Self {
                Self {
                    in_flight: storage.in_flight_picks().clone(),
                    seen_reserved: Default::default(),
                }
            }
        }

        #[async_trait::async_trait]
        impl ChainTracker for SlowTracker {
            async fn is_valid_root_for_height(
                &self,
                _root: &str,
                _height: u32,
            ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
                self.seen_reserved
                    .lock()
                    .unwrap()
                    .push(self.in_flight.reserved().len());
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                Ok(true)
            }

            async fn current_height(
                &self,
            ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
                Ok(100_000)
            }
        }

        /// Eight proven 900,000-sat change coins; one wanted.
        async fn eight_coins(storage: &StorageSqlx, user_id: i64) {
            want_one_change_coin(storage).await;
            for i in 0..8u8 {
                let t = tx_spending(None, 0x40 + i, 900_000);
                seed_change_coin(storage, user_id, &t, true).await;
            }
        }

        /// The reviewer's race: four createActions under `tokio::join!` on a
        /// wallet of eight 900,000-sat coins. At 87d3d43 two of four were
        /// refused 'Insufficient funds: need 1005, have 0'; with the
        /// in-flight reservation every call succeeds, each from its own
        /// coin, and no reservation outlives its call.
        #[tokio::test]
        async fn four_concurrent_creates_on_eight_coins_all_succeed() {
            let (storage, user_id) = setup().await;
            eight_coins(&storage, user_id).await;
            let tracker = SlowTracker::new(&storage);
            let f = || {
                create_action_internal(
                    &storage,
                    Some(&tracker as &dyn ChainTracker),
                    user_id,
                    funded_args(),
                )
            };
            let (a, b, c, d) = tokio::join!(f(), f(), f(), f());
            let mut sources = Vec::new();
            for r in [a, b, c, d] {
                let r = r.expect("a funded wallet funds every call");
                sources.extend(r.inputs.into_iter().map(|i| i.source_txid));
            }
            let distinct: HashSet<&String> = sources.iter().collect();
            assert_eq!(sources.len(), 4, "{sources:?}");
            assert_eq!(distinct.len(), 4, "each call spends its own coin");
            let seen = tracker.seen_reserved.lock().unwrap().clone();
            assert!(
                seen.iter().any(|n| *n > 1),
                "the pre-checks overlapped, several coins reserved at once: {seen:?}"
            );
            assert!(
                storage.in_flight_picks().reserved().is_empty(),
                "every reservation is released when its call returns"
            );
        }

        /// The same race with the reservation off (the case of another
        /// process): a call whose pre-check covered the action and whose
        /// coin was taken under it twice is refused `StoreBusy`
        /// (`MovedUnderUs`), never `InsufficientFunds`: the wallet is funded.
        #[tokio::test]
        async fn without_the_reservation_a_twice_lost_race_is_store_busy_never_insufficient_funds()
        {
            let (storage, user_id) = setup().await;
            eight_coins(&storage, user_id).await;
            storage.in_flight_picks().set_off(true);
            let tracker = SlowTracker::new(&storage);
            let f = || {
                create_action_internal(
                    &storage,
                    Some(&tracker as &dyn ChainTracker),
                    user_id,
                    funded_args(),
                )
            };
            let (a, b, c, d) = tokio::join!(f(), f(), f(), f());
            let errs: Vec<Error> = [a, b, c, d].into_iter().filter_map(|r| r.err()).collect();
            assert!(
                !errs.is_empty(),
                "with no reservation the concurrent pre-checks collide"
            );
            for err in &errs {
                assert!(
                    matches!(
                        err,
                        Error::StoreBusy {
                            reason: StoreBusyReason::MovedUnderUs
                        }
                    ),
                    "a funded wallet's lost race is busy: {err:?}"
                );
                assert_eq!(
                    err.to_string(),
                    "Store busy: the store moved between the pre-check and the write lock twice"
                );
            }
        }

        /// The reservation is released on a refusal too: a call refused
        /// for want of funds leaves no coin reserved, and the next call
        /// picks the same coin.
        #[tokio::test]
        async fn a_refused_call_releases_its_reservation() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let small = tx_spending(None, 0x61, 600);
            seed_change_coin(&storage, user_id, &small, true).await;
            let tracker = SlowTracker::new(&storage);
            let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect_err("600 sats do not fund 1,000");
            assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
            assert!(storage.in_flight_picks().reserved().is_empty());

            let big = tx_spending(None, 0x62, 50_000);
            seed_change_coin(&storage, user_id, &big, true).await;
            create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect("funded now");
            assert!(storage.in_flight_picks().reserved().is_empty());
        }

        /// Review 3 F2 of #44: nine concurrent createActions on eight coins,
        /// each needing one. Eight are done, each from its own coin; the
        /// ninth's pre-check finds every coin reserved by a call in flight,
        /// and planned without those reservations it is covered: it is busy
        /// (`InFlight`) or done, never `Insufficient funds`.
        #[tokio::test]
        async fn nine_concurrent_creates_on_eight_coins_are_done_or_busy_never_insufficient_funds()
        {
            let (storage, user_id) = setup().await;
            eight_coins(&storage, user_id).await;
            let tracker = SlowTracker::new(&storage);
            let results = futures::future::join_all((0..9).map(|_| {
                create_action_internal(
                    &storage,
                    Some(&tracker as &dyn ChainTracker),
                    user_id,
                    funded_args(),
                )
            }))
            .await;
            let mut sources = Vec::new();
            for result in results {
                match result {
                    Ok(done) => sources.extend(done.inputs.into_iter().map(|i| i.source_txid)),
                    Err(err) => {
                        assert!(
                            matches!(
                                err,
                                Error::StoreBusy {
                                    reason: StoreBusyReason::InFlight
                                }
                            ),
                            "a funded wallet's call is busy or done: {err:?}"
                        );
                        assert_eq!(err.to_string(), "Store busy: coins held by calls in flight");
                    }
                }
            }
            let distinct: HashSet<&String> = sources.iter().collect();
            assert_eq!(sources.len(), 8, "eight done: {sources:?}");
            assert_eq!(distinct.len(), 8, "each from its own coin");
            assert!(storage.in_flight_picks().reserved().is_empty());
        }

        /// The reviewer's probe (review 3 F2 of #44): the eight coins held
        /// by reservations of calls in flight (held here by hand). The call
        /// answers busy, `InFlight`, having written nothing; the reservations
        /// released, the same call is done.
        #[tokio::test]
        async fn coins_held_by_calls_in_flight_are_busy_then_done_once_released() {
            let (storage, user_id) = setup().await;
            eight_coins(&storage, user_id).await;
            let coins: Vec<i64> = sqlx::query_scalar(
                "SELECT output_id FROM outputs WHERE change = 1 AND spendable = 1",
            )
            .fetch_all(storage.pool())
            .await
            .unwrap();
            assert_eq!(coins.len(), 8);
            let mut held = PickReservation::new(storage.in_flight_picks().clone());
            for coin in &coins {
                assert!(held.try_take(*coin));
            }
            let before = the_store(&storage).await;

            let err = create_action_internal(&storage, None, user_id, funded_args())
                .await
                .expect_err("every coin is held by a call in flight");
            assert!(
                matches!(
                    err,
                    Error::StoreBusy {
                        reason: StoreBusyReason::InFlight
                    }
                ),
                "never insufficient funds on a funded wallet: {err:?}"
            );
            assert_eq!(the_store(&storage).await, before, "nothing written");
            assert_eq!(
                storage.in_flight_picks().reserved().len(),
                8,
                "the refused call holds nothing of its own"
            );

            drop(held);
            create_action_internal(&storage, None, user_id, funded_args())
                .await
                .expect("released, the wallet funds the call");
        }

        /// One store, two handles (a clone, as a host gives its `Wallet` and
        /// its `Monitor`): the coins reserved through one handle are held
        /// for the other's dry run, which answers `InFlight`, never picking
        /// them; released through the first, the second's call is done.
        #[tokio::test]
        async fn a_reservation_through_one_handle_is_seen_by_the_others_dry_run() {
            let (storage, user_id) = setup().await;
            eight_coins(&storage, user_id).await;
            let other = storage.clone();
            let coins: Vec<i64> = sqlx::query_scalar(
                "SELECT output_id FROM outputs WHERE change = 1 AND spendable = 1",
            )
            .fetch_all(storage.pool())
            .await
            .unwrap();
            let mut held = PickReservation::new(storage.in_flight_picks().clone());
            for coin in &coins {
                assert!(held.try_take(*coin));
            }
            assert_eq!(other.in_flight_picks().reserved().len(), 8);

            let err = create_action_internal(&other, None, user_id, funded_args())
                .await
                .expect_err("every coin is held through the first handle");
            assert!(
                matches!(
                    err,
                    Error::StoreBusy {
                        reason: StoreBusyReason::InFlight
                    }
                ),
                "{err:?}"
            );

            drop(held);
            assert!(other.in_flight_picks().reserved().is_empty());
            create_action_internal(&other, None, user_id, funded_args())
                .await
                .expect("released through the first handle, the second's call is done");
        }

        /// A shortfall that is the funds stays `InsufficientFunds` while a
        /// call is in flight: two 300-sat coins, one held; planned without
        /// the reservation the action is still uncovered, so not busy.
        #[tokio::test]
        async fn a_true_shortfall_with_a_call_in_flight_stays_insufficient_funds() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let small = tx_spending(None, 0x51, 300);
            let other = tx_spending(None, 0x52, 300);
            let a = seed_change_coin(&storage, user_id, &small, true).await;
            seed_change_coin(&storage, user_id, &other, true).await;
            let mut held = PickReservation::new(storage.in_flight_picks().clone());
            assert!(held.try_take(a));

            let err = create_action_internal(&storage, None, user_id, funded_args())
                .await
                .expect_err("600 sats in all do not fund 1,000");
            assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
        }

        // =====================================================================
        // A change coin whose BEEF cannot be built (review 2 F2 of #44)
        // =====================================================================

        /// A reorg-demoted received coin: its transaction `unproven`, with
        /// no proof, and its parent never stored. Returns the coin.
        async fn seed_demoted_coin(
            storage: &StorageSqlx,
            user_id: i64,
            tag: u8,
            satoshis: u64,
        ) -> (Transaction, Transaction) {
            let parent = tx_spending(None, tag, satoshis + 1_000);
            let funding = tx_spending(Some(&parent.id()), tag + 1, satoshis);
            seed_change_coin(storage, user_id, &funding, false).await;
            sqlx::query("UPDATE transactions SET status = 'unproven' WHERE txid = ?")
                .bind(funding.id())
                .execute(storage.writer())
                .await
                .unwrap();
            (parent, funding)
        }

        /// The demoted coin is the best pick (900,000 sats, the only coin at
        /// or above the target), but its BEEF cannot be built: the
        /// pre-check skips it and funds the action from the two proven
        /// 800-sat coins.
        #[tokio::test]
        async fn a_precheck_skips_a_coin_whose_beef_cannot_be_built() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let (_, demoted) = seed_demoted_coin(&storage, user_id, 0x71, 900_000).await;
            let b1 = tx_spending(None, 0x75, 800);
            let b2 = tx_spending(None, 0x76, 800);
            seed_change_coin(&storage, user_id, &b1, true).await;
            seed_change_coin(&storage, user_id, &b2, true).await;
            // One root per height: b2's proof is in the next block.
            sqlx::query("UPDATE proven_txs SET height = 901, merkle_path = ? WHERE txid = ?")
                .bind(bump_for(&b2.id(), 901).to_binary())
                .bind(b2.id())
                .execute(storage.writer())
                .await
                .unwrap();

            let tracker = AlwaysValidChainTracker::new(100_000);
            let result = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect("the proven coins fund the action");
            let mut sources: Vec<String> = result
                .inputs
                .iter()
                .map(|i| i.source_txid.clone())
                .collect();
            sources.sort();
            let mut want = vec![b1.id(), b2.id()];
            want.sort();
            assert_eq!(
                sources,
                want,
                "the demoted coin {} is not spent",
                demoted.id()
            );
            let beef = Beef::from_binary(result.input_beef.as_ref().unwrap()).unwrap();
            assert!(beef.find_txid(&demoted.id()).is_none());
            assert!(storage.in_flight_picks().reserved().is_empty());
        }

        /// The demoted coin alone covers the action; the proven 300-sat coin
        /// does not. The refusal names the wallet's coin and its missing
        /// ancestor under `Unspendable coin:`, never `inputBEEF` (the caller
        /// passed none) and never `Insufficient funds` (the coins offered
        /// cover the action).
        #[tokio::test]
        async fn an_unspendable_coin_refusal_names_the_coin_and_its_missing_ancestor() {
            let (storage, user_id) = setup().await;
            want_one_change_coin(&storage).await;
            let (parent, demoted) = seed_demoted_coin(&storage, user_id, 0x81, 900_000).await;
            let small = tx_spending(None, 0x85, 300);
            seed_change_coin(&storage, user_id, &small, true).await;
            let before = the_store(&storage).await;

            let tracker = AlwaysValidChainTracker::new(100_000);
            let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
                .await
                .expect_err("only the unspendable coin covers the action");
            match &err {
                Error::UnspendableCoin { outpoint, missing } => {
                    assert_eq!(outpoint, &format!("{}.0", demoted.id()));
                    assert_eq!(missing, &vec![parent.id()]);
                }
                other => panic!("expected UnspendableCoin, got {other:?}"),
            }
            let msg = err.to_string();
            assert_eq!(
                msg,
                format!(
                    "Unspendable coin: {}.0: its stored ancestry is incomplete, ancestor(s) not stored: [{}]; \
                     createAction fetches none, and the wallet's other coins do not cover the action",
                    demoted.id(),
                    parent.id()
                )
            );
            assert!(!msg.contains("inputBEEF"), "{msg}");
            assert!(!msg.starts_with("Insufficient funds"), "{msg}");
            assert_eq!(
                the_store(&storage).await,
                before,
                "the action wrote nothing"
            );
            assert!(storage.in_flight_picks().reserved().is_empty());
        }

        // =====================================================================
        // The host's offer (bsv-wallet-cli #44, bsv-low #828)
        // =====================================================================

        /// Eight 900,000-sat coins; their output ids, in seeding order.
        async fn eight_coin_ids(storage: &StorageSqlx, user_id: i64) -> Vec<i64> {
            want_one_change_coin(storage).await;
            let mut ids = Vec::new();
            for i in 0..8u8 {
                let t = tx_spending(None, 0x70 + i, 900_000);
                ids.push(seed_change_coin(storage, user_id, &t, true).await);
            }
            ids
        }

        /// `funded_args` through the door with `offer`.
        async fn offering(
            storage: &StorageSqlx,
            tracker: &dyn ChainTracker,
            user_id: i64,
            offer: &[i64],
        ) -> Result<StorageCreateActionResult> {
            create_action_at_door_internal(
                storage,
                Some(tracker),
                user_id,
                funded_args(),
                CreateActionDoor {
                    offer: Some(offer),
                    sign_and_process: None,
                },
            )
            .await
            .map(|done| done.created)
            .map_err(DoorRefusal::into_error)
        }

        /// The coins allocated to some action.
        async fn allocated(storage: &StorageSqlx) -> Vec<i64> {
            sqlx::query_scalar(
                "SELECT output_id FROM outputs WHERE spent_by IS NOT NULL ORDER BY output_id",
            )
            .fetch_all(storage.pool())
            .await
            .unwrap()
        }

        /// Eight coins, an offer of four, five concurrent calls: the four
        /// offered coins fund four calls, one each, and no coin outside the
        /// offer is ever allocated. The fifth refuses: busy (`InFlight`)
        /// when it pre-checks while the four hold the offer (review 3 F2),
        /// insufficient funds when it pre-checks after they spent it.
        #[tokio::test]
        async fn a_coin_outside_the_offer_is_never_allocated_under_concurrency() {
            let (storage, user_id) = setup().await;
            let ids = eight_coin_ids(&storage, user_id).await;
            let offer = vec![ids[1], ids[3], ids[5], ids[7]];
            let tracker = SlowTracker::new(&storage);
            let f = || offering(&storage, &tracker, user_id, &offer);
            let (a, b, c, d, e) = tokio::join!(f(), f(), f(), f(), f());
            let mut done = 0;
            for r in [a, b, c, d, e] {
                match r {
                    Ok(_) => done += 1,
                    Err(e) => assert!(
                        matches!(
                            e,
                            Error::InsufficientFunds { .. }
                                | Error::StoreBusy {
                                    reason: StoreBusyReason::InFlight
                                }
                        ),
                        "{e:?}"
                    ),
                }
            }
            assert_eq!(done, 4, "each offered coin funds one call");
            assert_eq!(
                allocated(&storage).await,
                offer,
                "exactly the offered coins, nothing outside"
            );
            assert!(storage.in_flight_picks().reserved().is_empty());
        }

        /// The pre-check's pick is intersected with the offer: one offered
        /// coin among eight equal ones is the coin the action spends.
        #[tokio::test]
        async fn the_precheck_picks_within_the_offer() {
            let (storage, user_id) = setup().await;
            let ids = eight_coin_ids(&storage, user_id).await;
            let tracker = SlowTracker::new(&storage);
            let result = offering(&storage, &tracker, user_id, &[ids[6]])
                .await
                .expect("the offered coin covers the action");
            let txid: String = sqlx::query_scalar("SELECT txid FROM outputs WHERE output_id = ?")
                .bind(ids[6])
                .fetch_one(storage.pool())
                .await
                .unwrap();
            let sources: Vec<&str> = result
                .inputs
                .iter()
                .map(|i| i.source_txid.as_str())
                .collect();
            assert_eq!(sources, vec![txid.as_str()]);
            assert_eq!(allocated(&storage).await, vec![ids[6]]);
        }

        /// An offer that does not cover the action refuses insufficient
        /// funds though the store holds coins enough outside it, and the
        /// action writes nothing.
        #[tokio::test]
        async fn an_offer_that_does_not_cover_refuses_insufficient_funds() {
            let (storage, user_id) = setup().await;
            let ids = eight_coin_ids(&storage, user_id).await;
            let small = tx_spending(None, 0x7f, 500);
            let small = seed_change_coin(&storage, user_id, &small, true).await;
            let before = the_store(&storage).await;
            let tracker = SlowTracker::new(&storage);
            let err = offering(&storage, &tracker, user_id, &[small])
                .await
                .expect_err("500 sats cannot fund 1,000");
            assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
            assert!(err.to_string().starts_with("Insufficient funds: "), "{err}");
            assert_eq!(the_store(&storage).await, before, "nothing written");
            assert!(allocated(&storage).await.is_empty(), "{ids:?} untouched");
        }

        /// An empty offer refuses insufficient funds, nothing written.
        #[tokio::test]
        async fn an_empty_offer_refuses_insufficient_funds() {
            let (storage, user_id) = setup().await;
            eight_coin_ids(&storage, user_id).await;
            let before = the_store(&storage).await;
            let tracker = SlowTracker::new(&storage);
            let err = offering(&storage, &tracker, user_id, &[])
                .await
                .expect_err("an empty offer funds nothing");
            assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
            assert_eq!(the_store(&storage).await, before, "nothing written");
        }
    }
}
