//! Sync chunk operations for multi-storage synchronization.
//!
//! This module implements `get_sync_chunk` and `process_sync_chunk` methods
//! that enable synchronization of wallet data between storage providers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};
use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::storage::entities::*;
use crate::storage::traits::*;

use super::StorageSqlx;

// =============================================================================
// Entity Names (for offsets)
// =============================================================================

/// Entity name constants for sync offsets
pub mod entity_names {
    pub const OUTPUT_BASKET: &str = "outputBasket";
    pub const PROVEN_TX: &str = "provenTx";
    pub const PROVEN_TX_REQ: &str = "provenTxReq";
    pub const TX_LABEL: &str = "txLabel";
    pub const OUTPUT_TAG: &str = "outputTag";
    pub const TRANSACTION: &str = "transaction";
    pub const OUTPUT: &str = "output";
    pub const TX_LABEL_MAP: &str = "txLabelMap";
    pub const OUTPUT_TAG_MAP: &str = "outputTagMap";
    pub const CERTIFICATE: &str = "certificate";
    pub const CERTIFICATE_FIELD: &str = "certificateField";
    pub const COMMISSION: &str = "commission";
}

// =============================================================================
// Sync State Tracking
// =============================================================================

/// Tracks state during chunk building
struct ChunkingState {
    items_count: u32,
    rough_size: u32,
    max_items: u32,
    max_rough_size: u32,
}

impl ChunkingState {
    fn new(max_items: u32, max_rough_size: u32) -> Self {
        Self {
            items_count: 0,
            rough_size: 0,
            max_items,
            max_rough_size,
        }
    }

    fn can_add(&self) -> bool {
        self.items_count < self.max_items && self.rough_size < self.max_rough_size
    }

    fn add_items(&mut self, count: u32, size: u32) {
        self.items_count += count;
        self.rough_size += size;
    }

    fn remaining_items(&self) -> u32 {
        self.max_items.saturating_sub(self.items_count)
    }
}

/// Lookup for offsets by entity name
type OffsetsLookup = HashMap<String, u32>;

fn make_offsets_lookup(offsets: &[SyncOffset]) -> OffsetsLookup {
    offsets.iter().map(|o| (o.name.clone(), o.offset)).collect()
}

// =============================================================================
// Sync Map for ID Translation
// =============================================================================

/// Tracks entity sync state including ID mappings
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncMapEntity {
    pub count: u64,
    pub max_updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub id_map: HashMap<i64, i64>,
}

/// Complete sync map for all entities
#[allow(dead_code)]
pub type SyncMap = HashMap<String, SyncMapEntity>;

// =============================================================================
// get_sync_chunk Implementation
// =============================================================================

/// Get a chunk of data for synchronization.
///
/// This function queries each entity type with:
/// - `updated_at > since` filter
/// - `LIMIT max_items` constraint
/// - Tracks rough size to stay under `max_rough_size`
/// - Uses offsets to resume from previous chunk
///
/// Order matters for foreign key dependencies.
pub async fn get_sync_chunk_internal(
    storage: &StorageSqlx,
    args: RequestSyncChunkArgs,
) -> Result<SyncChunk> {
    // Find the user
    let user = storage
        .find_user(&args.identity_key)
        .await?
        .ok_or_else(|| Error::UserNotFound(args.identity_key.clone()))?;

    let user_id = user.user_id;

    // Initialize result
    let mut chunk = SyncChunk {
        from_storage_identity_key: args.from_storage_identity_key.clone(),
        to_storage_identity_key: args.to_storage_identity_key.clone(),
        user_identity_key: args.identity_key.clone(),
        ..Default::default()
    };

    // Include user if updated since last sync
    if args.since.is_none() || user.updated_at > args.since.unwrap() {
        chunk.user = Some(user.clone());
    }

    // Build offsets lookup
    let offsets = make_offsets_lookup(&args.offsets);

    // Early return if no offsets provided
    if offsets.is_empty() {
        return Ok(chunk);
    }

    // Initialize chunking state
    let mut state = ChunkingState::new(args.max_items, args.max_rough_size);

    // Process entities in dependency order
    // Each chunker processes one entity type

    // 1. Output Baskets (no dependencies)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::OUTPUT_BASKET) {
            let baskets = fetch_baskets_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !baskets.is_empty() {
                let size = estimate_size(&baskets);
                state.add_items(baskets.len() as u32, size);
                chunk.output_baskets = Some(baskets);
            }
        }
    }

    // 2. Proven Txs (no dependencies)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::PROVEN_TX) {
            let proven_txs = fetch_proven_txs_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !proven_txs.is_empty() {
                let size = estimate_size(&proven_txs);
                state.add_items(proven_txs.len() as u32, size);
                chunk.proven_txs = Some(proven_txs);
            }
        }
    }

    // 3. Proven Tx Reqs (no dependencies)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::PROVEN_TX_REQ) {
            let reqs = fetch_proven_tx_reqs_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !reqs.is_empty() {
                let size = estimate_size(&reqs);
                state.add_items(reqs.len() as u32, size);
                chunk.proven_tx_reqs = Some(reqs);
            }
        }
    }

    // 4. Tx Labels (depends on user)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::TX_LABEL) {
            let labels = fetch_tx_labels_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !labels.is_empty() {
                let size = estimate_size(&labels);
                state.add_items(labels.len() as u32, size);
                chunk.tx_labels = Some(labels);
            }
        }
    }

    // 5. Output Tags (depends on user)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::OUTPUT_TAG) {
            let tags = fetch_output_tags_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !tags.is_empty() {
                let size = estimate_size(&tags);
                state.add_items(tags.len() as u32, size);
                chunk.output_tags = Some(tags);
            }
        }
    }

    // 6. Transactions (depends on user)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::TRANSACTION) {
            let txs = fetch_transactions_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !txs.is_empty() {
                let size = estimate_size(&txs);
                state.add_items(txs.len() as u32, size);
                chunk.transactions = Some(txs);
            }
        }
    }

    // 7. Outputs (depends on transactions)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::OUTPUT) {
            let outputs = fetch_outputs_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !outputs.is_empty() {
                let size = estimate_size(&outputs);
                state.add_items(outputs.len() as u32, size);
                chunk.outputs = Some(outputs);
            }
        }
    }

    // 8. Tx Label Maps (depends on transactions and labels)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::TX_LABEL_MAP) {
            let maps = fetch_tx_label_maps_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !maps.is_empty() {
                let size = estimate_size(&maps);
                state.add_items(maps.len() as u32, size);
                chunk.tx_label_maps = Some(maps);
            }
        }
    }

    // 9. Output Tag Maps (depends on outputs and tags)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::OUTPUT_TAG_MAP) {
            let maps = fetch_output_tag_maps_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !maps.is_empty() {
                let size = estimate_size(&maps);
                state.add_items(maps.len() as u32, size);
                chunk.output_tag_maps = Some(maps);
            }
        }
    }

    // 10. Certificates (depends on user)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::CERTIFICATE) {
            let certs = fetch_certificates_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !certs.is_empty() {
                let size = estimate_size(&certs);
                state.add_items(certs.len() as u32, size);
                chunk.certificates = Some(certs);
            }
        }
    }

    // 11. Certificate Fields (depends on certificates)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::CERTIFICATE_FIELD) {
            let fields = fetch_certificate_fields_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !fields.is_empty() {
                let size = estimate_size(&fields);
                state.add_items(fields.len() as u32, size);
                chunk.certificate_fields = Some(fields);
            }
        }
    }

    // 12. Commissions (depends on transactions)
    if state.can_add() {
        if let Some(&offset) = offsets.get(entity_names::COMMISSION) {
            let commissions = fetch_commissions_for_sync(
                storage,
                user_id,
                args.since,
                offset,
                state.remaining_items(),
            )
            .await?;
            if !commissions.is_empty() {
                let size = estimate_size(&commissions);
                state.add_items(commissions.len() as u32, size);
                chunk.commissions = Some(commissions);
            }
        }
    }

    Ok(chunk)
}

// =============================================================================
// process_sync_chunk Implementation
// =============================================================================

/// Process a received sync chunk.
///
/// Applies chunk data with upsert logic:
/// - For each entity in chunk: check if exists by primary key
/// - If exists and chunk.updated_at > local.updated_at: UPDATE
/// - If not exists: INSERT
/// - Track inserts and updates
/// - Return max_updated_at seen
pub async fn process_sync_chunk_internal(
    storage: &StorageSqlx,
    args: RequestSyncChunkArgs,
    chunk: SyncChunk,
    sync_map: &mut crate::storage::SyncMap,
) -> Result<ProcessSyncChunkResult> {
    // A chunk is bound to its request: its source, its destination and its
    // user (the reference's runPullSession, ts-stack 11faa7d16
    // `storage/sync/syncSession.ts` 115-121).
    if chunk.from_storage_identity_key != args.from_storage_identity_key
        || chunk.to_storage_identity_key != args.to_storage_identity_key
        || chunk.user_identity_key != args.identity_key
    {
        return Err(Error::SyncError(
            "The chunk parameter must be bound to this sync source, destination and wallet identity"
                .to_string(),
        ));
    }

    // The user must exist: a chunk never creates one (the reference's
    // processSyncChunk). The manager brings a store's user into being before
    // it syncs to that store.
    let user = storage
        .find_user(&args.identity_key)
        .await?
        .ok_or_else(|| Error::UserNotFound(args.identity_key.clone()))?;

    // One chunk, one write transaction (the reference's processSyncChunk runs
    // a page in one transaction): a chunk refused or failing part way leaves
    // none of its rows, and no other writer's row lands between two of them.
    // The id maps are restored with it, so a later chunk does not resolve a
    // parent this one never wrote.
    // The chunk's proof rows are decided before the writer is taken: every
    // tracker and header read happens here (super::sync_proof).
    let mut chunk = chunk;
    let decisions = match chunk.proven_txs.as_mut() {
        Some(proofs) => super::sync_proof::preflight(storage, proofs).await?,
        None => Default::default(),
    };
    let saved_map = sync_map.clone();
    let mut db_tx = storage.write_tx().await?;
    match apply_chunk(
        &mut db_tx,
        user.user_id,
        &args,
        &chunk,
        &decisions,
        sync_map,
    )
    .await
    {
        Ok(result) => {
            super::storage_sqlx::commit(db_tx).await?;
            Ok(result)
        }
        Err(e) => {
            *sync_map = saved_map;
            drop(db_tx);
            Err(e)
        }
    }
}

/// Record that the source row `from` of `entity` is the local row `to`. A
/// source id the map already holds for another local row is an incoherent
/// chunk and is refused, as the reference's `updateSyncMap` refuses it.
fn remap(map: &mut HashMap<i64, i64>, entity: &str, from: i64, to: i64) -> Result<()> {
    match map.get(&from) {
        Some(&held) if held != to => Err(Error::SyncError(format!(
            "sync chunk {entity} id {from} maps to local id {held} and {to}"
        ))),
        _ => {
            map.insert(from, to);
            Ok(())
        }
    }
}

async fn apply_chunk(
    conn: &mut SqliteConnection,
    user_id: i64,
    args: &RequestSyncChunkArgs,
    chunk: &SyncChunk,
    decisions: &super::sync_proof::ProofDecisions,
    sync_map: &mut crate::storage::SyncMap,
) -> Result<ProcessSyncChunkResult> {
    let mut result = ProcessSyncChunkResult {
        done: false,
        max_updated_at: None,
        updates: 0,
        inserts: 0,
        error: None,
    };

    // Find or create sync state for tracking
    ensure_sync_state(conn, user_id, &args.from_storage_identity_key).await?;

    // Track ID mappings from source to destination. Threaded across chunks via
    // `sync_map` (disjoint field borrows) so a child row resolves a parent that
    // arrived in an earlier chunk.
    let basket_id_map = &mut sync_map.basket;
    let label_id_map = &mut sync_map.label;
    let tag_id_map = &mut sync_map.tag;
    let transaction_id_map = &mut sync_map.transaction;
    let output_id_map = &mut sync_map.output;
    let certificate_id_map = &mut sync_map.certificate;
    let mut proof_id_map: HashMap<i64, i64> = HashMap::new();

    // Check if chunk is empty (sync complete)
    let chunk_is_empty = chunk.output_baskets.as_ref().is_none_or(|v| v.is_empty())
        && chunk.proven_txs.as_ref().is_none_or(|v| v.is_empty())
        && chunk.proven_tx_reqs.as_ref().is_none_or(|v| v.is_empty())
        && chunk.transactions.as_ref().is_none_or(|v| v.is_empty())
        && chunk.outputs.as_ref().is_none_or(|v| v.is_empty())
        && chunk.tx_labels.as_ref().is_none_or(|v| v.is_empty())
        && chunk.tx_label_maps.as_ref().is_none_or(|v| v.is_empty())
        && chunk.output_tags.as_ref().is_none_or(|v| v.is_empty())
        && chunk.output_tag_maps.as_ref().is_none_or(|v| v.is_empty())
        && chunk.certificates.as_ref().is_none_or(|v| v.is_empty())
        && chunk
            .certificate_fields
            .as_ref()
            .is_none_or(|v| v.is_empty())
        && chunk.commissions.as_ref().is_none_or(|v| v.is_empty());

    if chunk_is_empty {
        result.done = true;
        // Merge user if provided
        if let Some(ref chunk_user) = chunk.user {
            let merge_result =
                merge_user(&mut *conn, user_id, &args.identity_key, chunk_user).await?;
            if merge_result.updated {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, chunk_user.updated_at);
        }
        return Ok(result);
    }

    // Process user if included
    if let Some(ref chunk_user) = chunk.user {
        let merge_result = merge_user(&mut *conn, user_id, &args.identity_key, chunk_user).await?;
        if merge_result.updated {
            result.updates += 1;
        }
        update_max_updated_at(&mut result.max_updated_at, chunk_user.updated_at);
    }

    // Process proven txs ahead of every row that names one (the reference's
    // order puts provenTx ahead of every entity and provenTxReq last). A
    // row links to the proof of its own txid; the chunk's proof id map only
    // refuses a source proof id that maps to two local rows.
    if let Some(proven_txs) = &chunk.proven_txs {
        for ptx in proven_txs {
            let upsert_result = upsert_proven_tx(&mut *conn, ptx, decisions).await?;
            remap(
                &mut proof_id_map,
                "provenTx",
                ptx.proven_tx_id,
                upsert_result.local_id,
            )?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, ptx.updated_at);
        }
    }

    // Process output baskets
    if let Some(baskets) = &chunk.output_baskets {
        for basket in baskets {
            let upsert_result = upsert_basket(&mut *conn, user_id, basket).await?;
            remap(
                basket_id_map,
                "outputBasket",
                basket.basket_id,
                upsert_result.local_id,
            )?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, basket.updated_at);
        }
    }

    // Process tx labels
    if let Some(labels) = &chunk.tx_labels {
        for label in labels {
            let upsert_result = upsert_tx_label(&mut *conn, user_id, label).await?;
            remap(
                label_id_map,
                "txLabel",
                label.label_id,
                upsert_result.local_id,
            )?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, label.updated_at);
        }
    }

    // Process output tags
    if let Some(tags) = &chunk.output_tags {
        for tag in tags {
            let upsert_result = upsert_output_tag(&mut *conn, user_id, tag).await?;
            remap(tag_id_map, "outputTag", tag.tag_id, upsert_result.local_id)?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, tag.updated_at);
        }
    }

    // Process transactions
    if let Some(transactions) = &chunk.transactions {
        for tx in transactions {
            let upsert_result = upsert_transaction(&mut *conn, user_id, tx).await?;
            remap(
                transaction_id_map,
                "transaction",
                tx.transaction_id,
                upsert_result.local_id,
            )?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, tx.updated_at);
        }
    }

    // Process outputs (need transaction ID translation).
    //
    // Resolve the parent transaction's LOCAL FK in three steps before
    // calling `upsert_output`:
    //
    //   1. Try this session's threaded `transaction_id_map` (populated
    //      by `upsert_transaction` for transactions delivered earlier
    //      in this run).
    //   2. Fall back to a local-DB lookup by `(user_id, txid)`.
    //      `output.txid` is the parent transaction's on-chain txid;
    //      previously-synced parent transactions may live in local DB
    //      without appearing in this session's map (the chunk's
    //      `transactions` section only carries since-deltas).
    //   3. If still unresolvable, the output is orphan — skip,
    //      consistent with the skip-on-missing-parent pattern used by
    //      `tx_label_maps`, `output_tag_maps`, `commissions`, and
    //      `certificate_fields` below.
    //
    // The prior `local_tx_id.unwrap_or(output.transaction_id)` fallback
    // inside `upsert_output` silently substituted the REMOTE numerical
    // FK in place of a missing local one, which then collided on
    // `UNIQUE(transaction_id, vout, user_id)` and surfaced as
    // `rc=19 UNIQUE constraint failed: outputs.transaction_id,
    // outputs.vout, outputs.user_id` on apply. Making `tx_id` mandatory
    // at the call site makes that substitution structurally impossible.
    if let Some(outputs) = &chunk.outputs {
        for output in outputs {
            // An output naming a basket the chunk does not map is an
            // incoherent chunk, refused (the reference's mappedBasketId), not
            // an output stored without its basket.
            let local_basket_id = match output.basket_id {
                None => None,
                Some(bid) => Some(basket_id_map.get(&bid).copied().ok_or_else(|| {
                    Error::SyncError(format!(
                        "sync chunk output {} names basket {bid}, which the chunk does not map",
                        output.output_id
                    ))
                })?),
            };
            let local_tx_id = match transaction_id_map.get(&output.transaction_id).copied() {
                Some(tid) => Some(tid),
                None => find_transaction_id_for_sync(&mut *conn, user_id, &output.txid).await?,
            };
            let Some(tx_id) = local_tx_id else { continue };
            let upsert_result =
                upsert_output(&mut *conn, user_id, output, tx_id, local_basket_id).await?;
            remap(
                output_id_map,
                "output",
                output.output_id,
                upsert_result.local_id,
            )?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, output.updated_at);
        }
    }

    // Process tx label maps (need transaction and label ID translation)
    if let Some(maps) = &chunk.tx_label_maps {
        for map in maps {
            let local_tx_id = transaction_id_map.get(&map.transaction_id).copied();
            let local_label_id = label_id_map.get(&map.label_id).copied();
            if let (Some(tx_id), Some(label_id)) = (local_tx_id, local_label_id) {
                let upsert_result = upsert_tx_label_map(&mut *conn, map, tx_id, label_id).await?;
                if upsert_result.is_new {
                    result.inserts += 1;
                } else {
                    result.updates += 1;
                }
                update_max_updated_at(&mut result.max_updated_at, map.updated_at);
            }
        }
    }

    // Process output tag maps (need output and tag ID translation)
    if let Some(maps) = &chunk.output_tag_maps {
        for map in maps {
            let local_output_id = output_id_map.get(&map.output_id).copied();
            let local_tag_id = tag_id_map.get(&map.tag_id).copied();
            if let (Some(output_id), Some(tag_id)) = (local_output_id, local_tag_id) {
                let upsert_result =
                    upsert_output_tag_map(&mut *conn, map, output_id, tag_id).await?;
                if upsert_result.is_new {
                    result.inserts += 1;
                } else {
                    result.updates += 1;
                }
                update_max_updated_at(&mut result.max_updated_at, map.updated_at);
            }
        }
    }

    // Process certificates
    if let Some(certs) = &chunk.certificates {
        for cert in certs {
            let upsert_result = upsert_certificate(&mut *conn, user_id, cert).await?;
            remap(
                certificate_id_map,
                "certificate",
                cert.certificate_id,
                upsert_result.local_id,
            )?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, cert.updated_at);
        }
    }

    // Process certificate fields (need certificate ID translation)
    if let Some(fields) = &chunk.certificate_fields {
        for field in fields {
            let local_cert_id = certificate_id_map.get(&field.certificate_id).copied();
            if let Some(cert_id) = local_cert_id {
                let upsert_result =
                    upsert_certificate_field(&mut *conn, user_id, field, cert_id).await?;
                if upsert_result.is_new {
                    result.inserts += 1;
                } else {
                    result.updates += 1;
                }
                update_max_updated_at(&mut result.max_updated_at, field.updated_at);
            }
        }
    }

    // Process commissions (need transaction ID translation)
    if let Some(commissions) = &chunk.commissions {
        for comm in commissions {
            let local_tx_id = transaction_id_map.get(&comm.transaction_id).copied();
            if let Some(tx_id) = local_tx_id {
                let upsert_result = upsert_commission(&mut *conn, user_id, comm, tx_id).await?;
                if upsert_result.is_new {
                    result.inserts += 1;
                } else {
                    result.updates += 1;
                }
                update_max_updated_at(&mut result.max_updated_at, comm.updated_at);
            }
        }
    }

    // Process proven tx reqs last, as the reference does
    if let Some(reqs) = &chunk.proven_tx_reqs {
        for req in reqs {
            let upsert_result = upsert_proven_tx_req(&mut *conn, req, transaction_id_map).await?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, req.updated_at);
        }
    }

    Ok(result)
}

// =============================================================================
// Helper Functions for Fetching Entities
// =============================================================================

fn estimate_size<T: Serialize>(items: &[T]) -> u32 {
    serde_json::to_string(items)
        .map(|s| s.len() as u32)
        .unwrap_or(0)
}

fn update_max_updated_at(max: &mut Option<DateTime<Utc>>, updated_at: DateTime<Utc>) {
    match max {
        Some(current) if updated_at > *current => *max = Some(updated_at),
        None => *max = Some(updated_at),
        _ => {}
    }
}

async fn fetch_baskets_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableOutputBasket>> {
    let mut sql = String::from(
        r#"
        SELECT basket_id, user_id, name, number_of_desired_utxos, minimum_desired_utxo_value,
               created_at, updated_at
        FROM output_baskets
        WHERE user_id = ? AND is_deleted = 0
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let baskets = rows
        .iter()
        .map(|row| TableOutputBasket {
            basket_id: row.get("basket_id"),
            user_id: row.get("user_id"),
            name: row.get("name"),
            number_of_desired_utxos: row.get("number_of_desired_utxos"),
            minimum_desired_utxo_value: row.get("minimum_desired_utxo_value"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(baskets)
}

async fn fetch_proven_txs_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableProvenTx>> {
    let mut sql = String::from(
        r#"
        SELECT proven_tx_id, txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx,
               created_at, updated_at
        FROM proven_txs
        WHERE EXISTS (
            SELECT 1 FROM transactions t
            WHERE t.proven_tx_id = proven_txs.proven_tx_id AND t.user_id = ?
        )
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let proven_txs = rows
        .iter()
        .map(|row| TableProvenTx {
            proven_tx_id: row.get("proven_tx_id"),
            txid: row.get("txid"),
            height: row.get("height"),
            index: row.get("idx"),
            block_hash: row.get("block_hash"),
            merkle_root: row.get("merkle_root"),
            merkle_path: row.get("merkle_path"),
            raw_tx: row.get("raw_tx"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(proven_txs)
}

async fn fetch_proven_tx_reqs_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableProvenTxReq>> {
    let mut sql = String::from(
        r#"
        SELECT proven_tx_req_id, proven_tx_id, txid, status, attempts, history, notified,
               notify, batch, raw_tx, input_beef, created_at, updated_at
        FROM proven_tx_reqs
        WHERE EXISTS (
            SELECT 1 FROM transactions t
            WHERE t.txid = proven_tx_reqs.txid AND t.user_id = ?
        )
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let reqs = rows
        .iter()
        .map(|row| {
            let status_str: String = row.get("status");
            // Map all 17 canonical ProvenTxReqStatus variants. The
            // pre-PR mapping collapsed the 12 broadcast-lifecycle
            // variants (sending / unmined / unsent / callback /
            // unconfirmed / unfail / nosend / invalid / doubleSpend /
            // nonfinal / unprocessed / unknown) into `Pending`, which
            // silently broke `TaskSendWaiting` + `TaskCheckForProofs`
            // when an L2-restored row landed in the wrong bucket
            // (Calhoon PR #2 review item 3, 2026-05-30).
            //
            // Mirrors the write side at
            // `storage_sqlx.rs::update_proven_tx_req_status` exactly,
            // including the mixed-case forms ("inProgress",
            // "notFound", "doubleSpend") emitted by serde-camelCase
            // + the explicit serde renames to "nosend" / "nonfinal".
            // Snake-case + ALL-lowercase aliases preserved for forward
            // compat with any older writer that may have produced
            // them.
            let status = match status_str.as_str() {
                "pending" => ProvenTxReqStatus::Pending,
                "inProgress" | "inprogress" | "in_progress" => ProvenTxReqStatus::InProgress,
                "completed" => ProvenTxReqStatus::Completed,
                "failed" => ProvenTxReqStatus::Failed,
                "notFound" | "notfound" | "not_found" => ProvenTxReqStatus::NotFound,
                "unsent" => ProvenTxReqStatus::Unsent,
                "sending" => ProvenTxReqStatus::Sending,
                "unmined" => ProvenTxReqStatus::Unmined,
                "unknown" => ProvenTxReqStatus::Unknown,
                "callback" => ProvenTxReqStatus::Callback,
                "unconfirmed" => ProvenTxReqStatus::Unconfirmed,
                "unfail" => ProvenTxReqStatus::Unfail,
                "nosend" | "noSend" => ProvenTxReqStatus::NoSend,
                "invalid" => ProvenTxReqStatus::Invalid,
                "doubleSpend" | "doublespend" | "double_spend" => ProvenTxReqStatus::DoubleSpend,
                "nonfinal" | "nonFinal" | "non_final" => ProvenTxReqStatus::NonFinal,
                "unprocessed" => ProvenTxReqStatus::Unprocessed,
                other => {
                    // Unrecognised status — surface a warning instead
                    // of silently collapsing to `Pending` (which
                    // misroutes downstream Monitor tasks). The closest
                    // canonical "no signal" state is `Unknown`; the
                    // log line is the breadcrumb that lets us track
                    // a writer drift if one ever lands.
                    tracing::warn!(
                        status = other,
                        "fetch_proven_tx_reqs_for_sync: unrecognised \
                         ProvenTxReqStatus, mapping to Unknown"
                    );
                    ProvenTxReqStatus::Unknown
                }
            };
            let notified_val: i32 = row.get("notified");

            TableProvenTxReq {
                proven_tx_req_id: row.get("proven_tx_req_id"),
                txid: row.get("txid"),
                status,
                attempts: row.get("attempts"),
                history: row.get("history"),
                notified: notified_val != 0,
                notify: row.try_get::<String, _>("notify").unwrap_or_default(),
                raw_tx: row.try_get("raw_tx").ok().flatten(),
                input_beef: row.try_get("input_beef").ok().flatten(),
                proven_tx_id: row.get("proven_tx_id"),
                batch: row.try_get("batch").ok(),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            }
        })
        .collect();

    Ok(reqs)
}

async fn fetch_tx_labels_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableTxLabel>> {
    let mut sql = String::from(
        r#"
        SELECT tx_label_id, user_id, label, created_at, updated_at
        FROM tx_labels
        WHERE user_id = ? AND is_deleted = 0
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let labels = rows
        .iter()
        .map(|row| TableTxLabel {
            label_id: row.get("tx_label_id"),
            user_id: row.get("user_id"),
            label: row.get("label"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(labels)
}

async fn fetch_output_tags_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableOutputTag>> {
    let mut sql = String::from(
        r#"
        SELECT output_tag_id, user_id, tag, created_at, updated_at
        FROM output_tags
        WHERE user_id = ? AND is_deleted = 0
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let tags = rows
        .iter()
        .map(|row| TableOutputTag {
            tag_id: row.get("output_tag_id"),
            user_id: row.get("user_id"),
            tag: row.get("tag"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(tags)
}

async fn fetch_transactions_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableTransaction>> {
    let mut sql = String::from(
        r#"
        SELECT t.transaction_id, t.user_id, t.txid, t.status, t.reference, t.description,
               t.satoshis, t.version, t.lock_time, t.raw_tx, t.input_beef, t.is_outgoing,
               p.txid AS proof_txid, t.created_at, t.updated_at
        FROM transactions t
        LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id
        WHERE t.user_id = ?
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND t.updated_at > ?");
    }

    sql.push_str(" ORDER BY t.updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let transactions = rows
        .iter()
        .map(|row| {
            let status_str: String = row.get("status");
            let status = match status_str.as_str() {
                "completed" => TransactionStatus::Completed,
                "unprocessed" => TransactionStatus::Unprocessed,
                "sending" => TransactionStatus::Sending,
                "unproven" => TransactionStatus::Unproven,
                "unsigned" => TransactionStatus::Unsigned,
                "nosend" => TransactionStatus::NoSend,
                "nonfinal" => TransactionStatus::NonFinal,
                "failed" => TransactionStatus::Failed,
                "unfail" => TransactionStatus::Unfail,
                _ => TransactionStatus::Unprocessed,
            };

            TableTransaction {
                transaction_id: row.get("transaction_id"),
                user_id: row.get("user_id"),
                txid: row.get("txid"),
                status,
                reference: row.get("reference"),
                description: row.get("description"),
                satoshis: row.get("satoshis"),
                version: row.get("version"),
                lock_time: row.get("lock_time"),
                raw_tx: row.get("raw_tx"),
                input_beef: row.get("input_beef"),
                is_outgoing: row.get("is_outgoing"),
                // The proof a transaction is linked to travels as that
                // proof's txid (the field's own meaning): a proof row is
                // global by txid, and the writer finds its own row by it.
                proof_txid: row.get("proof_txid"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            }
        })
        .collect();

    Ok(transactions)
}

async fn fetch_outputs_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableOutput>> {
    let mut sql = String::from(
        r#"
        SELECT output_id, user_id, transaction_id, basket_id, txid, vout, satoshis,
               locking_script, script_length, script_offset, type, provided_by, purpose,
               spendable, change, derivation_prefix, derivation_suffix, sender_identity_key,
               custom_instructions, created_at, updated_at
        FROM outputs
        WHERE user_id = ?
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let outputs = rows
        .iter()
        .map(|row| TableOutput {
            output_id: row.get("output_id"),
            user_id: row.get("user_id"),
            transaction_id: row.get("transaction_id"),
            basket_id: row.get("basket_id"),
            txid: row.get("txid"),
            vout: row.get("vout"),
            satoshis: row.get("satoshis"),
            locking_script: row.get("locking_script"),
            script_length: row.get("script_length"),
            script_offset: row.get("script_offset"),
            output_type: row.get("type"),
            provided_by: row.get("provided_by"),
            purpose: row.get("purpose"),
            output_description: row.try_get("output_description").ok(),
            spent_by: row.try_get("spent_by").ok().flatten(),
            sequence_number: row
                .try_get::<Option<i32>, _>("sequence_number")
                .ok()
                .flatten()
                .map(|v| v as u32),
            spending_description: row.try_get("spending_description").ok(),
            spendable: row.get("spendable"),
            change: row.get("change"),
            derivation_prefix: row.get("derivation_prefix"),
            derivation_suffix: row.get("derivation_suffix"),
            sender_identity_key: row.get("sender_identity_key"),
            custom_instructions: row.get("custom_instructions"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(outputs)
}

async fn fetch_tx_label_maps_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableTxLabelMap>> {
    let mut sql = String::from(
        r#"
        SELECT m.tx_label_map_id, m.transaction_id, m.tx_label_id, m.created_at, m.updated_at
        FROM tx_labels_map m
        JOIN transactions t ON m.transaction_id = t.transaction_id
        WHERE t.user_id = ? AND m.is_deleted = 0
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND m.updated_at > ?");
    }

    sql.push_str(" ORDER BY m.updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let maps = rows
        .iter()
        .map(|row| TableTxLabelMap {
            tx_label_map_id: row.get("tx_label_map_id"),
            transaction_id: row.get("transaction_id"),
            label_id: row.get("tx_label_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(maps)
}

async fn fetch_output_tag_maps_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableOutputTagMap>> {
    let mut sql = String::from(
        r#"
        SELECT m.output_tag_map_id, m.output_id, m.output_tag_id, m.created_at, m.updated_at
        FROM output_tags_map m
        JOIN outputs o ON m.output_id = o.output_id
        WHERE o.user_id = ? AND m.is_deleted = 0
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND m.updated_at > ?");
    }

    sql.push_str(" ORDER BY m.updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let maps = rows
        .iter()
        .map(|row| TableOutputTagMap {
            output_tag_map_id: row.get("output_tag_map_id"),
            output_id: row.get("output_id"),
            tag_id: row.get("output_tag_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(maps)
}

async fn fetch_certificates_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableCertificate>> {
    let mut sql = String::from(
        r#"
        SELECT certificate_id, user_id, type, serial_number, certifier, subject, verifier,
               revocation_outpoint, signature, created_at, updated_at
        FROM certificates
        WHERE user_id = ? AND is_deleted = 0
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let certs = rows
        .iter()
        .map(|row| TableCertificate {
            certificate_id: row.get("certificate_id"),
            user_id: row.get("user_id"),
            cert_type: row.get("type"),
            serial_number: row.get("serial_number"),
            certifier: row.get("certifier"),
            subject: row.get("subject"),
            verifier: row.get("verifier"),
            revocation_outpoint: row.get("revocation_outpoint"),
            signature: row.get("signature"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(certs)
}

async fn fetch_certificate_fields_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableCertificateField>> {
    let mut sql = String::from(
        r#"
        SELECT certificate_field_id, certificate_id, user_id, field_name, field_value, master_key,
               created_at, updated_at
        FROM certificate_fields
        WHERE user_id = ?
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let fields = rows
        .iter()
        .map(|row| TableCertificateField {
            certificate_field_id: row.get("certificate_field_id"),
            certificate_id: row.get("certificate_id"),
            user_id: row.get("user_id"),
            field_name: row.get("field_name"),
            field_value: row.get("field_value"),
            master_key: row.get("master_key"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(fields)
}

async fn fetch_commissions_for_sync(
    storage: &StorageSqlx,
    user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableCommission>> {
    let mut sql = String::from(
        r#"
        SELECT commission_id, user_id, transaction_id, satoshis, locking_script,
               key_offset, is_redeemed, created_at, updated_at
        FROM commissions
        WHERE user_id = ?
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql).bind(user_id);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let commissions = rows
        .iter()
        .map(|row| TableCommission {
            commission_id: row.get("commission_id"),
            user_id: row.get("user_id"),
            transaction_id: row.get("transaction_id"),
            satoshis: row.get("satoshis"),
            payer_locking_script: row.get("locking_script"),
            key_offset: row.get("key_offset"),
            is_redeemed: row.get("is_redeemed"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();

    Ok(commissions)
}

// =============================================================================
// Upsert Helper Functions
// =============================================================================

struct UpsertResult {
    local_id: i64,
    is_new: bool,
}

struct MergeResult {
    updated: bool,
}

async fn merge_user(
    conn: &mut SqliteConnection,
    user_id: i64,
    identity_key: &str,
    chunk_user: &TableUser,
) -> Result<MergeResult> {
    // The user row is merged for the user the chunk is for: a row naming
    // any other identity refuses the chunk, as the reference's
    // EntityUser.mergeFind and mergeNew refuse it.
    if chunk_user.identity_key != identity_key {
        return Err(Error::SyncError(
            "sync chunk user row names an identity other than the chunk's".to_string(),
        ));
    }
    // Get current user
    let current: DateTime<Utc> =
        sqlx::query_scalar("SELECT updated_at FROM users WHERE user_id = ?")
            .bind(user_id)
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::UserNotFound(user_id.to_string()))?;

    // Only update if chunk is newer
    if chunk_user.updated_at > current {
        if let Some(ref active_storage) = chunk_user.active_storage {
            sqlx::query("UPDATE users SET active_storage = ?, updated_at = ? WHERE user_id = ?")
                .bind(active_storage)
                .bind(Utc::now())
                .bind(user_id)
                .execute(&mut *conn)
                .await?;
        }
        return Ok(MergeResult { updated: true });
    }

    Ok(MergeResult { updated: false })
}

/// The sync state row for the chunk's source store, found or inserted on the
/// chunk's own transaction (as `find_or_insert_sync_state_internal` does on
/// the writer).
async fn ensure_sync_state(
    conn: &mut SqliteConnection,
    user_id: i64,
    from_storage_identity_key: &str,
) -> Result<()> {
    let found: Option<i64> = sqlx::query_scalar(
        "SELECT sync_state_id FROM sync_states WHERE user_id = ? AND storage_identity_key = ?",
    )
    .bind(user_id)
    .bind(from_storage_identity_key)
    .fetch_optional(&mut *conn)
    .await?;
    if found.is_none() {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO sync_states (user_id, storage_identity_key, storage_name, status, init, ref_num, sync_map, created_at, updated_at)
            VALUES (?, ?, 'sync', 'unknown', 0, ?, '{}', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(from_storage_identity_key)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

async fn upsert_basket(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket: &TableOutputBasket,
) -> Result<UpsertResult> {
    // Check if exists by name (unique per user)
    let existing = sqlx::query(
        r#"
        SELECT basket_id, updated_at FROM output_baskets
        WHERE user_id = ? AND name = ?
        "#,
    )
    .bind(user_id)
    .bind(&basket.name)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("basket_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if basket.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE output_baskets
                SET number_of_desired_utxos = ?, minimum_desired_utxo_value = ?, updated_at = ?
                WHERE basket_id = ?
                "#,
            )
            .bind(basket.number_of_desired_utxos)
            .bind(basket.minimum_desired_utxo_value)
            .bind(basket.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&basket.name)
        .bind(basket.number_of_desired_utxos)
        .bind(basket.minimum_desired_utxo_value)
        .bind(basket.created_at)
        .bind(basket.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_proven_tx(
    conn: &mut SqliteConnection,
    ptx: &TableProvenTx,
    decisions: &super::sync_proof::ProofDecisions,
) -> Result<UpsertResult> {
    // Global by txid, as the reference's EntityProvenTx.mergeFind.
    let existing = super::sync_proof::held_proof(&mut *conn, &ptx.txid).await?;

    if let Some(held) = existing {
        let local_id = held.proven_tx_id;
        let same = super::sync_proof::same_proof(&held, ptx);
        // The reference's EntityProvenTx.mergeExisting: the same proof is
        // not written; a different one replaces the held row only when the
        // preflight validated it.
        if !same {
            let Some(block_hash) = decisions.replacement_authorized.get(&ptx.txid) else {
                let why = if decisions.insert_only.contains(&ptx.txid) {
                    "a concurrent proof row appeared; retry synchronization"
                } else {
                    "replacement requires active-chain validation"
                };
                return Err(Error::SyncError(format!(
                    "sync chunk proof {} is refused: {why}",
                    ptx.txid
                )));
            };
            sqlx::query(
                r#"
                UPDATE proven_txs
                SET height = ?, idx = ?, block_hash = ?, merkle_root = ?, merkle_path = ?, raw_tx = ?, updated_at = ?
                WHERE proven_tx_id = ?
                "#,
            )
            .bind(ptx.height)
            .bind(ptx.index)
            .bind(block_hash)
            .bind(&ptx.merkle_root)
            .bind(&ptx.merkle_path)
            .bind(&ptx.raw_tx)
            .bind(ptx.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // A new proof row is taken as given, as the reference's direct sync
        // takes one.
        let result = sqlx::query(
            r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&ptx.txid)
        .bind(ptx.height)
        .bind(ptx.index)
        .bind(&ptx.block_hash)
        .bind(&ptx.merkle_root)
        .bind(&ptx.merkle_path)
        .bind(&ptx.raw_tx)
        .bind(ptx.created_at)
        .bind(ptx.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

/// A request status in the reference's own words (`doubleSpend`,
/// `inProgress`, `notFound`, ...): the entity's serde name.
fn req_status_word(status: &ProvenTxReqStatus) -> Result<String> {
    match serde_json::to_value(status)? {
        serde_json::Value::String(word) => Ok(word),
        other => Err(Error::SyncError(format!("request status {other}"))),
    }
}

/// The reference's `addHistoryNote` over a request's `history` JSON: a note
/// equal in every property to a held note is not added; notes are kept in
/// `when what` order. A history that is not the `{ notes: [...] }` shape
/// adds nothing and is left as stored.
fn merge_history(local: &str, incoming: &str) -> String {
    let Ok(mut held) = serde_json::from_str::<serde_json::Value>(local) else {
        return local.to_string();
    };
    let Ok(theirs) = serde_json::from_str::<serde_json::Value>(incoming) else {
        return local.to_string();
    };
    let Some(new_notes) = theirs.get("notes").and_then(|n| n.as_array()).cloned() else {
        return local.to_string();
    };
    let Some(obj) = held.as_object_mut() else {
        return local.to_string();
    };
    let notes = obj
        .entry("notes")
        .or_insert_with(|| serde_json::Value::Array(vec![]));
    let Some(notes) = notes.as_array_mut() else {
        return local.to_string();
    };
    let mut added = false;
    for mut note in new_notes {
        let Some(n) = note.as_object_mut() else {
            continue;
        };
        n.entry("when")
            .or_insert_with(|| serde_json::Value::String(Utc::now().to_rfc3339()));
        let equal = notes.iter().any(|h| {
            h.as_object()
                .is_some_and(|h| h.iter().all(|(k, v)| n.get(k) == Some(v)))
        });
        if !equal {
            notes.push(note);
            added = true;
        }
    }
    if !added {
        return local.to_string();
    }
    let key = |n: &serde_json::Value| {
        let s = |k: &str| n.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
        format!("{} {}", s("when"), s("what"))
    };
    notes.sort_by_key(key);
    held.to_string()
}

/// The `transactionIds` of a request's `notify` JSON, mapped through the
/// transaction id map; an id the map does not hold is dropped.
fn mapped_notify_ids(notify: &str, transaction_id_map: &HashMap<i64, i64>) -> Option<Vec<i64>> {
    let v: serde_json::Value = serde_json::from_str(notify).ok()?;
    let ids = v.get("transactionIds")?.as_array()?;
    Some(
        ids.iter()
            .filter_map(|id| id.as_i64())
            .filter_map(|id| transaction_id_map.get(&id).copied())
            .collect(),
    )
}

/// `notify` with `ids` added to its `transactionIds` (a sorted set). A
/// `notify` that is not a JSON object is replaced by one holding `ids`.
fn notify_with_ids(notify: &str, ids: &[i64], keep_held: bool) -> String {
    let mut v: serde_json::Value =
        serde_json::from_str(notify).unwrap_or_else(|_| serde_json::json!({}));
    if !v.is_object() {
        v = serde_json::json!({});
    }
    let mut set: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    if keep_held {
        if let Some(held) = v.get("transactionIds").and_then(|t| t.as_array()) {
            set.extend(held.iter().filter_map(|id| id.as_i64()));
        }
    }
    set.extend(ids.iter().copied());
    v["transactionIds"] = serde_json::json!(set.into_iter().collect::<Vec<_>>());
    v.to_string()
}

async fn upsert_proven_tx_req(
    conn: &mut SqliteConnection,
    req: &TableProvenTxReq,
    transaction_id_map: &HashMap<i64, i64>,
) -> Result<UpsertResult> {
    let existing = sqlx::query(
        r#"
        SELECT proven_tx_req_id, history, notify, notified, batch, updated_at
        FROM proven_tx_reqs WHERE txid = ?
        "#,
    )
    .bind(&req.txid)
    .fetch_optional(&mut *conn)
    .await?;
    let incoming_notify_ids = mapped_notify_ids(&req.notify, transaction_id_map);

    if let Some(row) = existing {
        // The reference's EntityProvenTxReq.mergeExisting: the status, the
        // attempts, the proof link, the bytes and the broadcast flag stay as
        // this store holds them; the history and the notify ids are unioned;
        // a batch is taken only where none is held, and two batches that
        // differ refuse the chunk.
        let local_id: i64 = row.get("proven_tx_req_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");
        let local_history: String = row.get("history");
        let local_notify: String = row.get("notify");
        let mut notified: bool = row.get::<i64, _>("notified") != 0;
        let local_batch: Option<String> = row.get("batch");
        let batch = match (&local_batch, &req.batch) {
            (None, Some(theirs)) => Some(theirs.clone()),
            (Some(held), Some(theirs)) if held != theirs => {
                return Err(Error::SyncError(format!(
                    "sync chunk request {} batch {theirs} differs from the held batch {held}",
                    req.txid
                )));
            }
            _ => local_batch.clone(),
        };
        let history = merge_history(&local_history, &req.history);
        let notify = match incoming_notify_ids {
            Some(ids) if !ids.is_empty() => {
                notified = false;
                notify_with_ids(&local_notify, &ids, true)
            }
            _ => local_notify,
        };
        sqlx::query(
            r#"
            UPDATE proven_tx_reqs
            SET history = ?, notify = ?, notified = ?, batch = ?, updated_at = ?
            WHERE proven_tx_req_id = ?
            "#,
        )
        .bind(&history)
        .bind(&notify)
        .bind(notified as i32)
        .bind(&batch)
        .bind(req.updated_at.max(local_updated))
        .bind(local_id)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new, as the reference's mergeNew: a request that carries a
        // proof link is linked to the proof of its own txid when this store
        // holds it (whichever chunk of the sync brought it), the notify ids
        // mapped through the transaction map, the status in the reference's
        // own words. raw_tx is BLOB NOT NULL: a missing raw_tx is stored
        // empty (real reqs always carry it).
        let proven_tx_id = match req.proven_tx_id {
            Some(_) => local_proof_id(&mut *conn, Some(&req.txid), Some(&req.txid)).await?,
            None => None,
        };
        let (notify, notified) = match incoming_notify_ids {
            Some(ids) if !ids.is_empty() => (notify_with_ids(&req.notify, &ids, false), false),
            Some(ids) => (notify_with_ids(&req.notify, &ids, false), req.notified),
            None => (req.notify.clone(), req.notified),
        };
        let status = req_status_word(&req.status)?;
        let result = sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, input_beef, proven_tx_id, batch, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&req.txid)
        .bind(&status)
        .bind(req.attempts)
        .bind(&req.history)
        .bind(notified as i32)
        .bind(&notify)
        .bind(req.raw_tx.clone().unwrap_or_default())
        .bind(&req.input_beef)
        .bind(proven_tx_id)
        .bind(&req.batch)
        .bind(req.created_at)
        .bind(req.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_tx_label(
    conn: &mut SqliteConnection,
    user_id: i64,
    label: &TableTxLabel,
) -> Result<UpsertResult> {
    // Check if exists by label name (unique per user)
    let existing = sqlx::query(
        r#"
        SELECT tx_label_id, updated_at FROM tx_labels
        WHERE user_id = ? AND label = ?
        "#,
    )
    .bind(user_id)
    .bind(&label.label)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("tx_label_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if label.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE tx_labels SET updated_at = ? WHERE tx_label_id = ?
                "#,
            )
            .bind(label.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO tx_labels (user_id, label, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&label.label)
        .bind(label.created_at)
        .bind(label.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_output_tag(
    conn: &mut SqliteConnection,
    user_id: i64,
    tag: &TableOutputTag,
) -> Result<UpsertResult> {
    // Check if exists by tag name (unique per user)
    let existing = sqlx::query(
        r#"
        SELECT output_tag_id, updated_at FROM output_tags
        WHERE user_id = ? AND tag = ?
        "#,
    )
    .bind(user_id)
    .bind(&tag.tag)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("output_tag_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if tag.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE output_tags SET updated_at = ? WHERE output_tag_id = ?
                "#,
            )
            .bind(tag.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO output_tags (user_id, tag, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&tag.tag)
        .bind(tag.created_at)
        .bind(tag.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

/// The local proof row a synced row's proof link names: the link is the
/// proof's txid, a proof row is global by txid, and a row's proof is the
/// proof of its own txid. A link naming another txid, or a proof row this
/// store does not hold, is not stored.
async fn local_proof_id(
    conn: &mut SqliteConnection,
    own_txid: Option<&str>,
    proof_txid: Option<&str>,
) -> Result<Option<i64>> {
    let (Some(txid), Some(own)) = (proof_txid, own_txid) else {
        return Ok(None);
    };
    if txid != own {
        return Ok(None);
    }
    Ok(
        sqlx::query_scalar("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?,
    )
}

async fn upsert_transaction(
    conn: &mut SqliteConnection,
    user_id: i64,
    tx: &TableTransaction,
) -> Result<UpsertResult> {
    let proven_tx_id =
        local_proof_id(&mut *conn, tx.txid.as_deref(), tx.proof_txid.as_deref()).await?;
    // Check if exists by reference (unique per user)
    let existing = sqlx::query(
        r#"
        SELECT transaction_id, updated_at, proven_tx_id FROM transactions
        WHERE user_id = ? AND reference = ?
        "#,
    )
    .bind(user_id)
    .bind(&tx.reference)
    .fetch_optional(&mut *conn)
    .await?;

    let status_str = tx.status.as_str();

    if let Some(row) = existing {
        let local_id: i64 = row.get("transaction_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");
        let local_proof: Option<i64> = row.get("proven_tx_id");

        // The reference's EntityTransaction.mergeExisting (shouldMerge, with
        // no BRC-177 expiry state on either side, which this line does not
        // hold): a newer row merges its ordinary properties; an older row
        // that carries a proof link where this store holds none merges its
        // proof evidence alone (status, the link, the bytes where none are
        // held). The reference is never updated.
        let adds_proof_evidence = proven_tx_id.is_some() && local_proof.is_none();
        let incoming_is_newer = tx.updated_at > local_updated;
        if incoming_is_newer {
            sqlx::query(
                r#"
                UPDATE transactions
                SET txid = ?, status = ?, description = ?, satoshis = ?, version = ?, lock_time = ?,
                    raw_tx = ?, input_beef = ?, is_outgoing = ?, proven_tx_id = ?, updated_at = ?
                WHERE transaction_id = ?
                "#,
            )
            .bind(&tx.txid)
            .bind(status_str)
            .bind(&tx.description)
            .bind(tx.satoshis)
            .bind(tx.version)
            .bind(tx.lock_time)
            .bind(&tx.raw_tx)
            .bind(&tx.input_beef)
            .bind(tx.is_outgoing)
            .bind(proven_tx_id)
            .bind(tx.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        } else if adds_proof_evidence {
            sqlx::query(
                r#"
                UPDATE transactions
                SET status = ?, proven_tx_id = ?, raw_tx = COALESCE(raw_tx, ?),
                    input_beef = COALESCE(input_beef, ?)
                WHERE transaction_id = ?
                "#,
            )
            .bind(status_str)
            .bind(proven_tx_id)
            .bind(&tx.raw_tx)
            .bind(&tx.input_beef)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, raw_tx, input_beef, is_outgoing, proven_tx_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&tx.txid)
        .bind(status_str)
        .bind(&tx.reference)
        .bind(&tx.description)
        .bind(tx.satoshis)
        .bind(tx.version)
        .bind(tx.lock_time)
        .bind(&tx.raw_tx)
        .bind(&tx.input_beef)
        .bind(tx.is_outgoing)
        .bind(proven_tx_id)
        .bind(tx.created_at)
        .bind(tx.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

/// Resolve a parent transaction's local FK by its on-chain txid.
///
/// Used by the outputs apply-loop when the session's
/// `transaction_id_map` does not record the remote→local mapping —
/// typically because the parent transaction landed in local storage
/// during a prior sync session or via a local `createAction`, and the
/// current chunk's `transactions` section did not re-deliver it.
///
/// Returns `None` only when the parent is genuinely absent locally;
/// the orchestrator then treats the output as orphan and skips it,
/// consistent with the skip-on-missing-parent pattern already used by
/// `tx_label_maps`, `output_tag_maps`, `commissions`, and
/// `certificate_fields`.
async fn find_transaction_id_for_sync(
    conn: &mut SqliteConnection,
    user_id: i64,
    txid: &str,
) -> Result<Option<i64>> {
    let row = sqlx::query(
        r#"
        SELECT transaction_id FROM transactions
        WHERE user_id = ? AND txid = ?
        LIMIT 1
        "#,
    )
    .bind(user_id)
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|r| r.get::<i64, _>("transaction_id")))
}

/// The `purpose` a synced output is inserted with: the source row's, or `''`
/// when the chunk carries none. The column is `NOT NULL` and a source that
/// does not send the column says nothing of the row; `''` is what beta's
/// writer stores there (Go's zero value), and it is not `receive`, so such
/// a row is selected as the wallet's own change.
fn stored_purpose(output: &TableOutput) -> &str {
    output.purpose.as_deref().unwrap_or("")
}

async fn upsert_output(
    conn: &mut SqliteConnection,
    user_id: i64,
    output: &TableOutput,
    tx_id: i64,
    local_basket_id: Option<i64>,
) -> Result<UpsertResult> {
    // The SELECT key must match the schema's UNIQUE constraint exactly.
    // `outputs` is declared `UNIQUE(transaction_id, vout, user_id)`.
    // The prior SELECT used `(user_id, txid, vout)` — the denormalized
    // natural key — which can diverge from the UNIQUE key under either
    // of the following:
    //
    //   * Local storage already contains a row whose `txid` column is
    //     stale relative to its `transaction_id` FK (possible from a
    //     past sync attempt under the buggy `unwrap_or(remote_fk)`
    //     fallback). SELECT-by-txid matches the stale row; the
    //     subsequent UPDATE rewrites its `transaction_id` to the
    //     resolved value, colliding with a DIFFERENT existing row
    //     whose `(transaction_id, vout, user_id)` already matches.
    //
    //   * A freshly synced chunk re-resolves a parent transaction to
    //     a new local `transaction_id` (cross-chunk re-resolution);
    //     SELECT-by-txid misses and INSERT fires, colliding with the
    //     same logical UTXO already present at the UNIQUE key.
    //
    // Keying the SELECT on `(user_id, transaction_id, vout)` removes
    // the divergence. This also matches the canonical TS storage layer
    // (`EntityOutput.mergeFind` keys on
    // `partial: { userId, transactionId, vout }`).
    let existing = sqlx::query(
        r#"
        SELECT output_id, updated_at FROM outputs
        WHERE user_id = ? AND transaction_id = ? AND vout = ?
        "#,
    )
    .bind(user_id)
    .bind(tx_id)
    .bind(output.vout)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("output_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer, the columns the reference's
        // EntityOutput.mergeExisting copies: never the transaction, vout,
        // satoshis or key derivation of a held coin. `provided_by` and `purpose` are the
        // source row's, as EntityOutput.mergeExisting assigns them (ts-stack
        // 4d0ea63c8): the received-coin rule reads `purpose`. A chunk that
        // carries no `purpose` says nothing of either column, so the row
        // keeps both as stored (`COALESCE` of a NULL bind).
        if output.updated_at > local_updated {
            let said = output.purpose.is_some();
            sqlx::query(
                r#"
                UPDATE outputs
                SET basket_id = ?, locking_script = ?,
                    script_length = ?, script_offset = ?, type = ?, spendable = ?, change = ?,
                    sender_identity_key = ?,
                    custom_instructions = ?, provided_by = COALESCE(?, provided_by),
                    purpose = COALESCE(?, purpose), updated_at = ?
                WHERE output_id = ?
                "#,
            )
            .bind(local_basket_id)
            .bind(&output.locking_script)
            .bind(output.script_length)
            .bind(output.script_offset)
            .bind(&output.output_type)
            .bind(output.spendable)
            .bind(output.change)
            .bind(&output.sender_identity_key)
            .bind(&output.custom_instructions)
            .bind(said.then_some(output.provided_by.as_str()))
            .bind(output.purpose.as_deref())
            .bind(output.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new: `provided_by` and `purpose` are the source row's, as
        // EntityOutput.mergeNew inserts the merged row whole (ts-stack
        // 4d0ea63c8).
        let result = sqlx::query(
            r#"
            INSERT INTO outputs (user_id, transaction_id, basket_id, txid, vout, satoshis, locking_script,
                script_length, script_offset, type, spendable, change, derivation_prefix, derivation_suffix,
                sender_identity_key, custom_instructions, provided_by, purpose, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(tx_id)
        .bind(local_basket_id)
        .bind(&output.txid)
        .bind(output.vout)
        .bind(output.satoshis)
        .bind(&output.locking_script)
        .bind(output.script_length)
        .bind(output.script_offset)
        .bind(&output.output_type)
        .bind(output.spendable)
        .bind(output.change)
        .bind(&output.derivation_prefix)
        .bind(&output.derivation_suffix)
        .bind(&output.sender_identity_key)
        .bind(&output.custom_instructions)
        .bind(&output.provided_by)
        .bind(stored_purpose(output))
        .bind(output.created_at)
        .bind(output.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_tx_label_map(
    conn: &mut SqliteConnection,
    map: &TableTxLabelMap,
    local_tx_id: i64,
    local_label_id: i64,
) -> Result<UpsertResult> {
    // Check if exists
    let existing = sqlx::query(
        r#"
        SELECT tx_label_map_id, updated_at FROM tx_labels_map
        WHERE transaction_id = ? AND tx_label_id = ?
        "#,
    )
    .bind(local_tx_id)
    .bind(local_label_id)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("tx_label_map_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if map.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE tx_labels_map SET updated_at = ? WHERE tx_label_map_id = ?
                "#,
            )
            .bind(map.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO tx_labels_map (transaction_id, tx_label_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(local_tx_id)
        .bind(local_label_id)
        .bind(map.created_at)
        .bind(map.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_output_tag_map(
    conn: &mut SqliteConnection,
    map: &TableOutputTagMap,
    local_output_id: i64,
    local_tag_id: i64,
) -> Result<UpsertResult> {
    // Check if exists
    let existing = sqlx::query(
        r#"
        SELECT output_tag_map_id, updated_at FROM output_tags_map
        WHERE output_id = ? AND output_tag_id = ?
        "#,
    )
    .bind(local_output_id)
    .bind(local_tag_id)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("output_tag_map_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if map.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE output_tags_map SET updated_at = ? WHERE output_tag_map_id = ?
                "#,
            )
            .bind(map.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO output_tags_map (output_id, output_tag_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(local_output_id)
        .bind(local_tag_id)
        .bind(map.created_at)
        .bind(map.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_certificate(
    conn: &mut SqliteConnection,
    user_id: i64,
    cert: &TableCertificate,
) -> Result<UpsertResult> {
    // Check if exists by type + certifier + serial_number (unique per user)
    let existing = sqlx::query(
        r#"
        SELECT certificate_id, updated_at FROM certificates
        WHERE user_id = ? AND type = ? AND certifier = ? AND serial_number = ?
        "#,
    )
    .bind(user_id)
    .bind(&cert.cert_type)
    .bind(&cert.certifier)
    .bind(&cert.serial_number)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("certificate_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if cert.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE certificates
                SET subject = ?, verifier = ?, revocation_outpoint = ?, signature = ?, updated_at = ?
                WHERE certificate_id = ?
                "#,
            )
            .bind(&cert.subject)
            .bind(&cert.verifier)
            .bind(&cert.revocation_outpoint)
            .bind(&cert.signature)
            .bind(cert.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO certificates (user_id, type, serial_number, certifier, subject, verifier, revocation_outpoint, signature, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&cert.cert_type)
        .bind(&cert.serial_number)
        .bind(&cert.certifier)
        .bind(&cert.subject)
        .bind(&cert.verifier)
        .bind(&cert.revocation_outpoint)
        .bind(&cert.signature)
        .bind(cert.created_at)
        .bind(cert.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_certificate_field(
    conn: &mut SqliteConnection,
    user_id: i64,
    field: &TableCertificateField,
    local_cert_id: i64,
) -> Result<UpsertResult> {
    // Check if exists by certificate_id + field_name
    let existing = sqlx::query(
        r#"
        SELECT certificate_field_id, updated_at FROM certificate_fields
        WHERE certificate_id = ? AND field_name = ?
        "#,
    )
    .bind(local_cert_id)
    .bind(&field.field_name)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("certificate_field_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if field.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE certificate_fields
                SET field_value = ?, master_key = ?, updated_at = ?
                WHERE certificate_field_id = ?
                "#,
            )
            .bind(&field.field_value)
            .bind(&field.master_key)
            .bind(field.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO certificate_fields (certificate_id, user_id, field_name, field_value, master_key, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(local_cert_id)
        .bind(user_id)
        .bind(&field.field_name)
        .bind(&field.field_value)
        .bind(&field.master_key)
        .bind(field.created_at)
        .bind(field.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_commission(
    conn: &mut SqliteConnection,
    user_id: i64,
    comm: &TableCommission,
    local_tx_id: i64,
) -> Result<UpsertResult> {
    // Check if exists by transaction_id (one commission per transaction)
    let existing = sqlx::query(
        r#"
        SELECT commission_id, updated_at FROM commissions
        WHERE user_id = ? AND transaction_id = ?
        "#,
    )
    .bind(user_id)
    .bind(local_tx_id)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("commission_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if comm.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE commissions
                SET satoshis = ?, locking_script = ?, key_offset = ?, is_redeemed = ?, updated_at = ?
                WHERE commission_id = ?
                "#,
            )
            .bind(comm.satoshis)
            .bind(&comm.payer_locking_script)
            .bind(&comm.key_offset)
            .bind(comm.is_redeemed)
            .bind(comm.updated_at)
            .bind(local_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new
        let result = sqlx::query(
            r#"
            INSERT INTO commissions (user_id, transaction_id, satoshis, locking_script, key_offset, is_redeemed, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(local_tx_id)
        .bind(comm.satoshis)
        .bind(&comm.payer_locking_script)
        .bind(&comm.key_offset)
        .bind(comm.is_redeemed)
        .bind(comm.created_at)
        .bind(comm.updated_at)
        .execute(&mut *conn)
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_offsets() -> Vec<SyncOffset> {
        vec![
            SyncOffset {
                name: entity_names::OUTPUT_BASKET.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::PROVEN_TX.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::PROVEN_TX_REQ.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::TX_LABEL.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::OUTPUT_TAG.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::TRANSACTION.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::OUTPUT.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::TX_LABEL_MAP.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::OUTPUT_TAG_MAP.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::CERTIFICATE.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::CERTIFICATE_FIELD.to_string(),
                offset: 0,
            },
            SyncOffset {
                name: entity_names::COMMISSION.to_string(),
                offset: 0,
            },
        ]
    }

    #[tokio::test]
    async fn test_get_sync_chunk_empty_user() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        let (_user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: make_test_offsets(),
        };

        let chunk = get_sync_chunk_internal(&storage, args).await.unwrap();

        assert_eq!(chunk.from_storage_identity_key, "a".repeat(66));
        assert_eq!(chunk.to_storage_identity_key, "c".repeat(66));
        assert_eq!(chunk.user_identity_key, identity_key);
        assert!(chunk.user.is_some()); // User is always included when no since

        // Default basket should be present
        assert!(chunk.output_baskets.as_ref().is_some_and(|b| !b.is_empty()));
    }

    #[tokio::test]
    async fn test_get_sync_chunk_no_offsets() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        let (_user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: vec![], // No offsets
        };

        let chunk = get_sync_chunk_internal(&storage, args).await.unwrap();

        // With no offsets, only user should be included
        assert!(chunk.user.is_some());
        assert!(chunk.output_baskets.is_none());
        assert!(chunk.transactions.is_none());
    }

    #[tokio::test]
    async fn test_get_sync_chunk_with_since_future() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        let (_user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        // Set since to future - should return empty
        let future_time = Utc::now() + chrono::Duration::hours(1);

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: Some(future_time),
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: make_test_offsets(),
        };

        let chunk = get_sync_chunk_internal(&storage, args).await.unwrap();

        // User should not be included since updated_at < since
        assert!(chunk.user.is_none());
        // Baskets should be empty since all created before since
        assert!(chunk.output_baskets.as_ref().is_none_or(|b| b.is_empty()));
    }

    #[tokio::test]
    async fn test_get_sync_chunk_max_items() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        let (_user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        // Very small max_items
        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1,
            offsets: make_test_offsets(),
        };

        let chunk = get_sync_chunk_internal(&storage, args).await.unwrap();

        // Should only return 1 item (the basket)
        let basket_count = chunk.output_baskets.as_ref().map_or(0, |b| b.len());
        assert!(basket_count <= 1);
    }

    #[tokio::test]
    async fn test_process_sync_chunk_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        // A chunk never creates its user: the store holds it already.
        storage.find_or_insert_user(&identity_key).await.unwrap();

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: vec![],
        };

        let chunk = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            ..Default::default()
        };

        let result = process_sync_chunk_internal(&storage, args, chunk, &mut Default::default())
            .await
            .unwrap();

        assert!(result.done); // Empty chunk = sync complete
        assert_eq!(result.inserts, 0); // The user already existed
        assert_eq!(result.updates, 0);
    }

    #[tokio::test]
    async fn test_process_sync_chunk_with_baskets() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        // A chunk never creates its user: the store holds it already.
        storage.find_or_insert_user(&identity_key).await.unwrap();
        let now = Utc::now();

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: vec![],
        };

        let chunk = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            output_baskets: Some(vec![TableOutputBasket {
                basket_id: 100, // Source ID
                user_id: 1,
                name: "custom_basket".to_string(),
                number_of_desired_utxos: 10,
                minimum_desired_utxo_value: 5000,
                created_at: now,
                updated_at: now,
            }]),
            ..Default::default()
        };

        let result = process_sync_chunk_internal(&storage, args, chunk, &mut Default::default())
            .await
            .unwrap();

        assert!(!result.done); // Not empty chunk
        assert_eq!(result.inserts, 1); // The basket (the user existed)
        assert_eq!(result.updates, 0);
    }

    #[tokio::test]
    async fn test_process_sync_chunk_upsert_existing() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        let (_user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let now = Utc::now();
        let later = now + chrono::Duration::seconds(10);

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: vec![],
        };

        // First sync - create basket
        let chunk1 = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            output_baskets: Some(vec![TableOutputBasket {
                basket_id: 100,
                user_id: 1,
                name: "custom_basket".to_string(),
                number_of_desired_utxos: 10,
                minimum_desired_utxo_value: 5000,
                created_at: now,
                updated_at: now,
            }]),
            ..Default::default()
        };

        let result1 =
            process_sync_chunk_internal(&storage, args.clone(), chunk1, &mut Default::default())
                .await
                .unwrap();
        assert_eq!(result1.inserts, 1); // basket only (user already exists)

        // Second sync - update basket with newer timestamp
        let chunk2 = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            output_baskets: Some(vec![TableOutputBasket {
                basket_id: 100,
                user_id: 1,
                name: "custom_basket".to_string(),
                number_of_desired_utxos: 20,       // Updated value
                minimum_desired_utxo_value: 10000, // Updated value
                created_at: now,
                updated_at: later, // Newer timestamp
            }]),
            ..Default::default()
        };

        let result2 = process_sync_chunk_internal(&storage, args, chunk2, &mut Default::default())
            .await
            .unwrap();
        assert_eq!(result2.inserts, 0);
        assert_eq!(result2.updates, 1); // basket updated
    }

    #[tokio::test]
    async fn test_process_sync_chunk_id_translation() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        // A chunk never creates its user: the store holds it already.
        storage.find_or_insert_user(&identity_key).await.unwrap();
        let now = Utc::now();

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: vec![],
        };

        // Chunk with transaction and output - tests ID translation
        let chunk = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            transactions: Some(vec![TableTransaction {
                transaction_id: 999, // Source ID
                user_id: 1,
                txid: Some("a".repeat(64)),
                status: TransactionStatus::Completed,
                reference: "test_ref".to_string(),
                description: "Test transaction".to_string(),
                satoshis: 10000,
                version: 1,
                lock_time: 0,
                raw_tx: Some(vec![1, 2, 3]),
                input_beef: None,
                is_outgoing: true,
                proof_txid: None,
                created_at: now,
                updated_at: now,
            }]),
            outputs: Some(vec![TableOutput {
                output_id: 888, // Source ID
                user_id: 1,
                transaction_id: 999, // References source transaction ID
                basket_id: None,
                txid: "a".repeat(64),
                vout: 0,
                satoshis: 5000,
                locking_script: Some(vec![0x76, 0xa9]),
                script_length: 25,
                script_offset: 0,
                output_type: "P2PKH".to_string(),
                provided_by: "you".to_string(),
                purpose: None,
                output_description: None,
                spent_by: None,
                sequence_number: None,
                spending_description: None,
                spendable: true,
                change: false,
                derivation_prefix: None,
                derivation_suffix: None,
                sender_identity_key: None,
                custom_instructions: None,
                created_at: now,
                updated_at: now,
            }]),
            ..Default::default()
        };

        let result = process_sync_chunk_internal(&storage, args, chunk, &mut Default::default())
            .await
            .unwrap();

        // Transaction + output = 2 inserts (the user existed)
        assert_eq!(result.inserts, 2);

        // Verify output was created with correct local transaction_id
        let auth = AuthId::with_user_id(&identity_key, 1);
        let outputs = storage
            .find_outputs(
                &auth,
                FindOutputsArgs {
                    txid: Some("a".repeat(64)),
                    vout: Some(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(outputs.len(), 1);
        // transaction_id should be 1 (local ID), not 999 (source ID)
        assert_eq!(outputs[0].transaction_id, 1);
    }

    #[tokio::test]
    async fn test_process_sync_chunk_output_resolves_parent_via_natural_key() {
        // Cross-session re-resolution: a parent transaction landed
        // locally during an earlier sync run, the current chunk's
        // `transactions` section does not re-deliver it, but the
        // chunk's `outputs` section references the same logical UTXO
        // with a remote (sender-local) FK that has no entry in the
        // current session's `transaction_id_map`.
        //
        // Pre-fix: `local_tx_id` resolved to `None`, the orchestrator
        // passed it through to `upsert_output`, and `upsert_output`
        // fell back to `output.transaction_id` — the sender's local
        // FK — and either wrote it into the row's `transaction_id`
        // column (silently corrupting the schema) or collided on
        // `UNIQUE(transaction_id, vout, user_id)` if a row already
        // existed at that FK.
        //
        // Post-fix: the orchestrator falls through to
        // `find_transaction_id_for_sync`, resolves the parent by
        // `(user_id, txid)`, and either passes the correct local FK
        // to `upsert_output` (which then SELECTs on the matching
        // UNIQUE key) or skips the output as orphan if the parent is
        // genuinely absent.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"a".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        // A chunk never creates its user: the store holds it already.
        storage.find_or_insert_user(&identity_key).await.unwrap();
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(10);
        let txid_hex = "a".repeat(64);

        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: vec![],
        };

        // First chunk: deliver the parent transaction together with its
        // output. After this, the local schema holds `transactions`
        // row with `transaction_id = 1` and `outputs` row with
        // `(user_id = 1, transaction_id = 1, vout = 0)`.
        let chunk1 = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            transactions: Some(vec![TableTransaction {
                transaction_id: 999, // sender-local
                user_id: 1,
                txid: Some(txid_hex.clone()),
                status: TransactionStatus::Completed,
                reference: "test_ref".to_string(),
                description: "first chunk parent".to_string(),
                satoshis: 10_000,
                version: 1,
                lock_time: 0,
                raw_tx: Some(vec![1, 2, 3]),
                input_beef: None,
                is_outgoing: true,
                proof_txid: None,
                created_at: now,
                updated_at: now,
            }]),
            outputs: Some(vec![TableOutput {
                output_id: 888,
                user_id: 1,
                transaction_id: 999, // references sender-local parent
                basket_id: None,
                txid: txid_hex.clone(),
                vout: 0,
                satoshis: 5_000,
                locking_script: Some(vec![0x76, 0xa9]),
                script_length: 25,
                script_offset: 0,
                output_type: "P2PKH".to_string(),
                provided_by: "you".to_string(),
                purpose: None,
                output_description: None,
                spent_by: None,
                sequence_number: None,
                spending_description: None,
                spendable: true,
                change: false,
                derivation_prefix: None,
                derivation_suffix: None,
                sender_identity_key: None,
                custom_instructions: None,
                created_at: now,
                updated_at: now,
            }]),
            ..Default::default()
        };
        let result1 =
            process_sync_chunk_internal(&storage, args.clone(), chunk1, &mut Default::default())
                .await
                .unwrap();
        assert!(
            result1.inserts >= 2,
            "first chunk must insert at least the parent transaction and the output"
        );

        // Capture the local transaction_id assigned to the parent so
        // the post-state assertion can compare against it.
        let auth = AuthId::with_user_id(&identity_key, 1);
        let outputs_after_chunk1 = storage
            .find_outputs(
                &auth,
                FindOutputsArgs {
                    txid: Some(txid_hex.clone()),
                    vout: Some(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(outputs_after_chunk1.len(), 1);
        let local_parent_id = outputs_after_chunk1[0].transaction_id;

        // Second chunk: re-deliver the same logical output, but the
        // sender now uses a different remote FK (its own autoincrement
        // counter has moved on) AND the chunk's `transactions` section
        // does not re-deliver the parent — the parent is already
        // local from the prior session. This chunk's
        // `transaction_id_map` therefore has no entry for the output's
        // `transaction_id`.
        let chunk2 = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            transactions: None, // parent already local; chunk does not re-deliver
            outputs: Some(vec![TableOutput {
                output_id: 888,
                user_id: 1,
                transaction_id: 12_345, // unrelated to local; not in map
                basket_id: None,
                txid: txid_hex.clone(),
                vout: 0,
                satoshis: 5_000,
                locking_script: Some(vec![0x76, 0xa9]),
                script_length: 25,
                script_offset: 0,
                output_type: "P2PKH".to_string(),
                provided_by: "you".to_string(),
                purpose: None,
                output_description: None,
                spent_by: None,
                sequence_number: None,
                spending_description: None,
                spendable: true,
                change: false,
                derivation_prefix: None,
                derivation_suffix: None,
                sender_identity_key: None,
                custom_instructions: None,
                created_at: now,
                updated_at: later, // newer to trigger UPDATE branch
            }]),
            ..Default::default()
        };

        // Pre-fix, this call would either error with the `rc=19`
        // UNIQUE constraint message or silently overwrite the row's
        // `transaction_id` column with the remote `12_345`. Post-fix,
        // it must succeed without disturbing the local FK.
        let result2 = process_sync_chunk_internal(&storage, args, chunk2, &mut Default::default())
            .await
            .expect("second chunk apply must not error on unmapped parent");

        assert!(
            result2.inserts == 0,
            "no new row must be inserted for the same logical output"
        );

        let outputs_after_chunk2 = storage
            .find_outputs(
                &auth,
                FindOutputsArgs {
                    txid: Some(txid_hex.clone()),
                    vout: Some(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            outputs_after_chunk2.len(),
            1,
            "no duplicate row must be created"
        );
        assert_eq!(
            outputs_after_chunk2[0].transaction_id, local_parent_id,
            "the row's transaction_id must remain the local FK; \
             pre-fix this column could be overwritten with the chunk's remote FK \
             (or trigger an `rc=19 UNIQUE constraint failed: outputs...` error \
             if a row already existed at the resulting key)"
        );
    }

    #[tokio::test]
    async fn test_sync_roundtrip() {
        // Create source storage with data
        let source = StorageSqlx::in_memory().await.unwrap();
        source
            .migrate("source-storage", &"a".repeat(66))
            .await
            .unwrap();
        source.make_available().await.unwrap();

        let identity_key = "b".repeat(66);
        let (user, _) = source.find_or_insert_user(&identity_key).await.unwrap();

        // Add a custom basket to source
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at)
            VALUES (?, 'payments', 5, 1000, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(now)
        .bind(now)
        .execute(source.writer())
        .await
        .unwrap();

        // Get sync chunk from source
        let get_args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: make_test_offsets(),
        };

        let chunk = get_sync_chunk_internal(&source, get_args).await.unwrap();

        // Verify chunk has data
        assert!(chunk.user.is_some());
        assert!(chunk.output_baskets.as_ref().is_some_and(|b| b.len() >= 2)); // default + payments

        // Create destination storage
        let dest = StorageSqlx::in_memory().await.unwrap();
        dest.migrate("dest-storage", &"c".repeat(66)).await.unwrap();
        dest.make_available().await.unwrap();
        // A chunk never creates its user: the store holds it already.
        dest.find_or_insert_user(&identity_key).await.unwrap();

        // Process chunk on destination
        let process_args = RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: identity_key.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: vec![],
        };

        let result =
            process_sync_chunk_internal(&dest, process_args, chunk, &mut Default::default())
                .await
                .unwrap();

        // Should have synced the user row and the baskets (the user existed)
        let total_changes = result.inserts + result.updates;
        assert!(
            total_changes >= 2,
            "Expected at least 2 changes (user row + baskets), got {} inserts + {} updates",
            result.inserts,
            result.updates
        );

        // Verify destination has the data
        let dest_auth = AuthId::with_user_id(&identity_key, 1);
        let dest_baskets = dest
            .find_output_baskets(&dest_auth, FindOutputBasketsArgs::default())
            .await
            .unwrap();

        assert!(
            dest_baskets.iter().any(|b| b.name == "payments"),
            "payments basket not found in destination"
        );
    }
}
