//! Sync chunk operations for multi-storage synchronization.
//!
//! This module implements `get_sync_chunk` and `process_sync_chunk` methods
//! that enable synchronization of wallet data between storage providers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
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
        if self.items_count >= self.max_items {
            0
        } else {
            self.max_items - self.items_count
        }
    }
}

/// Lookup for offsets by entity name
type OffsetsLookup = HashMap<String, u32>;

fn make_offsets_lookup(offsets: &[SyncOffset]) -> OffsetsLookup {
    offsets
        .iter()
        .map(|o| (o.name.clone(), o.offset))
        .collect()
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
            let baskets = fetch_baskets_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let proven_txs = fetch_proven_txs_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let reqs = fetch_proven_tx_reqs_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let labels = fetch_tx_labels_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let tags = fetch_output_tags_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let txs = fetch_transactions_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let outputs = fetch_outputs_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let maps = fetch_tx_label_maps_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let maps = fetch_output_tag_maps_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let certs = fetch_certificates_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let fields = fetch_certificate_fields_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
            let commissions = fetch_commissions_for_sync(storage, user_id, args.since, offset, state.remaining_items()).await?;
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
) -> Result<ProcessSyncChunkResult> {
    let mut result = ProcessSyncChunkResult {
        done: false,
        max_updated_at: None,
        updates: 0,
        inserts: 0,
        error: None,
    };

    // Verify identity keys match
    if chunk.user_identity_key != args.identity_key {
        return Err(Error::SyncError(format!(
            "Chunk user identity key {} does not match args identity key {}",
            chunk.user_identity_key, args.identity_key
        )));
    }

    // Find or create the user
    let (user, user_is_new) = storage.find_or_insert_user(&args.identity_key).await?;
    let user_id = user.user_id;

    if user_is_new {
        result.inserts += 1;
    }

    // Find or create sync state for tracking
    let auth = AuthId::with_user_id(&args.identity_key, user_id);
    let (mut _sync_state, _) = storage
        .find_or_insert_sync_state(&auth, &args.from_storage_identity_key, "sync")
        .await?;

    // Track ID mappings from source to destination
    let mut basket_id_map: HashMap<i64, i64> = HashMap::new();
    let mut label_id_map: HashMap<i64, i64> = HashMap::new();
    let mut tag_id_map: HashMap<i64, i64> = HashMap::new();
    let mut transaction_id_map: HashMap<i64, i64> = HashMap::new();
    let mut output_id_map: HashMap<i64, i64> = HashMap::new();
    let mut certificate_id_map: HashMap<i64, i64> = HashMap::new();

    // Check if chunk is empty (sync complete)
    let chunk_is_empty = chunk.output_baskets.as_ref().map_or(true, |v| v.is_empty())
        && chunk.proven_txs.as_ref().map_or(true, |v| v.is_empty())
        && chunk.proven_tx_reqs.as_ref().map_or(true, |v| v.is_empty())
        && chunk.transactions.as_ref().map_or(true, |v| v.is_empty())
        && chunk.outputs.as_ref().map_or(true, |v| v.is_empty())
        && chunk.tx_labels.as_ref().map_or(true, |v| v.is_empty())
        && chunk.tx_label_maps.as_ref().map_or(true, |v| v.is_empty())
        && chunk.output_tags.as_ref().map_or(true, |v| v.is_empty())
        && chunk.output_tag_maps.as_ref().map_or(true, |v| v.is_empty())
        && chunk.certificates.as_ref().map_or(true, |v| v.is_empty())
        && chunk.certificate_fields.as_ref().map_or(true, |v| v.is_empty())
        && chunk.commissions.as_ref().map_or(true, |v| v.is_empty());

    if chunk_is_empty {
        result.done = true;
        // Merge user if provided
        if let Some(ref chunk_user) = chunk.user {
            let merge_result = merge_user(storage, user_id, chunk_user).await?;
            if merge_result.updated {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, chunk_user.updated_at);
        }
        return Ok(result);
    }

    // Process user if included
    if let Some(ref chunk_user) = chunk.user {
        let merge_result = merge_user(storage, user_id, chunk_user).await?;
        if merge_result.updated {
            result.updates += 1;
        }
        update_max_updated_at(&mut result.max_updated_at, chunk_user.updated_at);
    }

    // Process output baskets
    if let Some(baskets) = &chunk.output_baskets {
        for basket in baskets {
            let upsert_result = upsert_basket(storage, user_id, basket).await?;
            basket_id_map.insert(basket.basket_id, upsert_result.local_id);
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, basket.updated_at);
        }
    }

    // Process proven tx reqs
    if let Some(reqs) = &chunk.proven_tx_reqs {
        for req in reqs {
            let upsert_result = upsert_proven_tx_req(storage, req).await?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, req.updated_at);
        }
    }

    // Process proven txs
    if let Some(proven_txs) = &chunk.proven_txs {
        for ptx in proven_txs {
            let upsert_result = upsert_proven_tx(storage, ptx).await?;
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, ptx.updated_at);
        }
    }

    // Process tx labels
    if let Some(labels) = &chunk.tx_labels {
        for label in labels {
            let upsert_result = upsert_tx_label(storage, user_id, label).await?;
            label_id_map.insert(label.label_id, upsert_result.local_id);
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
            let upsert_result = upsert_output_tag(storage, user_id, tag).await?;
            tag_id_map.insert(tag.tag_id, upsert_result.local_id);
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
            let upsert_result = upsert_transaction(storage, user_id, tx).await?;
            transaction_id_map.insert(tx.transaction_id, upsert_result.local_id);
            if upsert_result.is_new {
                result.inserts += 1;
            } else {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, tx.updated_at);
        }
    }

    // Process outputs (need transaction ID translation)
    if let Some(outputs) = &chunk.outputs {
        for output in outputs {
            let local_tx_id = transaction_id_map.get(&output.transaction_id).copied();
            let local_basket_id = output.basket_id.and_then(|bid| basket_id_map.get(&bid).copied());
            let upsert_result = upsert_output(storage, user_id, output, local_tx_id, local_basket_id).await?;
            output_id_map.insert(output.output_id, upsert_result.local_id);
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
                let upsert_result = upsert_tx_label_map(storage, map, tx_id, label_id).await?;
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
                let upsert_result = upsert_output_tag_map(storage, map, output_id, tag_id).await?;
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
            let upsert_result = upsert_certificate(storage, user_id, cert).await?;
            certificate_id_map.insert(cert.certificate_id, upsert_result.local_id);
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
                let upsert_result = upsert_certificate_field(storage, user_id, field, cert_id).await?;
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
                let upsert_result = upsert_commission(storage, user_id, comm, tx_id).await?;
                if upsert_result.is_new {
                    result.inserts += 1;
                } else {
                    result.updates += 1;
                }
                update_max_updated_at(&mut result.max_updated_at, comm.updated_at);
            }
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
    _user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableProvenTx>> {
    let mut sql = String::from(
        r#"
        SELECT proven_tx_id, txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx,
               created_at, updated_at
        FROM proven_txs
        WHERE 1=1
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql);
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
    _user_id: i64,
    since: Option<DateTime<Utc>>,
    offset: u32,
    limit: u32,
) -> Result<Vec<TableProvenTxReq>> {
    let mut sql = String::from(
        r#"
        SELECT proven_tx_req_id, proven_tx_id, txid, status, attempts, history, notified,
               raw_tx, input_beef, created_at, updated_at
        FROM proven_tx_reqs
        WHERE 1=1
        "#,
    );

    if since.is_some() {
        sql.push_str(" AND updated_at > ?");
    }

    sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");

    let mut query = sqlx::query(&sql);
    if let Some(s) = since {
        query = query.bind(s);
    }
    query = query.bind(limit as i32).bind(offset as i32);

    let rows = query.fetch_all(storage.pool()).await?;

    let reqs = rows
        .iter()
        .map(|row| {
            let status_str: String = row.get("status");
            let status = match status_str.as_str() {
                "pending" => ProvenTxReqStatus::Pending,
                "inprogress" | "in_progress" => ProvenTxReqStatus::InProgress,
                "completed" => ProvenTxReqStatus::Completed,
                "failed" => ProvenTxReqStatus::Failed,
                "notfound" | "not_found" => ProvenTxReqStatus::NotFound,
                _ => ProvenTxReqStatus::Pending,
            };
            let notified_val: i32 = row.get("notified");
            let notify_txid = if notified_val != 0 {
                Some(row.get::<String, _>("txid"))
            } else {
                None
            };

            TableProvenTxReq {
                proven_tx_req_id: row.get("proven_tx_req_id"),
                txid: row.get("txid"),
                status,
                attempts: row.get("attempts"),
                history: row.get("history"),
                notify_txid,
                proven_tx_id: row.get("proven_tx_id"),
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
        SELECT transaction_id, user_id, txid, status, reference, description, satoshis,
               version, lock_time, raw_tx, input_beef, is_outgoing, proven_tx_id,
               created_at, updated_at
        FROM transactions
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
                proof_txid: row.get("proven_tx_id"),
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
               locking_script, script_length, script_offset, type, spendable, change,
               derivation_prefix, derivation_suffix, sender_identity_key, custom_instructions,
               created_at, updated_at
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
    storage: &StorageSqlx,
    user_id: i64,
    chunk_user: &TableUser,
) -> Result<MergeResult> {
    // Get current user
    let current = storage
        .find_user_by_id(user_id)
        .await?
        .ok_or_else(|| Error::UserNotFound(user_id.to_string()))?;

    // Only update if chunk is newer
    if chunk_user.updated_at > current.updated_at {
        if let Some(ref active_storage) = chunk_user.active_storage {
            storage
                .update_user_active_storage(user_id, active_storage)
                .await?;
        }
        return Ok(MergeResult { updated: true });
    }

    Ok(MergeResult { updated: false })
}

async fn upsert_basket(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_proven_tx(storage: &StorageSqlx, ptx: &TableProvenTx) -> Result<UpsertResult> {
    // Check if exists by txid
    let existing = sqlx::query(
        r#"
        SELECT proven_tx_id, updated_at FROM proven_txs
        WHERE txid = ?
        "#,
    )
    .bind(&ptx.txid)
    .fetch_optional(storage.pool())
    .await?;

    if let Some(row) = existing {
        let local_id: i64 = row.get("proven_tx_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if ptx.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE proven_txs
                SET height = ?, idx = ?, block_hash = ?, merkle_root = ?, merkle_path = ?, raw_tx = ?, updated_at = ?
                WHERE proven_tx_id = ?
                "#,
            )
            .bind(ptx.height)
            .bind(ptx.index)
            .bind(&ptx.block_hash)
            .bind(&ptx.merkle_root)
            .bind(&ptx.merkle_path)
            .bind(&ptx.raw_tx)
            .bind(ptx.updated_at)
            .bind(local_id)
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_proven_tx_req(storage: &StorageSqlx, req: &TableProvenTxReq) -> Result<UpsertResult> {
    // Check if exists by txid
    let existing = sqlx::query(
        r#"
        SELECT proven_tx_req_id, updated_at FROM proven_tx_reqs
        WHERE txid = ?
        "#,
    )
    .bind(&req.txid)
    .fetch_optional(storage.pool())
    .await?;

    let status_str = format!("{:?}", req.status).to_lowercase();

    if let Some(row) = existing {
        let local_id: i64 = row.get("proven_tx_req_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if req.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE proven_tx_reqs
                SET status = ?, attempts = ?, history = ?, notified = ?, proven_tx_id = ?, updated_at = ?
                WHERE proven_tx_req_id = ?
                "#,
            )
            .bind(&status_str)
            .bind(req.attempts)
            .bind(&req.history)
            .bind(req.notify_txid.is_some() as i32)
            .bind(req.proven_tx_id)
            .bind(req.updated_at)
            .bind(local_id)
            .execute(storage.pool())
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
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, proven_tx_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&req.txid)
        .bind(&status_str)
        .bind(req.attempts)
        .bind(&req.history)
        .bind(req.notify_txid.is_some() as i32)
        .bind(req.proven_tx_id)
        .bind(req.created_at)
        .bind(req.updated_at)
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_tx_label(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_output_tag(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_transaction(
    storage: &StorageSqlx,
    user_id: i64,
    tx: &TableTransaction,
) -> Result<UpsertResult> {
    // Check if exists by reference (unique per user)
    let existing = sqlx::query(
        r#"
        SELECT transaction_id, updated_at FROM transactions
        WHERE user_id = ? AND reference = ?
        "#,
    )
    .bind(user_id)
    .bind(&tx.reference)
    .fetch_optional(storage.pool())
    .await?;

    let status_str = tx.status.as_str();

    if let Some(row) = existing {
        let local_id: i64 = row.get("transaction_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if tx.updated_at > local_updated {
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
            .bind(&tx.proof_txid)
            .bind(tx.updated_at)
            .bind(local_id)
            .execute(storage.pool())
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
        .bind(&tx.proof_txid)
        .bind(tx.created_at)
        .bind(tx.updated_at)
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_output(
    storage: &StorageSqlx,
    user_id: i64,
    output: &TableOutput,
    local_tx_id: Option<i64>,
    local_basket_id: Option<i64>,
) -> Result<UpsertResult> {
    // Check if exists by txid and vout
    let existing = sqlx::query(
        r#"
        SELECT output_id, updated_at FROM outputs
        WHERE user_id = ? AND txid = ? AND vout = ?
        "#,
    )
    .bind(user_id)
    .bind(&output.txid)
    .bind(output.vout)
    .fetch_optional(storage.pool())
    .await?;

    let tx_id = local_tx_id.unwrap_or(output.transaction_id);

    if let Some(row) = existing {
        let local_id: i64 = row.get("output_id");
        let local_updated: DateTime<Utc> = row.get("updated_at");

        // Update if chunk is newer
        if output.updated_at > local_updated {
            sqlx::query(
                r#"
                UPDATE outputs
                SET transaction_id = ?, basket_id = ?, satoshis = ?, locking_script = ?,
                    script_length = ?, script_offset = ?, type = ?, spendable = ?, change = ?,
                    derivation_prefix = ?, derivation_suffix = ?, sender_identity_key = ?,
                    custom_instructions = ?, updated_at = ?
                WHERE output_id = ?
                "#,
            )
            .bind(tx_id)
            .bind(local_basket_id)
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
            .bind(output.updated_at)
            .bind(local_id)
            .execute(storage.pool())
            .await?;
        }

        Ok(UpsertResult {
            local_id,
            is_new: false,
        })
    } else {
        // Insert new - provided_by and purpose are required columns
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
        .bind("you") // provided_by - default to "you" for synced outputs
        .bind("change") // purpose - default to "change" for synced outputs
        .bind(output.created_at)
        .bind(output.updated_at)
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_tx_label_map(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_output_tag_map(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_certificate(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_certificate_field(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
        .await?;

        Ok(UpsertResult {
            local_id: result.last_insert_rowid(),
            is_new: true,
        })
    }
}

async fn upsert_commission(
    storage: &StorageSqlx,
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
    .fetch_optional(storage.pool())
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
            .execute(storage.pool())
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
        .execute(storage.pool())
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
            SyncOffset { name: entity_names::OUTPUT_BASKET.to_string(), offset: 0 },
            SyncOffset { name: entity_names::PROVEN_TX.to_string(), offset: 0 },
            SyncOffset { name: entity_names::PROVEN_TX_REQ.to_string(), offset: 0 },
            SyncOffset { name: entity_names::TX_LABEL.to_string(), offset: 0 },
            SyncOffset { name: entity_names::OUTPUT_TAG.to_string(), offset: 0 },
            SyncOffset { name: entity_names::TRANSACTION.to_string(), offset: 0 },
            SyncOffset { name: entity_names::OUTPUT.to_string(), offset: 0 },
            SyncOffset { name: entity_names::TX_LABEL_MAP.to_string(), offset: 0 },
            SyncOffset { name: entity_names::OUTPUT_TAG_MAP.to_string(), offset: 0 },
            SyncOffset { name: entity_names::CERTIFICATE.to_string(), offset: 0 },
            SyncOffset { name: entity_names::CERTIFICATE_FIELD.to_string(), offset: 0 },
            SyncOffset { name: entity_names::COMMISSION.to_string(), offset: 0 },
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
        assert!(chunk.output_baskets.as_ref().map_or(false, |b| !b.is_empty()));
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
        assert!(chunk.output_baskets.as_ref().map_or(true, |b| b.is_empty()));
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

        let result = process_sync_chunk_internal(&storage, args, chunk).await.unwrap();

        assert!(result.done); // Empty chunk = sync complete
        assert_eq!(result.inserts, 1); // User was created
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
            output_baskets: Some(vec![
                TableOutputBasket {
                    basket_id: 100, // Source ID
                    user_id: 1,
                    name: "custom_basket".to_string(),
                    number_of_desired_utxos: 10,
                    minimum_desired_utxo_value: 5000,
                    created_at: now,
                    updated_at: now,
                },
            ]),
            ..Default::default()
        };

        let result = process_sync_chunk_internal(&storage, args, chunk).await.unwrap();

        assert!(!result.done); // Not empty chunk
        assert_eq!(result.inserts, 2); // User + basket
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
            output_baskets: Some(vec![
                TableOutputBasket {
                    basket_id: 100,
                    user_id: 1,
                    name: "custom_basket".to_string(),
                    number_of_desired_utxos: 10,
                    minimum_desired_utxo_value: 5000,
                    created_at: now,
                    updated_at: now,
                },
            ]),
            ..Default::default()
        };

        let result1 = process_sync_chunk_internal(&storage, args.clone(), chunk1).await.unwrap();
        assert_eq!(result1.inserts, 1); // basket only (user already exists)

        // Second sync - update basket with newer timestamp
        let chunk2 = SyncChunk {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            user_identity_key: identity_key.clone(),
            output_baskets: Some(vec![
                TableOutputBasket {
                    basket_id: 100,
                    user_id: 1,
                    name: "custom_basket".to_string(),
                    number_of_desired_utxos: 20, // Updated value
                    minimum_desired_utxo_value: 10000, // Updated value
                    created_at: now,
                    updated_at: later, // Newer timestamp
                },
            ]),
            ..Default::default()
        };

        let result2 = process_sync_chunk_internal(&storage, args, chunk2).await.unwrap();
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
            transactions: Some(vec![
                TableTransaction {
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
                },
            ]),
            outputs: Some(vec![
                TableOutput {
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
                    spendable: true,
                    change: false,
                    derivation_prefix: None,
                    derivation_suffix: None,
                    sender_identity_key: None,
                    custom_instructions: None,
                    created_at: now,
                    updated_at: now,
                },
            ]),
            ..Default::default()
        };

        let result = process_sync_chunk_internal(&storage, args, chunk).await.unwrap();

        // User + transaction + output = 3 inserts
        assert_eq!(result.inserts, 3);

        // Verify output was created with correct local transaction_id
        let auth = AuthId::with_user_id(&identity_key, 1);
        let outputs = storage.find_outputs(&auth, FindOutputsArgs {
            txid: Some("a".repeat(64)),
            vout: Some(0),
            ..Default::default()
        }).await.unwrap();

        assert_eq!(outputs.len(), 1);
        // transaction_id should be 1 (local ID), not 999 (source ID)
        assert_eq!(outputs[0].transaction_id, 1);
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
        .execute(source.pool())
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
        assert!(chunk.output_baskets.as_ref().map_or(false, |b| b.len() >= 2)); // default + payments

        // Create destination storage
        let dest = StorageSqlx::in_memory().await.unwrap();
        dest.migrate("dest-storage", &"c".repeat(66))
            .await
            .unwrap();
        dest.make_available().await.unwrap();

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

        let result = process_sync_chunk_internal(&dest, process_args, chunk).await.unwrap();

        // Should have synced user + baskets (inserts + updates)
        let total_changes = result.inserts + result.updates;
        assert!(total_changes >= 3, "Expected at least 3 changes (user + 2 baskets), got {} inserts + {} updates", result.inserts, result.updates);

        // Verify destination has the data
        let dest_auth = AuthId::with_user_id(&identity_key, 1);
        let dest_baskets = dest.find_output_baskets(&dest_auth, FindOutputBasketsArgs::default()).await.unwrap();

        assert!(dest_baskets.iter().any(|b| b.name == "payments"), "payments basket not found in destination");
    }
}
