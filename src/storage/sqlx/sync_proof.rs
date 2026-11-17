//! The proof rows of a sync chunk, decided before the chunk's write
//! transaction: the reference's processSyncChunk preflight (ts-stack
//! 4d0ea63c8, `storage/StorageProvider.ts` 1392-1410) and
//! `validateSyncProof` (`storage/methods/validateSyncProof.ts` 93-142).
//!
//! Every identifier of every incoming proof row is checked and lowercased.
//! A block hash may be empty: this line's own stores hold proof rows without
//! one (a proof stored with no header service to read it from), so an empty
//! block hash is admitted and left empty, and two rows that differ only in
//! that one of their block hashes is empty are the same proof.
//! A proof row whose txid this store does not hold is taken as given (a new
//! proof row keeps the trust a direct sync gives it). A proof row that
//! differs from the one this store holds replaces it only once it is
//! validated: its bytes hash to its txid, its path is at its height with the
//! transaction at its index, the path's root is its root, the tracker holds
//! that root at that height, and the header at that height hashes to its
//! block hash and carries that root. Every tracker and header read happens
//! here, before the writer is taken (the write lock rule: never await a
//! service or a tracker while holding the writer).

use std::collections::{HashMap, HashSet};

use bsv_rs::primitives::{sha256d, to_hex};
use bsv_rs::transaction::{MerklePath, Transaction};
use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::error::{Error, Result};
use crate::storage::entities::TableProvenTx;
use crate::storage::traits::WalletStorageReader;

use super::StorageSqlx;

/// What the preflight decided for the chunk's proof rows, by txid.
#[derive(Default)]
pub(super) struct ProofDecisions {
    /// No row of this txid was held: insert only.
    pub insert_only: HashSet<String>,
    /// A held row differs and the incoming row was validated: replace.
    pub replacement_authorized: HashMap<String, Authorized>,
}

/// A validated replacement: the block hash of the active header its
/// validation read, and the held row it was validated against, which the
/// write transaction must find unchanged.
pub(super) struct Authorized {
    pub block_hash: String,
    pub held: HeldProof,
}

fn refused(txid: &str, why: &str) -> Error {
    Error::SyncError(format!("sync chunk proof {txid} is refused: {why}"))
}

fn is_hex32(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The reference's `canonicalizeSyncProofIdentifiers`, but for an empty
/// block hash, which this line's own stores hold and which is left empty.
pub(super) fn canonicalize(proof: &mut TableProvenTx) -> Result<()> {
    if !is_hex32(&proof.txid) {
        return Err(refused(&proof.txid, "txid must be 32-byte hexadecimal"));
    }
    if !proof.block_hash.is_empty() && !is_hex32(&proof.block_hash) {
        return Err(refused(
            &proof.txid,
            "block hash must be 32-byte hexadecimal",
        ));
    }
    if !is_hex32(&proof.merkle_root) {
        return Err(refused(
            &proof.txid,
            "Merkle root must be 32-byte hexadecimal",
        ));
    }
    proof.txid = proof.txid.to_lowercase();
    proof.block_hash = proof.block_hash.to_lowercase();
    proof.merkle_root = proof.merkle_root.to_lowercase();
    Ok(())
}

/// The proof row this store holds for a txid: its id and its authority
/// fields.
pub(super) struct HeldProof {
    pub proven_tx_id: i64,
    txid: String,
    height: i64,
    index: i64,
    merkle_path: Vec<u8>,
    raw_tx: Vec<u8>,
    block_hash: String,
    merkle_root: String,
    pub updated_at: DateTime<Utc>,
}

/// The held row is the row seen before: the same id and the same authority
/// fields, compared exactly.
pub(super) fn unchanged(now: &HeldProof, before: &HeldProof) -> bool {
    now.proven_tx_id == before.proven_tx_id
        && now.txid == before.txid
        && now.height == before.height
        && now.index == before.index
        && now.merkle_path == before.merkle_path
        && now.raw_tx == before.raw_tx
        && now.block_hash == before.block_hash
        && now.merkle_root == before.merkle_root
}

/// The held proof row of `txid` (proof rows are global by txid, as the
/// reference's EntityProvenTx.mergeFind finds them).
pub(super) async fn held_proof<'e, E>(executor: E, txid: &str) -> Result<Option<HeldProof>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        r#"
        SELECT proven_tx_id, txid, height, idx, merkle_path, raw_tx, block_hash, merkle_root,
               updated_at
        FROM proven_txs WHERE txid = ?
        "#,
    )
    .bind(txid)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|row| HeldProof {
        proven_tx_id: row.get("proven_tx_id"),
        txid: row.get("txid"),
        height: row.get("height"),
        index: row.get("idx"),
        merkle_path: row.get("merkle_path"),
        raw_tx: row.get("raw_tx"),
        block_hash: row.get("block_hash"),
        merkle_root: row.get("merkle_root"),
        updated_at: row.get("updated_at"),
    }))
}

/// The reference's `sameSyncProof`: the proof's authority fields, not its
/// ids or timestamps. A block hash that is empty on either side matches the
/// other's.
pub(super) fn same_proof(held: &HeldProof, p: &TableProvenTx) -> bool {
    held.txid.eq_ignore_ascii_case(&p.txid)
        && held.height == p.height
        && held.index == p.index
        && held.merkle_path == p.merkle_path
        && held.raw_tx == p.raw_tx
        && (held.block_hash.is_empty()
            || p.block_hash.is_empty()
            || held.block_hash.eq_ignore_ascii_case(&p.block_hash))
        && held.merkle_root.eq_ignore_ascii_case(&p.merkle_root)
}

fn hash_hex(bytes: &[u8]) -> String {
    let mut h = sha256d(bytes);
    h.reverse();
    to_hex(&h)
}

/// The reference's `validateSyncProof` for a row that replaces a held one.
/// Answers the block hash of the active header at the row's height: the
/// row's own when it carries one (it must match), else the header's.
async fn validate(storage: &StorageSqlx, p: &TableProvenTx) -> Result<String> {
    let txid = p.txid.as_str();
    if p.height < 0 || p.height > u32::MAX as i64 {
        return Err(refused(txid, "height must be a non-negative integer"));
    }
    if p.index < 0 {
        return Err(refused(txid, "index must be a non-negative integer"));
    }
    if p.raw_tx.is_empty() {
        return Err(refused(txid, "raw transaction is required"));
    }
    if p.merkle_path.is_empty() {
        return Err(refused(txid, "Merkle path is required"));
    }
    let unreadable = || {
        refused(
            txid,
            "transaction, Merkle path, or active header could not be validated",
        )
    };
    let parsed = Transaction::from_binary(&p.raw_tx).map_err(|_| unreadable())?;
    let raw_txid = hash_hex(&p.raw_tx);
    if parsed.id() != raw_txid || raw_txid != txid {
        return Err(refused(txid, "raw transaction hash does not match txid"));
    }
    let path = MerklePath::from_binary(&p.merkle_path).map_err(|_| unreadable())?;
    if path.block_height as i64 != p.height {
        return Err(refused(
            txid,
            "Merkle path height does not match the record",
        ));
    }
    let leaf = path.path.first().and_then(|level| {
        level
            .iter()
            .find(|l| l.txid && l.hash.as_deref() == Some(txid))
    });
    if leaf.map(|l| l.offset as i64) != Some(p.index) {
        return Err(refused(
            txid,
            "Merkle path does not contain the transaction at the recorded index",
        ));
    }
    let root = path.compute_root(Some(txid)).map_err(|_| unreadable())?;
    if root != p.merkle_root {
        return Err(refused(
            txid,
            "computed Merkle root does not match the record",
        ));
    }
    let services = storage.get_services().map_err(|_| unreadable())?;
    let tracker = services
        .get_chain_tracker()
        .await
        .map_err(|_| unreadable())?;
    if tracker
        .is_valid_root_for_height(&root, p.height as u32)
        .await
        .ok()
        != Some(true)
    {
        return Err(refused(
            txid,
            "Merkle root is not active at the recorded height",
        ));
    }
    let header = services
        .get_header_for_height(p.height as u32)
        .await
        .map_err(|_| unreadable())?;
    if header.len() != 80 {
        return Err(refused(txid, "active block header must be 80 bytes"));
    }
    let mut header_root = header[36..68].to_vec();
    header_root.reverse();
    let header_hash = hash_hex(&header);
    if (!p.block_hash.is_empty() && header_hash != p.block_hash) || to_hex(&header_root) != root {
        return Err(refused(
            txid,
            "block metadata does not match the active header",
        ));
    }
    Ok(header_hash)
}

/// The preflight over the chunk's proof rows, on the reader pool and the
/// network, before any write: canonicalize every row, then validate every
/// row that would replace a held row that differs.
pub(super) async fn preflight(
    storage: &StorageSqlx,
    proofs: &mut [TableProvenTx],
) -> Result<ProofDecisions> {
    let mut decisions = ProofDecisions::default();
    for p in proofs.iter_mut() {
        canonicalize(p)?;
    }
    for p in proofs.iter() {
        match held_proof(storage.pool(), &p.txid).await? {
            None => {
                decisions.insert_only.insert(p.txid.clone());
            }
            Some(held) => {
                if !same_proof(&held, p) {
                    let block_hash = validate(storage, p).await?;
                    decisions
                        .replacement_authorized
                        .insert(p.txid.clone(), Authorized { block_hash, held });
                }
            }
        }
    }
    Ok(decisions)
}
