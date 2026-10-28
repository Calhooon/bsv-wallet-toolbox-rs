//! Stored evidence for createAction's double-spend review (#389).

use super::beef_graph::merge_beef_linked;
use super::beef_storage::parse_input_txids;
use super::StorageSqlx;
use crate::error::Result;
use crate::storage::sqlx::storage_sqlx::RAW_TX_REQ_STATUSES;
use bsv_rs::transaction::{Beef, MerklePath, Transaction};
use bsv_rs::wallet::{CreateActionArgs, ReviewActionsRefusal};
use sqlx::{Row, SqliteConnection};
use std::collections::{hash_map::Entry, HashMap, HashSet};

/// Review BEEFs read before the door acquires its writer. Assembly faults
/// are held until the locked input check actually names that competitor.
pub(super) struct PreparedReviews(HashMap<String, Result<Vec<u8>>>);

impl PreparedReviews {
    /// Read only the caller's named inputs and their stored competing spends.
    pub(super) async fn read(
        storage: &StorageSqlx,
        user_id: i64,
        args: &CreateActionArgs,
    ) -> Result<Self> {
        let mut prepared = HashMap::new();
        let Some(inputs) = args.inputs.as_deref().filter(|inputs| !inputs.is_empty()) else {
            return Ok(Self(prepared));
        };
        let mut conn = storage.pool().acquire().await?;
        for input in inputs {
            let competing: Option<String> = sqlx::query_scalar(
                "SELECT t.txid FROM outputs o JOIN transactions t ON t.transaction_id = o.spent_by
                 WHERE o.user_id = ? AND o.txid = ? AND o.vout = ? AND t.txid <> ''",
            )
            .bind(user_id)
            .bind(hex::encode(input.outpoint.txid))
            .bind(input.outpoint.vout)
            .fetch_optional(&mut *conn)
            .await?;
            if let Some(txid) = competing {
                if let Entry::Vacant(entry) = prepared.entry(txid.clone()) {
                    let beef = build_competing_beef(&mut conn, &txid).await;
                    entry.insert(beef);
                }
            }
        }
        Ok(Self(prepared))
    }

    /// Attach only evidence for the competitor re-read under the write lock.
    /// A changed competitor needs another pre-check after rollback.
    pub(super) fn attach(&mut self, refusal: &mut ReviewActionsRefusal) -> Result<bool> {
        let [review] = refusal.review_action_results.as_mut_slice() else {
            return Ok(true);
        };
        let Some([txid]) = review.competing_txs.as_deref() else {
            return Ok(true);
        };
        let Some(beef) = self.0.remove(&hex::encode(txid)) else {
            return Ok(false);
        };
        review.competing_beef = Some(beef?);
        Ok(true)
    }
}

// createAction.ts:528-539 calls getBeefForTransaction after the input lock
// ends. StorageProvider.ts:1288-1338 merges raw bytes and a proof, or the
// stored inputBEEF and each missing ancestor. This diagnostic read needs no
// tracker verdict and writes nothing. Missing stored data answers a BEEF
// containing the competing txid alone: the reference's INVALID_PARAMETER
// fallback, reached there only after services also miss. Create asks no
// provider, so a competitor neither store holds answers txid-only at once.
async fn build_competing_beef(conn: &mut SqliteConnection, root: &str) -> Result<Vec<u8>> {
    let mut beef = Beef::new();
    let mut pending = vec![root.to_owned()];
    let mut visited = HashSet::new();
    while let Some(txid) = pending.pop() {
        if !visited.insert(txid.clone()) {
            continue;
        }
        if beef.find_txid(&txid).is_none_or(|tx| tx.is_txid_only()) {
            let Some(fragment) = stored_fragment(conn, &txid).await? else {
                return Ok(txid_only(root));
            };
            merge_beef_linked(&mut beef, &fragment);
        }
        if beef.find_txid(&txid).is_none() {
            return Ok(txid_only(root));
        }
        let raw = beef.find_txid(&txid).and_then(|tx| {
            if tx.bump_index().is_none() {
                tx.raw_tx().map(<[u8]>::to_vec)
            } else {
                None
            }
        });
        if let Some(raw) = raw {
            pending.extend(parse_input_txids(&raw)?);
        }
    }
    beef.sort_txs();
    if !beef.verify_valid(false).valid {
        return Ok(txid_only(root));
    }
    Ok(beef.to_binary())
}

fn txid_only(txid: &str) -> Vec<u8> {
    let mut beef = Beef::new();
    beef.merge_txid_only(txid.to_owned());
    beef.to_binary()
}

async fn stored_fragment(conn: &mut SqliteConnection, txid: &str) -> Result<Option<Beef>> {
    // The sources of getProvenOrRawTx (StorageKnex.ts:189-210), as the port's
    // get_proven_or_raw_tx reads them: proven_txs, else a request in the six
    // served statuses. transactions.raw_tx is not read (#351).
    let row = sqlx::query(&format!(
        "SELECT raw_tx, input_beef, merkle_path FROM (
           SELECT raw_tx, NULL AS input_beef, merkle_path, 0 AS priority
             FROM proven_txs WHERE txid = ?
           UNION ALL
           SELECT raw_tx, input_beef, NULL, 1 FROM proven_tx_reqs WHERE txid = ?
             AND status IN ({RAW_TX_REQ_STATUSES})
         ) WHERE raw_tx IS NOT NULL ORDER BY priority LIMIT 1"
    ))
    .bind(txid)
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let raw: Vec<u8> = row.get("raw_tx");
    Transaction::from_binary(&raw)?;
    let proof: Option<Vec<u8>> = row.get("merkle_path");
    let ancestry: Option<Vec<u8>> = row.get("input_beef");
    let mut beef = Beef::new();
    if let Some(proof) = proof {
        let index = beef.merge_bump(MerklePath::from_binary(&proof)?);
        beef.merge_raw_tx(raw, Some(index));
    } else {
        beef.merge_raw_tx(raw, None);
        if let Some(ancestry) = ancestry {
            merge_beef_linked(&mut beef, &Beef::from_binary(&ancestry)?);
        }
    }
    Ok(Some(beef))
}
