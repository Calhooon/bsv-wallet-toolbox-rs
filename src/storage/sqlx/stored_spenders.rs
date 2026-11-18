//! Which transactions this store holds, not failed, spend an outpoint, read
//! from their bytes.
//!
//! A store a sync has written holds a coin the other store spent as not
//! spendable with no spender: the sync carries a transaction's bytes and
//! not an output's spender. The bytes still name what they spend: each
//! input carries its outpoint as the source txid's 32 bytes in internal
//! order, then the output index as four bytes little-endian. This module
//! finds the transactions whose bytes carry those 36 bytes and PARSES each
//! one, so a match anywhere else in a transaction (an output script, an
//! unlocking script) is never read as a spend.
//!
//! Where a transaction's bytes live on this line: `transactions.raw_tx`
//! (NULL for an action `createAction` wrote and `processAction` has not yet
//! signed, for a completed row a purge cleared, and for such a row a sync
//! brought from a store that held it so), and `proven_txs.raw_tx` (NOT NULL)
//! for a transaction linked to its proof. A transaction not failed whose
//! bytes are in neither table cannot be read: it is counted, and a caller
//! that would free a coin frees none while any such row is held.

use sqlx::{Row, SqliteConnection};

use crate::error::{Error, Result};

/// A transaction this store holds that spends the outpoint, read from its
/// bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSpender {
    pub transaction_id: i64,
    pub txid: Option<String>,
    pub status: String,
}

/// The answer for one outpoint: the transactions not failed whose bytes
/// spend it, and how many transactions not failed could not be read (no
/// bytes in either table, or bytes carrying the outpoint that do not parse).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StoredSpenders {
    pub spenders: Vec<StoredSpender>,
    pub unreadable: u64,
}

impl StoredSpenders {
    /// Nothing this store holds spends the outpoint, and nothing it holds
    /// is unread: the only answer on which a coin may be freed.
    pub fn none_held(&self) -> bool {
        self.spenders.is_empty() && self.unreadable == 0
    }
}

/// The outpoint as an input carries it: the txid's 32 bytes in internal
/// (reversed display) order, then the output index little-endian.
pub(crate) fn outpoint_bytes(txid: &str, vout: u32) -> Result<[u8; 36]> {
    let mut id = hex::decode(txid)
        .map_err(|e| Error::ValidationError(format!("txid {txid} is not hex: {e}")))?;
    if id.len() != 32 {
        return Err(Error::ValidationError(format!(
            "txid {txid} is not 32 bytes"
        )));
    }
    id.reverse();
    let mut out = [0u8; 36];
    out[..32].copy_from_slice(&id);
    out[32..].copy_from_slice(&vout.to_le_bytes());
    Ok(out)
}

/// Whether `raw` parses and one of its inputs spends `txid`.`vout`: `None`
/// when it does not parse.
fn bytes_spend(raw: &[u8], txid: &str, vout: u32) -> Option<bool> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    Some(tx.inputs.iter().any(|i| {
        i.source_output_index == vout
            && i.source_txid
                .as_deref()
                .is_some_and(|s| s.eq_ignore_ascii_case(txid))
    }))
}

/// Which transactions of this store that are not failed spend `txid`.`vout`,
/// by their bytes (`transactions.raw_tx`, else the linked `proven_txs.raw_tx`),
/// and how many transactions not failed cannot be read. Two reads on `conn`:
/// a byte search (`instr`, a scan of the bytes of every transaction not
/// failed, no index) whose candidates are each parsed, and a count of the
/// rows with no bytes in either table.
pub(crate) async fn stored_spenders_of(
    conn: &mut SqliteConnection,
    txid: &str,
    vout: u32,
) -> Result<StoredSpenders> {
    let needle = outpoint_bytes(txid, vout)?.to_vec();
    let candidates = sqlx::query(
        "SELECT t.transaction_id, t.txid, t.status, t.raw_tx AS t_raw, p.raw_tx AS p_raw \
         FROM transactions t LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id \
         WHERE t.status != 'failed' \
           AND (instr(t.raw_tx, ?) > 0 OR instr(p.raw_tx, ?) > 0) \
         ORDER BY t.transaction_id",
    )
    .bind(&needle)
    .bind(&needle)
    .fetch_all(&mut *conn)
    .await?;

    let mut answer = StoredSpenders::default();
    for row in &candidates {
        let readings: Vec<Option<bool>> = ["t_raw", "p_raw"]
            .iter()
            .filter_map(|c| row.get::<Option<Vec<u8>>, _>(*c))
            .filter(|raw| raw.windows(36).any(|w| w == needle.as_slice()))
            .map(|raw| bytes_spend(&raw, txid, vout))
            .collect();
        if readings.contains(&Some(true)) {
            answer.spenders.push(StoredSpender {
                transaction_id: row.get("transaction_id"),
                txid: row.get("txid"),
                status: row.get("status"),
            });
        } else if readings.contains(&None) {
            // Carries the outpoint's bytes and does not parse: it cannot be
            // ruled out.
            answer.unreadable += 1;
        }
    }

    let bare: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM transactions t \
         LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id \
         WHERE t.status != 'failed' \
           AND (t.raw_tx IS NULL OR length(t.raw_tx) = 0) \
           AND (p.raw_tx IS NULL OR length(p.raw_tx) = 0)",
    )
    .fetch_one(&mut *conn)
    .await?
    .get("n");
    answer.unreadable += bare as u64;
    Ok(answer)
}
