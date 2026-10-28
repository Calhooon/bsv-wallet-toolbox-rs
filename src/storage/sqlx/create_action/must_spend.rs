//! The offering call's must-spend list (#353, bsv-wallet-cli #313): coins
//! the action spends ahead of the pick. Each is a wallet-managed change coin
//! inside the offer, the user's and unspent, under the same filters as the
//! pick (#295's managed-change clause, R1 (d)'s received rule), or the call
//! refuses it by name. A named coin is allocated as a picked coin is, so the
//! signer sees one path: storage-provided change, the wallet's derivation.

use super::allocation::{AllocatedChangeInput, ChangePick, MANAGED_CHANGE_CLAUSE};
use crate::error::{Error, Result, StoreBusyReason};
use chrono::Utc;
use sqlx::sqlite::SqliteConnection;
use sqlx::Row;

fn refusal(index: usize, coin: impl std::fmt::Display, why: impl std::fmt::Display) -> Error {
    Error::ValidationError(format!("mustSpend[{index}]: output {coin} {why}"))
}

/// Before anything is read: every named coin inside the offer (with one),
/// and none named twice.
pub(super) fn check_listed(offer: Option<&[i64]>, must_spend: &[i64]) -> Result<()> {
    for (index, id) in must_spend.iter().enumerate() {
        if offer.is_some_and(|offer| !offer.contains(id)) {
            return Err(refusal(index, id, "is outside the offer"));
        }
        if must_spend[..index].contains(id) {
            return Err(refusal(index, id, "is named twice"));
        }
    }
    Ok(())
}

/// Takes the named coin `must_spend[index]` as `pick` says: a dry run
/// reserves it, an allocation marks it spent by `transaction_id`, after
/// `read_named_coin`'s checks.
#[allow(clippy::too_many_arguments)]
pub(super) async fn allocate_named_change_input(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    require_spendable: bool,
    pick: &mut ChangePick<'_>,
    index: usize,
    output_id: i64,
) -> Result<AllocatedChangeInput> {
    let named = NamedCoin {
        index,
        output_id,
        user_id,
        basket_id,
        require_spendable,
        allow_received: pick.allows_received(),
    };
    let input = named.read(conn).await?;
    if let ChangePick::DryRun { reserved, .. } = pick {
        // Another call of this process holds it: transient, as the pick's
        // own coins held by calls in flight.
        if !reserved.try_take(output_id) {
            return Err(Error::StoreBusy {
                reason: StoreBusyReason::InFlight,
            });
        }
    } else {
        let marked = sqlx::query(
            "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? \
             WHERE output_id = ? AND spent_by IS NULL",
        )
        .bind(transaction_id)
        .bind(Utc::now())
        .bind(output_id)
        .execute(&mut *conn)
        .await?;
        if marked.rows_affected() == 0 {
            let outpoint = format!("{}.{}", input.txid, input.vout);
            return Err(refusal(index, outpoint, "is spent or reserved"));
        }
    }
    Ok(input)
}

/// One named coin and the rules it is read under.
struct NamedCoin {
    index: usize,
    output_id: i64,
    user_id: i64,
    basket_id: i64,
    require_spendable: bool,
    allow_received: bool,
}

impl NamedCoin {
    /// The coin's row, its parent's status and whether it is managed change
    /// of the default basket (#295's clause).
    fn sql() -> String {
        format!(
            r"
            SELECT o.user_id, o.satoshis, o.txid, o.vout, o.locking_script,
                   o.derivation_prefix, o.derivation_suffix, o.sender_identity_key,
                   o.spendable, o.spent_by, t.status AS tx_status,
                   COALESCE(o.purpose, '') AS purpose,
                   COALESCE((o.change = 1 AND o.basket_id = ? {MANAGED_CHANGE_CLAUSE}), 0) AS managed
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            WHERE o.output_id = ?
            "
        )
    }

    /// Reads the coin, refusing by name one that is not this user's, not
    /// wallet-managed change of the default basket, spent or reserved, a
    /// received coin whose transaction is not `completed` when the action
    /// does not name the rule, or one whose transaction is neither
    /// `completed` nor `unproven` (the pick's own set).
    async fn read(&self, conn: &mut SqliteConnection) -> Result<AllocatedChangeInput> {
        let row = sqlx::query(&Self::sql())
            .bind(self.basket_id)
            .bind(self.output_id)
            .fetch_optional(&mut *conn)
            .await?;
        let index = self.index;
        let Some(row) = row.filter(|row| row.get::<i64, _>("user_id") == self.user_id) else {
            return Err(refusal(index, self.output_id, "is not this user's"));
        };
        let txid: String = row.get("txid");
        let vout: i32 = row.get("vout");
        let outpoint = format!("{txid}.{vout}");
        if !row.get::<bool, _>("managed") {
            return Err(refusal(index, outpoint, "is not wallet-managed change"));
        }
        let spent_by: Option<i64> = row.get("spent_by");
        if spent_by.is_some() || (self.require_spendable && !row.get::<bool, _>("spendable")) {
            return Err(refusal(index, outpoint, "is spent or reserved"));
        }
        let status: String = row.get("tx_status");
        let purpose: String = row.get("purpose");
        // R1 (d): the list names a coin; it does not bypass the received rule.
        if purpose == "receive" && status != "completed" && !self.allow_received {
            return Err(refusal(
                index,
                outpoint,
                format!(
                    "is a received coin whose transaction is {status}, not completed; \
                     the action does not name underReceivedRule"
                ),
            ));
        }
        if status != "completed" && status != "unproven" {
            return Err(refusal(
                index,
                outpoint,
                format!("is not spendable: its transaction is {status}"),
            ));
        }
        let satoshis: i64 = row.get("satoshis");
        let locking_script: Option<Vec<u8>> = row.get("locking_script");
        Ok(AllocatedChangeInput {
            output_id: self.output_id,
            satoshis: satoshis.cast_unsigned(),
            txid,
            vout,
            locking_script: locking_script.unwrap_or_default(),
            derivation_prefix: row.get("derivation_prefix"),
            derivation_suffix: row.get("derivation_suffix"),
            sender_identity_key: row.get("sender_identity_key"),
        })
    }
}
