use super::beef::{build_input_beef, change_coin_gap};
use super::change::plan_change;
use super::inputs::validate_and_extend_inputs;
use super::outputs::validate_and_extend_outputs;
use super::tracker::RecordingTracker;
use super::Door;
use crate::error::{Error, Result, StoreBusyReason};
use bsv_rs::transaction::ChainTracker;
use chrono::Utc;
use sqlx::sqlite::SqliteConnection;
use sqlx::Row;
use std::collections::HashSet;

// #295: the reference's managed-change filters, its WHERE at ts-stack edf6e03
// `StorageKnex.allocateChangeInput` 2241-2250 (`o.type`, `o.change`,
// `o.providedBy`, `o.purpose` from `managedChangeOutputFields`, then
// `whereNotNull`/`whereNot ''` on `o.derivationPrefix` and
// `o.derivationSuffix`). `change = 1` is each query's own line. The port
// spells a received wallet payment's purpose `receive` where the reference
// spells it `change` (#169); R1 (d) below still holds an unproven one.
pub(in crate::storage::sqlx) const MANAGED_CHANGE_CLAUSE: &str = "AND o.type = 'P2PKH' \
     AND o.provided_by = 'storage' \
     AND o.purpose IN ('change', 'receive') \
     AND COALESCE(o.derivation_prefix, '') <> '' \
     AND COALESCE(o.derivation_suffix, '') <> ''";

// R1 (d): a received unproven coin needs this action's recorded rule. Its
// one `?` binds the rule; the public allocator, whose signature names no
// rule, binds it off (#304).
pub(in crate::storage::sqlx) const RECEIVED_COIN_CLAUSE: &str =
    "AND (t.status = 'completed' OR COALESCE(o.purpose, '') <> 'receive' OR ?)";

/// An allocated change input.
#[derive(Debug, Clone)]
pub(in crate::storage::sqlx) struct AllocatedChangeInput {
    #[allow(dead_code)]
    pub(super) output_id: i64,
    pub(super) satoshis: u64,
    pub(super) txid: String,
    pub(super) vout: i32,
    pub(super) locking_script: Vec<u8>,
    pub(super) derivation_prefix: Option<String>,
    pub(super) derivation_suffix: Option<String>,
    pub(super) sender_identity_key: Option<String>,
}

/// Counts available change inputs with the received rule off.
#[cfg(test)]
pub(super) async fn count_change_inputs(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket_id: i64,
    require_spendable: bool,
) -> Result<usize> {
    count_change_inputs_under_rule(conn, user_id, basket_id, require_spendable, false).await
}

/// Counts change inputs eligible under this action's received rule.
pub(super) async fn count_change_inputs_under_rule(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket_id: i64,
    require_spendable: bool,
    allow_received: bool,
) -> Result<usize> {
    let spendable_clause = if require_spendable {
        "AND o.spendable = 1"
    } else {
        ""
    };

    let sql = format!(
        r#"
        SELECT COUNT(*) as count
        FROM outputs o
        JOIN transactions t ON o.transaction_id = t.transaction_id
        WHERE o.user_id = ?
          AND o.basket_id = ?
          AND o.change = 1
          AND t.status IN ('completed', 'unproven')
          {MANAGED_CHANGE_CLAUSE}
          {RECEIVED_COIN_CLAUSE}
          {}
        "#,
        spendable_clause
    );

    let row = sqlx::query(&sql)
        .bind(user_id)
        .bind(basket_id)
        .bind(allow_received)
        .fetch_one(&mut *conn)
        .await?;

    let count: i64 = row.get("count");
    Ok(count as usize)
}

/// How `generate_change` takes a change coin (bsv-wallet-cli #44, the door
/// in one step).
pub(in crate::storage::sqlx) enum ChangePick<'a> {
    /// Mark each coin picked as allocated to the transaction, from every
    /// free coin (the tests' allocation).
    #[cfg(any(test, feature = "bench-support"))]
    Allocate,
    /// Before the write lock, on a reader: pick exactly as the allocation
    /// would and mark nothing, never a coin another call of this process has
    /// picked and not yet returned from; the picks are reserved in
    /// `reserved` (a coin released is let go), whose coins are the set the
    /// pre-check reads.
    /// `skip` are coins the pre-check found unspendable (their BEEF cannot
    /// be built) and never picks again. With an `offer` (the host's), only
    /// those coins are picked. `must_spend` (#353) are taken ahead of the
    /// pick.
    DryRun {
        reserved: &'a mut PickReservation,
        skip: &'a [i64],
        offer: Option<&'a [i64]>,
        allow_received: bool,
        must_spend: &'a [i64],
    },
    /// Under the write lock: allocate, but only from the coins the pre-check
    /// read. A coin taken meanwhile is skipped as any allocated coin is.
    Within {
        allowed: &'a [i64],
        allow_received: bool,
        must_spend: &'a [i64],
    },
}

impl<'a> ChangePick<'a> {
    /// The coins the action must spend, ahead of the pick (#353).
    pub(super) fn must_spend(&self) -> &'a [i64] {
        match self {
            #[cfg(any(test, feature = "bench-support"))]
            Self::Allocate => &[],
            Self::DryRun { must_spend, .. } | Self::Within { must_spend, .. } => must_spend,
        }
    }

    pub(super) fn allows_received(&self) -> bool {
        match self {
            #[cfg(any(test, feature = "bench-support"))]
            Self::Allocate => false,
            Self::DryRun { allow_received, .. } | Self::Within { allow_received, .. } => {
                *allow_received
            }
        }
    }
}

/// The change coins `createAction` pre-checks in this process have picked
/// and whose calls have not yet returned (review 2 F1 of bsv-wallet-cli #44).
/// The dry run sees only committed state, so without this two concurrent
/// calls pick the same best coin, one loses it under the lock, and a funded
/// wallet reads as empty. Another process's picks are not here: for those
/// the bounded re-check stands, and its end is `StoreBusy`.
#[derive(Default)]
pub(crate) struct InFlightPicks {
    pub(super) coins: std::sync::Mutex<HashSet<i64>>,
    /// Tests only: reserve nothing, so the cross-process race is reachable
    /// within one process.
    #[cfg(test)]
    pub(super) off: std::sync::atomic::AtomicBool,
}

impl InFlightPicks {
    fn coins(&self) -> std::sync::MutexGuard<'_, HashSet<i64>> {
        self.coins.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn is_off(&self) -> bool {
        #[cfg(test)]
        {
            self.off.load(std::sync::atomic::Ordering::SeqCst)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    /// Tests only: turn the reservation off (`true`) or on.
    #[cfg(test)]
    pub(crate) fn set_off(&self, off: bool) {
        self.off.store(off, std::sync::atomic::Ordering::SeqCst);
    }

    /// The coins reserved now, by every call.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> HashSet<i64> {
        self.coins().clone()
    }
}

/// One `createAction` call's reserved pre-check picks. Dropped when the call
/// returns, on every path (a refusal, an early `?`, a panic), and every coin
/// it holds is released.
pub(crate) struct PickReservation {
    pub(super) in_flight: std::sync::Arc<InFlightPicks>,
    pub(super) mine: Vec<i64>,
}

impl PickReservation {
    pub(crate) fn new(in_flight: std::sync::Arc<InFlightPicks>) -> Self {
        Self {
            in_flight,
            mine: Vec::new(),
        }
    }

    /// The coins this call holds, in the order picked.
    fn mine(&self) -> &[i64] {
        &self.mine
    }

    /// The coins a dry run may not pick: this call's and every other call's.
    fn unavailable(&self) -> Vec<i64> {
        if self.in_flight.is_off() {
            return self.mine.clone();
        }
        let mut ids: Vec<i64> = self.in_flight.coins().iter().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Reserves `id` for this call; `false` when another call holds it.
    pub(super) fn try_take(&mut self, id: i64) -> bool {
        if !self.in_flight.is_off() && !self.in_flight.coins().insert(id) {
            return false;
        }
        self.mine.push(id);
        true
    }

    /// Lets `id` go, if this call holds it.
    fn release(&mut self, id: i64) {
        let held = self.mine.len();
        self.mine.retain(|m| *m != id);
        if self.mine.len() != held && !self.in_flight.is_off() {
            self.in_flight.coins().remove(&id);
        }
    }

    /// Lets every coin this call holds go.
    fn release_all(&mut self) {
        if self.in_flight.is_off() {
            self.mine.clear();
            return;
        }
        let mut coins = self.in_flight.coins();
        for id in self.mine.drain(..) {
            coins.remove(&id);
        }
    }
}

impl Drop for PickReservation {
    fn drop(&mut self) {
        self.release_all();
    }
}

/// Releases a previously allocated change input.
pub(super) async fn release_change_input(
    conn: &mut SqliteConnection,
    output_id: i64,
    pick: &mut ChangePick<'_>,
) -> Result<()> {
    if let ChangePick::DryRun { reserved, .. } = pick {
        reserved.release(output_id);
        return Ok(());
    }
    sqlx::query("UPDATE outputs SET spendable = 1, spent_by = NULL WHERE output_id = ?")
        .bind(output_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Allocates a change input from the default basket.
#[cfg(any(test, feature = "bench-support"))]
pub(in crate::storage::sqlx) async fn allocate_change_input(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    target_satoshis: u64,
    require_spendable: bool,
) -> Result<Option<AllocatedChangeInput>> {
    allocate_change_input_by(
        conn,
        user_id,
        basket_id,
        transaction_id,
        target_satoshis,
        require_spendable,
        &mut ChangePick::Allocate,
    )
    .await
}

/// Unstable access for the isolated allocation benchmark, without a new
/// wallet operation or a change to the production door's selection mode.
#[cfg(feature = "bench-support")]
pub(crate) async fn benchmark_allocate_change_input(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    target_satoshis: u64,
) -> Result<Option<i64>> {
    allocate_change_input(
        conn,
        user_id,
        basket_id,
        transaction_id,
        target_satoshis,
        true,
    )
    .await
    .map(|input| input.map(|input| input.output_id))
}

/// Picks a change input from the default basket as `pick` says.
///
/// The order: a coin covering the target before one that does not, a
/// `completed` coin before an `unproven` one, the nearest value to the
/// target, then the lower `output_id`. The pick is a function of the
/// store's rows and never of a byte length (a BEEF's length moves with DER
/// signature lengths between runs, #105). The reference
/// (`StorageKnex.allocateChangeInput`) ends every order on `outputId`: an
/// exact match by `outputId asc`, at or above the target by `satoshis asc,
/// outputId asc`, below it by `satoshis desc, outputId desc`. This crate ends
/// every branch on `output_id ASC`, so below the target it picks the same
/// value as the reference and the opposite id (#108).
pub(super) async fn allocate_change_input_by(
    conn: &mut SqliteConnection,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    target_satoshis: u64,
    require_spendable: bool,
    pick: &mut ChangePick<'_>,
) -> Result<Option<AllocatedChangeInput>> {
    let spendable_clause = if require_spendable {
        "AND o.spendable = 1"
    } else {
        ""
    };
    // A dry run reserves each coin it picks; a coin another call reserved
    // between the query and the reservation is passed over and the query
    // asked again (each pass over adds the coin to the unavailable set, so
    // the loop ends within the number of coins).
    let row = loop {
        let (pick_clause, pick_ids): (String, Vec<i64>) = match pick {
            #[cfg(any(test, feature = "bench-support"))]
            ChangePick::Allocate => (String::new(), Vec::new()),
            ChangePick::DryRun {
                offer: Some(offer), ..
            } if offer.is_empty() => return Ok(None),
            ChangePick::DryRun {
                reserved,
                skip,
                offer,
                ..
            } => {
                let mut unavailable = reserved.unavailable();
                unavailable.extend_from_slice(skip);
                let mut clause = String::new();
                if !unavailable.is_empty() {
                    clause = format!(
                        "AND o.output_id NOT IN ({})",
                        placeholders(unavailable.len())
                    );
                }
                if let Some(offer) = offer {
                    clause.push_str(&format!(
                        " AND o.output_id IN ({})",
                        placeholders(offer.len())
                    ));
                    unavailable.extend_from_slice(offer);
                }
                (clause, unavailable)
            }
            ChangePick::Within { allowed, .. } if allowed.is_empty() => return Ok(None),
            ChangePick::Within { allowed, .. } => (
                format!("AND o.output_id IN ({})", placeholders(allowed.len())),
                allowed.to_vec(),
            ),
        };

        // Try to find an output with at least target_satoshis
        let sql = format!(
            r#"
            SELECT o.output_id, o.satoshis, o.txid, o.vout, o.locking_script,
                   o.derivation_prefix, o.derivation_suffix, o.sender_identity_key
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            WHERE o.user_id = ?
              AND o.basket_id = ?
              AND o.change = 1
              AND o.spent_by IS NULL
              AND t.status IN ('completed', 'unproven')
              {MANAGED_CHANGE_CLAUSE}
              {RECEIVED_COIN_CLAUSE}
              {}
              {}
            ORDER BY
                CASE WHEN o.satoshis >= ? THEN 0 ELSE 1 END,
                CASE WHEN t.status = 'completed' THEN 0 ELSE 1 END,
                ABS(o.satoshis - ?) ASC,
                o.output_id ASC
            LIMIT 1
            "#,
            spendable_clause, pick_clause
        );

        let mut query = sqlx::query(&sql)
            .bind(user_id)
            .bind(basket_id)
            .bind(pick.allows_received());
        for id in &pick_ids {
            query = query.bind(*id);
        }
        let row = query
            .bind(target_satoshis as i64)
            .bind(target_satoshis as i64)
            .fetch_optional(&mut *conn)
            .await?;
        match (row, &mut *pick) {
            (Some(row), ChangePick::DryRun { reserved, .. }) => {
                if reserved.try_take(row.get("output_id")) {
                    break Some(row);
                }
            }
            (row, _) => break row,
        }
    };

    if let Some(row) = row {
        let output_id: i64 = row.get("output_id");
        let satoshis: i64 = row.get("satoshis");
        let txid: String = row.get("txid");
        let vout: i32 = row.get("vout");
        let locking_script: Option<Vec<u8>> = row.get("locking_script");
        let derivation_prefix: Option<String> = row.get("derivation_prefix");
        let derivation_suffix: Option<String> = row.get("derivation_suffix");
        let sender_identity_key: Option<String> = row.get("sender_identity_key");

        if let ChangePick::DryRun { .. } = pick {
            // Reserved above; a dry run marks nothing.
        } else {
            // Mark as allocated (spent_by this transaction)
            let now = Utc::now();
            sqlx::query(
            "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? WHERE output_id = ?",
        )
            .bind(transaction_id)
            .bind(now)
            .bind(output_id)
            .execute(&mut *conn)
            .await?;
        }

        Ok(Some(AllocatedChangeInput {
            output_id,
            satoshis: satoshis as u64,
            txid,
            vout,
            locking_script: locking_script.unwrap_or_default(),
            derivation_prefix,
            derivation_suffix,
            sender_identity_key,
        }))
    } else {
        Ok(None)
    }
}

/// `?, ?, ?` for `n` binds.
fn placeholders(n: usize) -> String {
    vec!["?"; n].join(", ")
}
impl Door<'_> {
    /// The pre-check's pick: a dry run within the host's offer, never `skip`,
    /// the named coins ahead of it (#353).
    fn dry_run<'b>(&'b self, reserved: &'b mut PickReservation, skip: &'b [i64]) -> ChangePick<'b> {
        ChangePick::DryRun {
            reserved,
            skip,
            offer: self.offer,
            allow_received: self.allows_received(),
            must_spend: self.must_spend,
        }
    }

    /// Before the write lock, on a reader: pick the change coins as the
    /// allocation would (marking nothing) and assemble the BEEF they and the
    /// caller's inputs need, every stored proof's root read against the
    /// tracker with no lock held (stillConfirmed) and every answer kept.
    /// The walk's read checks are written through the writer, one short
    /// write each, as before. Returns the coins picked; an error is the
    /// call's refusal, and the action wrote nothing.
    pub(super) async fn precheck(
        &self,
        chain_tracker: Option<&dyn ChainTracker>,
        storage_tracker: Option<&dyn ChainTracker>,
        reserved: &mut PickReservation,
    ) -> Result<Vec<i64>> {
        // A second pre-check picks afresh: the first one's coins go back.
        reserved.release_all();
        let mut conn = self.storage.pool().acquire().await?;
        let extended_outputs = validate_and_extend_outputs(self.args)?;
        let extended_inputs =
            validate_and_extend_inputs(self.storage, &mut conn, self.user_id, self.args).await?;
        let available_change_count = count_change_inputs_under_rule(
            &mut conn,
            self.user_id,
            self.change_basket.basket_id,
            !self.is_delayed(),
            self.allows_received(),
        )
        .await?;
        let caller = chain_tracker.map(|inner| RecordingTracker {
            inner,
            answers: self.answers,
        });
        let own = storage_tracker.map(|inner| RecordingTracker {
            inner,
            answers: self.answers,
        });
        let caller = caller.as_ref().map(|t| t as &dyn ChainTracker);
        let own = own.as_ref().map(|t| t as &dyn ChainTracker);

        // The change coins whose BEEF cannot be built from what this wallet
        // stores (review 2 F2: a reorg-demoted received coin, its parents
        // never stored). Each is skipped and the coins picked again; every
        // round skips at least one more coin, so the loop ends within the
        // number of coins.
        let mut unspendable: Vec<(AllocatedChangeInput, Vec<String>)> = Vec::new();
        loop {
            reserved.release_all();
            let skip: Vec<i64> = unspendable.iter().map(|(c, _)| c.output_id).collect();
            let planned = plan_change(
                &mut conn,
                self.user_id,
                self.change_basket,
                0, // no row: a dry run marks nothing
                self.args,
                &extended_inputs,
                &extended_outputs,
                available_change_count,
                self.is_delayed(),
                &mut self.dry_run(&mut *reserved, &skip),
                &self.storage.rng,
            )
            .await;
            // Uncovered: planned once more without the other calls'
            // reservations (a private one, released when dropped). Covered
            // so, the shortfall is the calls in flight, not the funds: the
            // refusal is StoreBusy, at once (review 3 F2 of bsv-wallet-cli
            // #44). No wait: a call in flight that commits spends its coin,
            // and after it the true answer may be a shortfall; the host's
            // repeat reads that state.
            if let Err(Error::InsufficientFunds { .. }) = planned {
                let mut own_only = PickReservation::new(Default::default());
                let covered_without_others = plan_change(
                    &mut conn,
                    self.user_id,
                    self.change_basket,
                    0,
                    self.args,
                    &extended_inputs,
                    &extended_outputs,
                    available_change_count,
                    self.is_delayed(),
                    &mut self.dry_run(&mut own_only, &skip),
                    &self.storage.rng,
                )
                .await
                .is_ok();
                if covered_without_others {
                    return Err(Error::StoreBusy {
                        reason: StoreBusyReason::InFlight,
                    });
                }
            }
            let change_result = match planned {
                Ok((change_result, _)) => change_result,
                // The coins offered covered the action until a skipped coin
                // was taken out: that coin is the refusal, not the funds.
                Err(Error::InsufficientFunds { .. }) if !unspendable.is_empty() => {
                    let (coin, missing) = unspendable.swap_remove(0);
                    return Err(Error::UnspendableCoin {
                        outpoint: format!("{}.{}", coin.txid, coin.vout),
                        missing,
                    });
                }
                Err(e) => return Err(e),
            };

            let built = build_input_beef(
                &mut conn,
                Some(self.storage.writer()),
                caller,
                &extended_inputs,
                &change_result.allocated_change_inputs,
                self.args.input_beef.as_deref(),
                &self.known_txids(),
                self.return_txid_only(),
                self.trust_self(),
                own,
            )
            .await;
            let Err(refusal) = built else {
                return Ok(reserved.mine().to_vec());
            };
            // Which of the picked coins cannot be built on its own? None:
            // the refusal is the caller's inputs', and it stands.
            let before = unspendable.len();
            for coin in &change_result.allocated_change_inputs {
                if let Some(missing) = change_coin_gap(
                    &mut conn,
                    Some(self.storage.writer()),
                    caller.or(own),
                    &coin.txid,
                )
                .await?
                {
                    // A named coin is never picked around: it is the refusal.
                    if self.must_spend.contains(&coin.output_id) {
                        return Err(Error::UnspendableCoin {
                            outpoint: format!("{}.{}", coin.txid, coin.vout),
                            missing,
                        });
                    }
                    tracing::info!(
                        outpoint = %format!("{}.{}", coin.txid, coin.vout),
                        missing = ?missing,
                        marker = "create_action_unspendable_coin_skipped",
                        "create_action: a change coin's stored ancestry is incomplete; picking again without it"
                    );
                    unspendable.push((coin.clone(), missing));
                }
            }
            if unspendable.len() == before {
                return Err(refusal);
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/allocation.rs"]
mod tests;

#[cfg(test)]
mod received_rule_tests {
    use super::*;
    use crate::storage::sqlx::create_action::test_support::seed_change_output_with_status;
    use crate::storage::WalletStorageWriter;
    use crate::StorageSqlx;

    async fn received_change() -> (StorageSqlx, i64, i64) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("received-rule", "02test_key")
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage.find_or_insert_user("02user_key").await.unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();
        seed_change_output_with_status(&storage, user.user_id, 10_000, "unproven", "a169").await;
        sqlx::query("UPDATE outputs SET purpose = 'receive'")
            .execute(storage.writer())
            .await
            .unwrap();
        (storage, user.user_id, basket.basket_id)
    }

    #[tokio::test]
    async fn received_unproven_change_is_not_offered_without_the_action_rule() {
        let (storage, user_id, basket_id) = received_change().await;
        let mut conn = storage.pool().acquire().await.unwrap();
        let count = count_change_inputs(&mut conn, user_id, basket_id, true)
            .await
            .unwrap();
        let mut reserved = PickReservation::new(Default::default());
        let picked = allocate_change_input_by(
            &mut conn,
            user_id,
            basket_id,
            0,
            1000,
            true,
            &mut ChangePick::DryRun {
                reserved: &mut reserved,
                skip: &[],
                offer: None,
                allow_received: false,
                must_spend: &[],
            },
        )
        .await
        .unwrap();
        assert_eq!((count, picked.is_some()), (0, false));
    }

    #[tokio::test]
    async fn received_rule_preserves_mined_and_own_change_eligibility() {
        let (storage, user_id, basket_id) = received_change().await;
        for (status, purpose, allow_received, expected) in [
            ("unproven", "receive", false, 0),
            ("unproven", "receive", true, 1),
            ("completed", "receive", false, 1),
            ("completed", "receive", true, 1),
            ("unproven", "change", false, 1),
            ("unproven", "", false, 0),
            ("completed", "change", false, 1),
            ("nosend", "receive", true, 0),
            ("failed", "receive", true, 0),
        ] {
            sqlx::query("UPDATE transactions SET status = ?")
                .bind(status)
                .execute(storage.writer())
                .await
                .unwrap();
            sqlx::query("UPDATE outputs SET purpose = ?")
                .bind(purpose)
                .execute(storage.writer())
                .await
                .unwrap();
            let mut conn = storage.pool().acquire().await.unwrap();
            for require_spendable in [false, true] {
                let count = count_change_inputs_under_rule(
                    &mut conn,
                    user_id,
                    basket_id,
                    require_spendable,
                    allow_received,
                )
                .await
                .unwrap();
                let mut reserved = PickReservation::new(Default::default());
                let picked = allocate_change_input_by(
                    &mut conn,
                    user_id,
                    basket_id,
                    0,
                    1000,
                    require_spendable,
                    &mut ChangePick::DryRun {
                        reserved: &mut reserved,
                        skip: &[],
                        offer: Some(&[1]),
                        allow_received,
                        must_spend: &[],
                    },
                )
                .await
                .unwrap();
                assert_eq!((count, picked.is_some()), (expected, expected == 1),
                    "status={status}, purpose={purpose}, rule={allow_received}, spendable={require_spendable}");
            }
        }
    }
}
