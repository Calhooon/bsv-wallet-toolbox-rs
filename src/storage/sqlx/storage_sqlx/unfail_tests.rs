#[cfg(test)]
mod tests {
    use super::super::*;

    // =========================================================================
    // un_fail: auto-unfail canary (producer) + explicit-request consumer
    // =========================================================================

    mod un_fail_recovery {
        use super::*;
        use crate::services::mock::{MockErrorKind, MockResponse, MockWalletServicesBuilder};
        use crate::services::traits::{
            GetMerklePathResult, GetStatusForTxidsResult, TxStatusDetail,
        };
        use bsv_rs::script::{LockingScript, UnlockingScript};
        use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
        use std::sync::Arc;

        async fn create_storage() -> StorageSqlx {
            let storage = StorageSqlx::in_memory().await.unwrap();
            storage
                .migrate("test-unfail", &"0".repeat(64))
                .await
                .unwrap();
            storage.make_available().await.unwrap();
            storage
        }

        async fn create_user(storage: &StorageSqlx) -> i64 {
            let (user, _) = storage
                .find_or_insert_user(
                    "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
                )
                .await
                .unwrap();
            user.user_id
        }

        /// A minimal spending tx consuming `source_txid:source_vout`;
        /// returns (raw bytes, txid).
        fn spending_tx(source_txid: &str, source_vout: u32) -> (Vec<u8>, String) {
            let mut tx = Transaction::new();
            tx.version = 1;
            tx.lock_time = 0;
            let mut input = TransactionInput::new(source_txid.to_string(), source_vout);
            input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
            tx.inputs.push(input);
            tx.outputs.push(TransactionOutput {
                satoshis: Some(9000),
                locking_script: LockingScript::from_hex(
                    "76a914000000000000000000000000000000000000000088ac",
                )
                .unwrap(),
                change: false,
            });
            let txid = tx.id();
            (tx.to_binary(), txid)
        }

        async fn insert_tx(storage: &StorageSqlx, user_id: i64, txid: &str, status: &str) -> i64 {
            let now = chrono::Utc::now();
            sqlx::query(
                "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, is_outgoing, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, 'unfail test tx', 0, 1, 0, 1, ?, ?)",
            )
            .bind(user_id)
            .bind(txid)
            .bind(status)
            .bind(format!("ref-{}", &txid[..8]))
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
        }

        async fn insert_output(
            storage: &StorageSqlx,
            user_id: i64,
            transaction_id: i64,
            txid: &str,
            vout: i32,
            spendable: bool,
        ) -> i64 {
            let now = chrono::Utc::now();
            sqlx::query(
                "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, created_at, updated_at) \
                 VALUES (?, ?, ?, 1, ?, 9000, 'storage', 'change', 'P2PKH', ?, ?, ?)",
            )
            .bind(user_id)
            .bind(transaction_id)
            .bind(spendable)
            .bind(vout)
            .bind(txid)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
        }

        /// Insert a proven_tx_req row aged `hours_old` hours into the past
        /// (controls canary backoff eligibility).
        async fn insert_req_aged(
            storage: &StorageSqlx,
            txid: &str,
            status: &str,
            raw_tx: &[u8],
            hours_old: i64,
        ) -> i64 {
            let stamp = chrono::Utc::now() - chrono::Duration::hours(hours_old);
            sqlx::query(
                "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notify, notified, raw_tx, created_at, updated_at) \
                 VALUES (?, ?, 0, '{}', '{}', 0, ?, ?, ?)",
            )
            .bind(txid)
            .bind(status)
            .bind(raw_tx)
            .bind(stamp)
            .bind(stamp)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
        }

        fn status_response(txid: &str, status: &str) -> MockResponse<GetStatusForTxidsResult> {
            MockResponse::Success(GetStatusForTxidsResult {
                name: "mock".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.to_string(),
                    status: status.to_string(),
                    depth: Some(1),
                    ..Default::default()
                }],
            })
        }

        async fn req_state(storage: &StorageSqlx, txid: &str) -> (String, i64) {
            let row = sqlx::query("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
                .bind(txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            (row.get("status"), row.get::<i32, _>("attempts") as i64)
        }

        async fn tx_status(storage: &StorageSqlx, txid: &str) -> String {
            sqlx::query("SELECT status FROM transactions WHERE txid = ?")
                .bind(txid)
                .fetch_one(storage.pool())
                .await
                .unwrap()
                .get("status")
        }

        async fn output_state(storage: &StorageSqlx, output_id: i64) -> (bool, Option<i64>) {
            let row = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
                .bind(output_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            (row.get("spendable"), row.get("spent_by"))
        }

        /// The incident shape (btc-relay 2026-07-28): a mined tx wrongly
        /// demoted to failed/invalid recovers via the canary — req →
        /// 'unmined' (attempts 0), tx → 'unproven', its de-credited change
        /// re-enabled, and the coin it consumed re-marked spent.
        #[tokio::test]
        async fn canary_recovers_mined_false_fail() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            // The consumed coin: released back to spendable when the
            // spender was wrongly failed.
            let prev_txid = "11".repeat(32);
            let prev_tx_id = insert_tx(&storage, user_id, &prev_txid, "completed").await;
            let prev_out = insert_output(&storage, user_id, prev_tx_id, &prev_txid, 0, true).await;

            // The false-failed spender: tx 'failed', change de-credited,
            // req 'invalid', aged past the 1h canary backoff.
            let (raw, txid) = spending_tx(&prev_txid, 0);
            let tx_id = insert_tx(&storage, user_id, &txid, "failed").await;
            let own_out = insert_output(&storage, user_id, tx_id, &txid, 0, false).await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;

            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "mined"))
                .is_utxo_response(MockResponse::Success(true))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            // Own change output re-enabled (is_utxo said true).
            assert_eq!(output_state(&storage, own_out).await, (true, None));
            // Consumed coin re-marked spent by this transaction.
            assert_eq!(output_state(&storage, prev_out).await, (false, Some(tx_id)));
        }

        /// No chain evidence → the row is re-stamped (attempts+1) but stays
        /// 'invalid' and watched; nothing else moves.
        #[tokio::test]
        async fn canary_restamps_unknown_and_keeps_watching() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "22".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;

            // Every source: the status sources, no broadcaster, no proof.
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("invalid".into(), 1));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        /// The hourly backoff: a row stamped within the last hour is not
        /// re-checked even when the chain would vouch for it.
        #[tokio::test]
        async fn canary_skips_recent_rows() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "33".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "mined"))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("invalid".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        /// An explicit 'unfail' request recovers when a merkle path exists,
        /// exercising the raw_tx-from-req input re-mark path.
        #[tokio::test]
        async fn explicit_unfail_recovers_with_proof() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "44".repeat(32);
            let prev_tx_id = insert_tx(&storage, user_id, &prev_txid, "completed").await;
            let prev_out = insert_output(&storage, user_id, prev_tx_id, &prev_txid, 0, true).await;

            let (raw, txid) = spending_tx(&prev_txid, 0);
            let tx_id = insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "unfail", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .is_utxo_response(MockResponse::Success(true))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(output_state(&storage, prev_out).await, (false, Some(tx_id)));
        }

        /// A provider outage must hold an explicit 'unfail' request at
        /// 'unfail' — terminalizing on missing evidence is the incident
        /// class where an outage makes a false-fail permanent.
        #[tokio::test]
        async fn explicit_unfail_holds_on_provider_error() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "55".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "unfail", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .get_merkle_path_response(MockResponse::Error(
                    MockErrorKind::ServiceError,
                    "provider outage".to_string(),
                ))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unfail".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        /// A positive no-proof answer moves an explicit 'unfail' request to
        /// 'invalid' — which the canary then keeps watching (not terminal).
        #[tokio::test]
        async fn explicit_unfail_no_proof_becomes_invalid() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "66".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "unfail", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                    name: Some("mock".to_string()),
                    merkle_path: None,
                    header: None,
                    error: None,
                    notes: vec![],
                }))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("invalid".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        // =================================================================
        // One broadcaster's refusal against every other source
        // (Calgooon/zanaadu-v2#357)
        // =================================================================

        use crate::monitor::ArcadeEventsTask;
        use crate::services::broadcast_memory::{
            BroadcastStatus, BROADCAST_STATUS_ACCEPTED, PROVIDER_ARCADE_V2,
            PROVIDER_GORILLAPOOL_ARC, PROVIDER_TAAL_ARC,
        };
        use std::sync::atomic::{AtomicBool, Ordering};

        fn no_proof() -> MockResponse<GetMerklePathResult> {
            MockResponse::Success(GetMerklePathResult {
                name: Some("mock".to_string()),
                merkle_path: None,
                header: None,
                error: None,
                notes: vec![],
            })
        }

        /// A broadcast transaction as the wallet holds it after a submit one
        /// broadcaster accepted: tx 'unproven', req 'unmined', its change
        /// spendable. Returns (txid, change output id).
        async fn seed_broadcast(storage: &StorageSqlx, seed: &str) -> (String, i64) {
            let user_id = create_user(storage).await;
            let (raw, txid) = spending_tx(&seed.repeat(32), 0);
            let tx_id = insert_tx(storage, user_id, &txid, "unproven").await;
            let change = insert_output(storage, user_id, tx_id, &txid, 0, true).await;
            insert_req_aged(storage, &txid, "unmined", &raw, 0).await;
            (txid, change)
        }

        async fn memory_status(storage: &StorageSqlx, txid: &str, provider: &str) -> String {
            storage
                .broadcast_status_of(txid, provider)
                .await
                .unwrap()
                .map(|r| r.status)
                .unwrap_or_default()
        }

        /// The incident (2026-10-07, the 1.9 MB post): Arcade 500 on submit,
        /// GorillaPool ARC accepted, then Arcade's stream said REJECTED 460
        /// "missing input source data" and the wallet failed a transaction
        /// that was MINED at 970030. Now the REJECTED is Arcade's own view:
        /// the transaction stays unproven with its change, and Arcade's later
        /// MINED takes it to the proof task.
        #[tokio::test]
        async fn arcade_rejected_after_gorillapool_accepted_stays_unproven_and_mined_proves_it() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "a1").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, BROADCAST_STATUS_ACCEPTED)
                .await
                .unwrap();
            let trigger = AtomicBool::new(false);

            let applied =
                ArcadeEventsTask::apply_status_event(&storage, &txid, "REJECTED", &trigger)
                    .await
                    .unwrap();

            assert!(
                !applied,
                "a refusal another broadcaster overrules applies nothing"
            );
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(output_state(&storage, change).await, (true, None));
            // Arcade lacks it; GorillaPool's acceptance stands.
            assert_eq!(
                memory_status(&storage, &txid, PROVIDER_ARCADE_V2).await,
                "rejected"
            );
            assert_eq!(
                memory_status(&storage, &txid, PROVIDER_GORILLAPOOL_ARC).await,
                "accepted"
            );

            // Later MINED: the proof task is asked now.
            ArcadeEventsTask::apply_status_event(&storage, &txid, "MINED", &trigger)
                .await
                .unwrap();
            assert!(trigger.load(Ordering::SeqCst));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
        }

        /// No memory row, but the network holds it by a live read of another
        /// broadcaster (GorillaPool SEEN_ON_NETWORK): not final either.
        #[tokio::test]
        async fn refusal_overruled_by_another_broadcasters_live_status() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "a2").await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_GORILLAPOOL_ARC.to_string(), BroadcastStatus::Seen),
                ])
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap();

            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }

        /// Every broadcaster refuses or never heard of it and the status
        /// sources do not know it: still hints (bsv-stack-lean #66). No
        /// word, the change counted; the re-ask and the proof task decide.
        /// Until 0.7.4 this was final: failed, req invalid, change dead.
        #[tokio::test]
        async fn a_refusal_from_every_source_is_still_a_hint() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "a3").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_ACCEPTED)
                .await
                .unwrap();
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_TAAL_ARC.to_string(), BroadcastStatus::Unknown),
                    (
                        PROVIDER_GORILLAPOOL_ARC.to_string(),
                        BroadcastStatus::Rejected,
                    ),
                ])
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap();

            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }

        /// A conflict verdict (466, a competitor) is a hint like any other
        /// broadcaster's word (bsv-stack-lean #66): no word is written, the
        /// refusing broadcaster is remembered. Until 0.7.4 it was final
        /// unless the network held the transaction.
        #[tokio::test]
        async fn a_conflict_word_is_a_hint() {
            let storage = create_storage().await;
            let (txid, _) = seed_broadcast(&storage, "a4").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, BROADCAST_STATUS_ACCEPTED)
                .await
                .unwrap();

            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, true)
                .await
                .unwrap();

            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(
                storage
                    .broadcast_status_of(&txid, PROVIDER_ARCADE_V2)
                    .await
                    .unwrap()
                    .map(|r| r.status),
                Some(crate::services::broadcast_memory::BROADCAST_STATUS_REJECTED.to_string())
            );
        }

        /// The canary asks EVERY source, not only the one that refused: the
        /// status sources and Arcade say nothing, GorillaPool says MINED.
        /// Recovered without a hand-set 'unfail'.
        #[tokio::test]
        async fn canary_recovers_on_another_broadcasters_mined() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;
            let (raw, txid) = spending_tx(&"a5".repeat(32), 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_GORILLAPOOL_ARC.to_string(), BroadcastStatus::Mined),
                ])
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        /// ... and a merkle path the chain tracker accepted is a source too.
        #[tokio::test]
        async fn canary_recovers_on_a_checked_merkle_path() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;
            let (raw, txid) = spending_tx(&"a6".repeat(32), 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        // ============ LENS replay (not part of the lane) ============

        async fn age_memory(storage: &StorageSqlx, txid: &str, provider: &str, hours: i64) {
            sqlx::query(&format!(
                "UPDATE broadcast_seen SET seen_at = datetime('now', '-{} hours') WHERE txid = ? AND provider = ?",
                hours
            ))
            .bind(txid)
            .bind(provider)
            .execute(storage.writer())
            .await
            .unwrap();
        }

        fn evicted_everywhere(txid: &str) -> crate::services::mock::MockWalletServices {
            MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_TAAL_ARC.to_string(), BroadcastStatus::Unknown),
                    (
                        PROVIDER_GORILLAPOOL_ARC.to_string(),
                        BroadcastStatus::Unknown,
                    ),
                ])
                .build()
        }

        /// LENS: GorillaPool SEEN six hours ago, evicted from every mempool
        /// since, then Arcade's REJECTED: a hint, no word, the change still
        /// counted (bsv-stack-lean #66). Until 0.7.4 the REJECTED was final
        /// here and the change written off.
        #[tokio::test]
        async fn lens_stale_seen_then_rejected_writes_no_word() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "b1").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, "seen")
                .await
                .unwrap();
            age_memory(&storage, &txid, PROVIDER_GORILLAPOOL_ARC, 6).await;
            WalletStorageProvider::set_services(&storage, Arc::new(evicted_everywhere(&txid)));
            let trigger = AtomicBool::new(false);
            assert!(
                !ArcadeEventsTask::apply_status_event(&storage, &txid, "REJECTED", &trigger)
                    .await
                    .unwrap()
            );
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }

        /// LENS (corrected): a fresh SEEN still overrules (the incident).
        #[tokio::test]
        async fn lens_fresh_seen_still_overrules_rejected() {
            let storage = create_storage().await;
            let (txid, _) = seed_broadcast(&storage, "b2").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, "seen")
                .await
                .unwrap();
            age_memory(&storage, &txid, PROVIDER_GORILLAPOOL_ARC, 1).await;
            WalletStorageProvider::set_services(&storage, Arc::new(evicted_everywhere(&txid)));
            assert!(!storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap());
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        /// LENS: every live source down (no broadcaster answers, status
        /// sources error), no memory row: the refusal is still a hint
        /// (bsv-stack-lean #66). Until 0.7.4 it was final, failing closed
        /// to `failed` as the reference does unconditionally.
        #[tokio::test]
        async fn lens_sources_down_refusal_is_a_hint() {
            let storage = create_storage().await;
            let (txid, _) = seed_broadcast(&storage, "b3").await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(MockResponse::Error(
                    crate::services::mock::MockErrorKind::NetworkError,
                    "down".into(),
                ))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));
            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap();
            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        /// LENS: the incident with SEEN (not ACCEPTED) from GorillaPool, then
        /// Arcade MINED: stays unproven, change visible, proof triggered.
        #[tokio::test]
        async fn lens_incident_with_seen_then_mined() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "b4").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, "seen")
                .await
                .unwrap();
            let trigger = AtomicBool::new(false);
            assert!(
                !ArcadeEventsTask::apply_status_event(&storage, &txid, "REJECTED", &trigger)
                    .await
                    .unwrap()
            );
            ArcadeEventsTask::apply_status_event(&storage, &txid, "MINED", &trigger)
                .await
                .unwrap();
            assert!(trigger.load(Ordering::SeqCst));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }
    }
}
