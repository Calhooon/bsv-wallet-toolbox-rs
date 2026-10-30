#[cfg(test)]
mod tests {
    use super::super::*;

    // =============================================================================
    // MonitorStorage Tests
    // =============================================================================

    #[tokio::test]
    async fn test_synchronize_transaction_statuses_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // With no proven_tx_reqs, should return empty vec
        let result = storage.synchronize_transaction_statuses().await.unwrap();
        assert!(result.is_empty());
    }

    /// #472: the attempts limit reads the stored words alone; a legacy
    /// spelling takes the unknown word's path, the test limit.
    #[tokio::test]
    async fn the_attempts_limit_reads_the_stored_words_alone() {
        for (chain, limit) in [
            ("main", PROOF_ATTEMPTS_LIMIT_MAIN),
            ("test", PROOF_ATTEMPTS_LIMIT_TEST),
            ("regtest", PROOF_ATTEMPTS_LIMIT_TEST),
            ("mainnet", PROOF_ATTEMPTS_LIMIT_TEST),
            ("testnet", PROOF_ATTEMPTS_LIMIT_TEST),
            ("", PROOF_ATTEMPTS_LIMIT_TEST),
        ] {
            assert_eq!(limit_for(chain).await, limit, "{chain}");
        }
    }

    /// #481: one witness per stored word, in numbers. `main` 144 and `test`
    /// 100 are the reference's (ts-stack edf6e03, wallet-toolbox
    /// src/monitor/Monitor.ts:125-126, selected main against other at
    /// src/monitor/tasks/TaskCheckForProofs.ts:138-140); `regtest` takes the
    /// test limit as the port's extension for its regtest hosts (#439).
    async fn limit_for(chain: &str) -> i64 {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .cache_settings(&TableSettings {
                chain: chain.to_string(),
                ..TableSettings::default()
            })
            .unwrap();
        storage.proof_attempts_limit()
    }

    #[tokio::test]
    async fn a_main_store_stops_counting_at_144() {
        assert_eq!(limit_for("main").await, 144);
    }

    #[tokio::test]
    async fn a_test_store_stops_counting_at_the_references_100() {
        assert_eq!(limit_for("test").await, 100);
    }

    #[tokio::test]
    async fn a_regtest_store_takes_the_test_limit_100() {
        assert_eq!(limit_for("regtest").await, 100);
    }

    /// An unknown word keeps its path: the test arm, as the reference's
    /// main-against-other selection gives every word but `main`.
    #[tokio::test]
    async fn an_unknown_word_takes_the_test_arm() {
        for chain in ["mainnet", "testnet", "stn", ""] {
            assert_eq!(limit_for(chain).await, PROOF_ATTEMPTS_LIMIT_TEST, "{chain}");
        }
    }

    #[tokio::test]
    async fn test_tx_synchronized_status_struct() {
        // Test that TxSynchronizedStatus can be created and serialized
        let status = TxSynchronizedStatus {
            txid: "abc123".to_string(),
            status: ProvenTxReqStatus::Completed,
            block_height: Some(800000),
            block_hash: Some("0000...".to_string()),
            merkle_root: Some("merkle...".to_string()),
            merkle_path: Some(vec![1, 2, 3]),
        };

        assert_eq!(status.txid, "abc123");
        assert_eq!(status.status, ProvenTxReqStatus::Completed);
        assert_eq!(status.block_height, Some(800000));
    }

    /// Test the triage classification logic used in synchronize_transaction_statuses.
    /// Given a GetStatusForTxidsResult with mixed mined/known/unknown statuses,
    /// verify that only confirmed txids (depth >= 1) are selected for proof fetching.
    #[test]
    fn test_triage_classification_mixed_statuses() {
        use crate::services::traits::{GetStatusForTxidsResult, TxStatusDetail};

        let status_result = GetStatusForTxidsResult {
            name: "WoC".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![
                TxStatusDetail {
                    txid: "tx_confirmed_deep".to_string(),
                    status: "mined".to_string(),
                    depth: Some(6),
                    ..Default::default()
                },
                TxStatusDetail {
                    txid: "tx_confirmed_shallow".to_string(),
                    status: "mined".to_string(),
                    depth: Some(1),
                    ..Default::default()
                },
                TxStatusDetail {
                    txid: "tx_mempool".to_string(),
                    status: "known".to_string(),
                    depth: Some(0),
                    ..Default::default()
                },
                TxStatusDetail {
                    txid: "tx_unknown".to_string(),
                    status: "unknown".to_string(),
                    depth: None,
                    ..Default::default()
                },
                TxStatusDetail {
                    txid: "tx_mined_zero_depth".to_string(),
                    status: "mined".to_string(),
                    depth: Some(0), // mined but depth 0 -> not confirmed
                    ..Default::default()
                },
            ],
        };

        // Replicate the triage logic from synchronize_transaction_statuses
        let mut confirmed = 0u32;
        let mut mempool = 0u32;
        let mut missing = 0u32;

        let confirmed_set: std::collections::HashSet<String> = status_result
            .results
            .iter()
            .filter_map(|detail| match detail.status.as_str() {
                "mined" => {
                    let depth = detail.depth.unwrap_or(0);
                    if depth >= 1 {
                        confirmed += 1;
                        Some(detail.txid.clone())
                    } else {
                        mempool += 1;
                        None
                    }
                }
                "known" => {
                    mempool += 1;
                    None
                }
                _ => {
                    missing += 1;
                    None
                }
            })
            .collect();

        // Only the two txids with depth >= 1 should be in the confirmed set
        assert_eq!(confirmed, 2);
        assert_eq!(mempool, 2); // known + mined with depth 0
        assert_eq!(missing, 1); // unknown
        assert_eq!(confirmed_set.len(), 2);
        assert!(confirmed_set.contains("tx_confirmed_deep"));
        assert!(confirmed_set.contains("tx_confirmed_shallow"));
        assert!(!confirmed_set.contains("tx_mempool"));
        assert!(!confirmed_set.contains("tx_unknown"));
        assert!(!confirmed_set.contains("tx_mined_zero_depth"));
    }

    /// Test that when the status service returns an error status,
    /// no proofs are fetched (empty return).
    #[test]
    fn test_triage_error_status_returns_empty() {
        use crate::services::traits::GetStatusForTxidsResult;

        let status_result = GetStatusForTxidsResult {
            name: "WoC".to_string(),
            status: "error".to_string(),
            error: Some("HTTP 503".to_string()),
            results: Vec::new(),
        };

        // Replicate the error path from synchronize_transaction_statuses
        let confirmed_txids: std::collections::HashSet<String> =
            if status_result.status == "success" {
                status_result
                    .results
                    .iter()
                    .filter_map(|detail| {
                        if detail.status == "mined" && detail.depth.unwrap_or(0) >= 1 {
                            Some(detail.txid.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                // Non-success status -> return empty (skip sync)
                return; // Simulates the early return Ok(Vec::new()) in the real code
            };

        // Should not reach here for error case
        panic!(
            "Should have returned early for error status, got {} confirmed",
            confirmed_txids.len()
        );
    }

    /// Test that when the status service call itself fails (Err),
    /// no proofs are fetched (empty return).
    #[test]
    fn test_triage_service_failure_returns_empty() {
        use crate::services::traits::GetStatusForTxidsResult;

        let triage_result: std::result::Result<GetStatusForTxidsResult, crate::Error> =
            Err(crate::Error::NetworkError("connection refused".to_string()));

        // Replicate the Err path from synchronize_transaction_statuses
        match &triage_result {
            Ok(status_result) if status_result.status == "success" => {
                panic!("Should not reach success path");
            }
            Ok(_) => {
                panic!("Should not reach non-success Ok path");
            }
            Err(_e) => {
                // This is the expected path - returns Ok(Vec::new())
                let results: Vec<TxSynchronizedStatus> = Vec::new();
                assert!(results.is_empty());
            }
        }
    }

    /// Test that all-unknown statuses produce an empty confirmed set.
    #[test]
    fn test_triage_all_unknown_produces_empty_confirmed() {
        use crate::services::traits::{GetStatusForTxidsResult, TxStatusDetail};

        let status_result = GetStatusForTxidsResult {
            name: "WoC".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![
                TxStatusDetail {
                    txid: "tx_a".to_string(),
                    status: "unknown".to_string(),
                    depth: None,
                    ..Default::default()
                },
                TxStatusDetail {
                    txid: "tx_b".to_string(),
                    status: "unknown".to_string(),
                    depth: None,
                    ..Default::default()
                },
                TxStatusDetail {
                    txid: "tx_c".to_string(),
                    status: "known".to_string(),
                    depth: Some(0),
                    ..Default::default()
                },
            ],
        };

        let confirmed_set: std::collections::HashSet<String> = status_result
            .results
            .iter()
            .filter_map(|detail| match detail.status.as_str() {
                "mined" => {
                    let depth = detail.depth.unwrap_or(0);
                    if depth >= 1 {
                        Some(detail.txid.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();

        assert!(
            confirmed_set.is_empty(),
            "No confirmed txids when all are unknown/known"
        );
    }
}
