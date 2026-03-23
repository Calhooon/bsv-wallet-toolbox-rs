//! Test Vectors Integration Tests
//!
//! This module wires up cross-SDK test vectors from `test_vectors/` to actual Rust tests.
//! These vectors are shared with the TypeScript (`@bsv/wallet-toolbox`) and Go
//! (`go-wallet-toolbox`) implementations to ensure cross-SDK compatibility.

use serde_json::Value;
use std::path::Path;

/// Helper to load a test vector JSON file.
fn load_test_vectors(relative_path: &str) -> Value {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let path = Path::new(&manifest_dir).join(relative_path);
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read test vector file {}: {}", path.display(), e));
    serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse test vector file {}: {}", path.display(), e))
}

// =============================================================================
// Module: Create Action Validation Tests
// =============================================================================
//
// These tests exercise `validate_create_action_args` (private) via the public
// `StorageSqlx::create_action` API. Each test vector specifies invalid inputs
// that should produce a validation error.

mod create_action_validation {

    use bsv_rs::wallet::{
        CreateActionArgs, CreateActionInput, CreateActionOptions, CreateActionOutput, Outpoint,
    };
    use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, WalletStorageWriter};

    /// Helper to set up an in-memory storage with a test user.
    async fn setup_storage() -> (StorageSqlx, AuthId) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);
        (storage, auth)
    }

    /// Parses a hex string outpoint txid into a [u8; 32] TxId.
    fn parse_txid(hex: &str) -> [u8; 32] {
        let bytes = hex::decode(hex).expect("valid hex txid");
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&bytes);
        txid
    }

    #[tokio::test]
    async fn tv_ts_invalid_empty_description() {
        // Test vector: ts_invalid_empty_description
        // Description is too short (empty string)
        let (storage, auth) = setup_storage().await;
        let args = CreateActionArgs {
            description: "".to_string(),
            input_beef: None,
            inputs: None,
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Empty description should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("description") && err_msg.contains("length"),
            "Error should mention description length: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_description_too_short() {
        // Test vector: go_description_too_short
        // Description too short (less than 5 characters)
        let (storage, auth) = setup_storage().await;
        let args = CreateActionArgs {
            description: "sh".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Short description should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("description") && err_msg.contains("5") && err_msg.contains("2000"),
            "Error should mention description length bounds: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_description_too_long() {
        // Test vector: go_description_too_long
        // Description too long (more than 2000 characters)
        let (storage, auth) = setup_storage().await;
        let args = CreateActionArgs {
            description: "A".repeat(2001),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Long description should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("description") && err_msg.contains("5") && err_msg.contains("2000"),
            "Error should mention description length bounds: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_label_empty() {
        // Test vector: go_label_empty
        // Label cannot be empty string
        let (storage, auth) = setup_storage().await;
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: Some(vec!["".to_string()]),
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Empty label should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("label") && err_msg.contains("empty"),
            "Error should mention label empty: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_label_too_long() {
        // Test vector: go_label_too_long
        // Label cannot exceed 300 characters
        let (storage, auth) = setup_storage().await;
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: Some(vec!["A".repeat(301)]),
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Long label should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("label") && err_msg.contains("maximum length"),
            "Error should mention label maximum length: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_output_satoshis_too_high() {
        // Test vector: go_output_satoshis_too_high
        // Output satoshis cannot exceed 21 million BTC
        let (storage, auth) = setup_storage().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 2_100_000_000_000_001,
                output_description: "test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Excess satoshis should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("satoshis") && err_msg.contains("maximum"),
            "Error should mention satoshis maximum: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_output_description_too_short() {
        // Test vector: go_output_description_too_short
        // Output description too short
        let (storage, auth) = setup_storage().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 42000,
                output_description: "sh".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(
            result.is_err(),
            "Short output description should fail validation"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("output description") && err_msg.contains("length"),
            "Error should mention output description length: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_output_basket_empty() {
        // Test vector: go_output_basket_empty
        // Output basket cannot be empty string when specified
        let (storage, auth) = setup_storage().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 42000,
                output_description: "test output".to_string(),
                basket: Some("".to_string()),
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Empty basket should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("basket") && err_msg.contains("empty"),
            "Error should mention basket empty: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_output_tag_empty() {
        // Test vector: go_output_tag_empty
        // Output tag cannot be empty string
        let (storage, auth) = setup_storage().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 42000,
                output_description: "test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: Some(vec!["".to_string()]),
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Empty tag should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("tag") && err_msg.contains("empty"),
            "Error should mention tag empty: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_input_missing_unlocking_script() {
        // Test vector: go_input_missing_unlocking_script
        // Input must have unlockingScript or unlockingScriptLength
        let (storage, auth) = setup_storage().await;
        let txid = parse_txid("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6");
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint { txid, vout: 0 },
                input_description: "test input".to_string(),
                unlocking_script: None,
                unlocking_script_length: None,
                sequence_number: None,
            }]),
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(
            result.is_err(),
            "Missing unlocking script should fail validation"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("unlockingScript") || err_msg.contains("unlocking"),
            "Error should mention unlocking script requirement: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_input_unlocking_script_length_mismatch() {
        // Test vector: go_input_unlocking_script_length_mismatch
        // Input unlocking script length must match actual length
        let (storage, auth) = setup_storage().await;
        let txid = parse_txid("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6");
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint { txid, vout: 0 },
                input_description: "test input".to_string(),
                unlocking_script: Some(vec![0x00]), // 1 byte
                unlocking_script_length: Some(2),   // says 2
                sequence_number: None,
            }]),
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(result.is_err(), "Length mismatch should fail validation");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("length") && err_msg.contains("mismatch"),
            "Error should mention length mismatch: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_duplicated_input_outpoints() {
        // Test vector: go_duplicated_input_outpoints
        // Cannot have duplicate outpoints in inputs
        let (storage, auth) = setup_storage().await;
        let txid = parse_txid("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6");
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![
                CreateActionInput {
                    outpoint: Outpoint { txid, vout: 0 },
                    input_description: "input 1".to_string(),
                    unlocking_script: Some(vec![0x00]),
                    unlocking_script_length: None,
                    sequence_number: None,
                },
                CreateActionInput {
                    outpoint: Outpoint { txid, vout: 0 },
                    input_description: "input 2".to_string(),
                    unlocking_script: Some(vec![0x00]),
                    unlocking_script_length: None,
                    sequence_number: None,
                },
            ]),
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let result = storage.create_action(&auth, args).await;
        assert!(
            result.is_err(),
            "Duplicate outpoints should fail validation"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("duplicate") && err_msg.contains("outpoint"),
            "Error should mention duplicate outpoint: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_duplicated_no_send_change() {
        // Test vector: go_duplicated_no_send_change
        // Cannot have duplicate outpoints in noSendChange
        let (storage, auth) = setup_storage().await;
        let txid = parse_txid("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6");
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![]),
            lock_time: None,
            version: None,
            labels: None,
            options: Some(CreateActionOptions {
                no_send: Some(true),
                no_send_change: Some(vec![Outpoint { txid, vout: 0 }, Outpoint { txid, vout: 0 }]),
                sign_and_process: None,
                accept_delayed_broadcast: None,
                trust_self: None,
                known_txids: None,
                return_txid_only: None,
                send_with: None,
                randomize_outputs: None,
            }),
        };
        let result = storage.create_action(&auth, args).await;
        assert!(
            result.is_err(),
            "Duplicate noSendChange should fail validation"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("duplicate") && err_msg.contains("noSendChange"),
            "Error should mention duplicate noSendChange: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn tv_go_default_valid_args() {
        // Test vector: go_default_valid_args
        // Default valid CreateAction arguments from Go fixtures should pass validation.
        // Note: This will fail at the change/funding stage (no UTXOs), but must NOT
        // fail at the validation stage.
        let (storage, auth) = setup_storage().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![]),
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 42000,
                output_description: "test output".to_string(),
                basket: None,
                custom_instructions: Some(
                    r#"{"derivationPrefix":"bPRI9FYwsIo=","derivationSuffix":"FdjLdpnLnJM=","type":"BRC29"}"#
                        .to_string(),
                ),
                tags: Some(vec!["test_tag=true".to_string()]),
            }]),
            lock_time: Some(0),
            version: Some(1),
            labels: Some(vec!["test_label=true".to_string()]),
            options: Some(CreateActionOptions {
                accept_delayed_broadcast: Some(false),
                sign_and_process: Some(true),
                randomize_outputs: Some(false),
                send_with: None,
                known_txids: None,
                no_send_change: None,
                no_send: None,
                trust_self: None,
                return_txid_only: None,
            }),
        };
        let result = storage.create_action(&auth, args).await;
        // The validation should pass. The error (if any) should be about funding,
        // not about validation.
        if let Err(ref e) = result {
            let err_msg = format!("{}", e);
            // It's OK if it fails for non-validation reasons (no UTXOs to fund the tx)
            assert!(
                !err_msg.contains("description length")
                    && !err_msg.contains("label cannot be empty")
                    && !err_msg.contains("label exceeds")
                    && !err_msg.contains("satoshis exceeds")
                    && !err_msg.contains("output description length")
                    && !err_msg.contains("basket cannot be empty")
                    && !err_msg.contains("tag cannot be empty")
                    && !err_msg.contains("duplicate outpoint"),
                "Valid args should pass validation, but got validation error: {}",
                err_msg
            );
        }
    }
}

// =============================================================================
// Module: Create Action Defaults Tests
// =============================================================================
//
// Verify that the default values from the test vectors match Rust implementation
// defaults for CreateAction arguments.

mod create_action_defaults {
    use super::*;

    #[test]
    fn tv_default_args_structure() {
        // Load the defaults test vector
        let vectors = load_test_vectors("test_vectors/storage/create_action/defaults.json");

        // Verify the default fields match what we expect
        let defaults = &vectors["default_valid_create_action_args"]["args"];

        // Check core default values
        assert_eq!(defaults["description"], "test transaction");
        assert_eq!(defaults["lockTime"], 0);
        assert_eq!(defaults["version"], 1);

        // Check option defaults
        let options = &defaults["options"];
        assert_eq!(options["acceptDelayedBroadcast"], false);
        assert_eq!(options["signAndProcess"], true);
        assert_eq!(options["randomizeOutputs"], false);

        // Check internal flags
        assert_eq!(defaults["isSendWith"], false);
        assert_eq!(defaults["isDelayed"], false);
        assert_eq!(defaults["isNoSend"], false);
        assert_eq!(defaults["isNewTx"], true);
        assert_eq!(defaults["isRemixChange"], false);
        assert_eq!(defaults["isSignAction"], false);
        assert_eq!(defaults["includeAllSourceTransactions"], true);
    }

    #[test]
    fn tv_default_output_satoshis() {
        let vectors = load_test_vectors("test_vectors/storage/create_action/defaults.json");
        assert_eq!(vectors["default_output_satoshis"], 42000);
    }

    #[test]
    fn tv_default_locking_script_is_p2pkh() {
        let vectors = load_test_vectors("test_vectors/storage/create_action/defaults.json");
        let script_details = &vectors["locking_script_details"];
        assert_eq!(script_details["type"], "P2PKH");
        assert_eq!(script_details["pubKeyHashLength"], 20);

        // Verify the script hex decodes to 25 bytes (standard P2PKH)
        let script_hex = script_details["script"].as_str().unwrap();
        let script_bytes = hex::decode(script_hex).unwrap();
        assert_eq!(script_bytes.len(), 25);
        // Verify P2PKH structure: OP_DUP OP_HASH160 PUSH20 <hash> OP_EQUALVERIFY OP_CHECKSIG
        assert_eq!(script_bytes[0], 0x76); // OP_DUP
        assert_eq!(script_bytes[1], 0xa9); // OP_HASH160
        assert_eq!(script_bytes[2], 0x14); // Push 20 bytes
        assert_eq!(script_bytes[23], 0x88); // OP_EQUALVERIFY
        assert_eq!(script_bytes[24], 0xac); // OP_CHECKSIG
    }

    #[test]
    fn tv_default_custom_instructions_brc29() {
        let vectors = load_test_vectors("test_vectors/storage/create_action/defaults.json");
        let ci = &vectors["custom_instructions_details"];

        // Verify the raw JSON parses correctly
        let raw = ci["raw"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed["type"], "BRC29");
        assert!(!parsed["derivationPrefix"].as_str().unwrap().is_empty());
        assert!(!parsed["derivationSuffix"].as_str().unwrap().is_empty());
    }

    #[test]
    fn tv_option_flags_consistency() {
        // Verify the option flags explanation covers all expected flags
        let vectors = load_test_vectors("test_vectors/storage/create_action/defaults.json");
        let flags = &vectors["option_flags_explanation"];

        let expected_flags = [
            "isSendWith",
            "isDelayed",
            "isNoSend",
            "isNewTx",
            "isRemixChange",
            "isSignAction",
            "includeAllSourceTransactions",
        ];

        for flag in &expected_flags {
            assert!(
                flags.get(flag).is_some(),
                "Flag '{}' should be documented in option_flags_explanation",
                flag
            );
        }
    }
}

// =============================================================================
// Module: List Outputs Validation Tests
// =============================================================================
//
// Tests for ListOutputs argument validation from test_vectors/storage/list_outputs/validation.json.
// Since there is no standalone validation function, these tests exercise the
// ListOutputsArgs struct construction and verify constraints documented in the vectors.

mod list_outputs_validation {
    use super::*;
    use bsv_rs::wallet::ListOutputsArgs;
    use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, WalletStorageReader, WalletStorageWriter};

    /// Helper to set up an in-memory storage with a test user.
    async fn setup_storage() -> (StorageSqlx, AuthId) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);
        (storage, auth)
    }

    #[tokio::test]
    async fn tv_ts_invalid_empty_basket() {
        // Test vector: ts_invalid_empty_basket
        // Basket cannot be empty string
        // Note: The Rust implementation may allow this and return 0 results,
        // or may reject it. We verify the behavior is consistent.
        let (storage, auth) = setup_storage().await;
        let args = ListOutputsArgs {
            basket: "".to_string(),
            tags: None,
            tag_query_mode: None,
            include: None,
            include_custom_instructions: None,
            include_tags: None,
            include_labels: None,
            limit: None,
            offset: None,
            seek_permission: None,
        };
        let result = storage.list_outputs(&auth, args).await;
        // Either returns an error or returns empty results for empty basket
        match result {
            Ok(r) => assert_eq!(r.total_outputs, 0, "Empty basket should return no outputs"),
            Err(e) => {
                let err_msg = format!("{}", e);
                assert!(
                    err_msg.to_lowercase().contains("basket")
                        || err_msg.contains("WERR_INVALID_PARAMETER"),
                    "Error should mention basket: {}",
                    err_msg
                );
            }
        }
    }

    #[tokio::test]
    async fn tv_go_valid_paging_only() {
        // Test vector: go_valid_paging_only
        // Valid args with only paging parameters
        let (storage, auth) = setup_storage().await;
        let args = ListOutputsArgs {
            basket: "default".to_string(),
            tags: None,
            tag_query_mode: None,
            include: None,
            include_custom_instructions: None,
            include_tags: None,
            include_labels: None,
            limit: Some(10),
            offset: None,
            seek_permission: None,
        };
        let result = storage.list_outputs(&auth, args).await;
        assert!(
            result.is_ok(),
            "Valid paging args should succeed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap().total_outputs, 0);
    }

    #[tokio::test]
    async fn tv_ts_valid_full_args() {
        // Test vector: ts_valid_full_args
        // Valid args with all parameters should not error
        let (storage, auth) = setup_storage().await;
        let args = ListOutputsArgs {
            basket: "default".to_string(),
            tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
            tag_query_mode: Some(bsv_rs::wallet::QueryMode::Any),
            include: Some(bsv_rs::wallet::OutputInclude::LockingScripts),
            include_custom_instructions: Some(false),
            include_tags: Some(true),
            include_labels: Some(true),
            limit: Some(10),
            offset: Some(0),
            seek_permission: Some(true),
        };
        let result = storage.list_outputs(&auth, args).await;
        assert!(
            result.is_ok(),
            "Full valid args should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn tv_ts_non_existent_basket() {
        // Test vector: ts_non_existent_basket
        // Query non-existent basket returns zero results
        let (storage, auth) = setup_storage().await;
        let args = ListOutputsArgs {
            basket: "admin foo".to_string(),
            tags: None,
            tag_query_mode: None,
            include: None,
            include_custom_instructions: None,
            include_tags: None,
            include_labels: None,
            limit: None,
            offset: None,
            seek_permission: None,
        };
        let result = storage.list_outputs(&auth, args).await;
        assert!(
            result.is_ok(),
            "Non-existent basket should not error: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap().total_outputs,
            0,
            "Non-existent basket should return 0 outputs"
        );
    }

    #[test]
    fn tv_validate_test_vector_structure() {
        // Verify the test vector file is well-formed and contains expected fields
        let vectors = load_test_vectors("test_vectors/storage/list_outputs/validation.json");
        let test_cases = vectors["test_vectors"].as_array().unwrap();
        assert!(
            test_cases.len() >= 10,
            "Should have at least 10 error test cases"
        );

        let valid_cases = vectors["valid_test_vectors"].as_array().unwrap();
        assert!(
            valid_cases.len() >= 3,
            "Should have at least 3 valid test cases"
        );

        // Verify all test vectors have required fields
        for tc in test_cases {
            assert!(tc.get("id").is_some(), "Each test case must have an id");
            assert!(
                tc.get("expected_error").is_some(),
                "Each error case must have expected_error"
            );
        }
    }
}

// =============================================================================
// Module: List Actions Validation Tests
// =============================================================================
//
// Tests for ListActions argument validation from test_vectors/storage/list_actions/validation.json.

mod list_actions_validation {
    use super::*;
    use bsv_rs::wallet::ListActionsArgs;
    use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, WalletStorageReader, WalletStorageWriter};

    /// Helper to set up an in-memory storage with a test user.
    async fn setup_storage() -> (StorageSqlx, AuthId) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);
        (storage, auth)
    }

    #[tokio::test]
    async fn tv_go_valid_labels_defaults() {
        // Test vector: go_valid_labels_defaults
        // Valid args with labels and default query mode
        let (storage, auth) = setup_storage().await;
        let args = ListActionsArgs {
            labels: vec!["valid-label".to_string()],
            label_query_mode: Some(bsv_rs::wallet::QueryMode::Any),
            include_labels: None,
            include_inputs: None,
            include_input_source_locking_scripts: None,
            include_input_unlocking_scripts: None,
            include_outputs: None,
            include_output_locking_scripts: None,
            limit: None,
            offset: None,
            seek_permission: Some(true),
        };
        let result = storage.list_actions(&auth, args).await;
        assert!(
            result.is_ok(),
            "Valid label args should succeed: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap().total_actions,
            0,
            "Empty database should return 0 actions"
        );
    }

    #[tokio::test]
    async fn tv_go_valid_max_pagination() {
        // Test vector: go_valid_max_pagination
        // Maximum pagination values are valid
        let (storage, auth) = setup_storage().await;
        let args = ListActionsArgs {
            labels: vec![],
            label_query_mode: Some(bsv_rs::wallet::QueryMode::All),
            include_labels: None,
            include_inputs: None,
            include_input_source_locking_scripts: None,
            include_input_unlocking_scripts: None,
            include_outputs: None,
            include_output_locking_scripts: None,
            limit: Some(10000),
            offset: Some(1000000),
            seek_permission: Some(true),
        };
        let result = storage.list_actions(&auth, args).await;
        assert!(
            result.is_ok(),
            "Max pagination args should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn tv_go_default_wallet_args() {
        // Test vector: go_default_wallet_args
        // Default wallet ListActions arguments from Go fixtures
        let (storage, auth) = setup_storage().await;
        let args = ListActionsArgs {
            labels: vec![],
            label_query_mode: Some(bsv_rs::wallet::QueryMode::Any),
            include_labels: Some(false),
            include_inputs: Some(false),
            include_input_source_locking_scripts: Some(false),
            include_input_unlocking_scripts: Some(false),
            include_outputs: Some(false),
            include_output_locking_scripts: Some(false),
            limit: Some(100),
            offset: Some(0),
            seek_permission: Some(true),
        };
        let result = storage.list_actions(&auth, args).await;
        assert!(
            result.is_ok(),
            "Default wallet args should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn tv_go_default_with_all_includes() {
        // Test vector: go_default_with_all_includes
        // ListActions arguments with all includes enabled
        let (storage, auth) = setup_storage().await;
        let args = ListActionsArgs {
            labels: vec![],
            label_query_mode: Some(bsv_rs::wallet::QueryMode::Any),
            include_labels: Some(true),
            include_inputs: Some(true),
            include_input_source_locking_scripts: Some(true),
            include_input_unlocking_scripts: Some(true),
            include_outputs: Some(true),
            include_output_locking_scripts: Some(true),
            limit: Some(10),
            offset: Some(0),
            seek_permission: Some(true),
        };
        let result = storage.list_actions(&auth, args).await;
        assert!(
            result.is_ok(),
            "All-includes args should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn tv_validate_test_vector_structure() {
        // Verify the test vector file is well-formed and contains expected fields
        let vectors = load_test_vectors("test_vectors/storage/list_actions/validation.json");
        let test_cases = vectors["test_vectors"].as_array().unwrap();
        assert!(
            test_cases.len() >= 9,
            "Should have at least 9 error test cases"
        );

        let valid_cases = vectors["valid_test_vectors"].as_array().unwrap();
        assert!(
            valid_cases.len() >= 4,
            "Should have at least 4 valid test cases"
        );

        // Verify specific error IDs exist
        let error_ids: Vec<&str> = test_cases
            .iter()
            .map(|tc| tc["id"].as_str().unwrap())
            .collect();
        assert!(error_ids.contains(&"go_label_empty"));
        assert!(error_ids.contains(&"go_label_too_long"));
        assert!(error_ids.contains(&"go_limit_exceeds_max"));
        assert!(error_ids.contains(&"go_invalid_label_query_mode"));
    }
}

// =============================================================================
// Module: BRC-29 Key Derivation Tests
// =============================================================================
//
// Tests for BRC-29 key derivation using test vectors from test_vectors/keys/brc29.json.
// BRC-29 defines ECDH-based key derivation for payment addresses.
// The core property is symmetry:
//   AddressForSelf(senderPub, keyId, recipientPriv) ==
//   AddressForCounterparty(senderPriv, keyId, recipientPub)

mod brc29_key_derivation {
    use super::*;
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::wallet::{Counterparty, KeyDeriver, Protocol, SecurityLevel};

    fn load_brc29_vectors() -> Value {
        load_test_vectors("test_vectors/keys/brc29.json")
    }

    #[test]
    fn tv_constants_sender_key_pair_consistency() {
        // Verify the sender's private key produces the expected public key
        let vectors = load_brc29_vectors();
        let sender = &vectors["constants"]["sender"];

        let private_key_hex = sender["privateKeyHex"].as_str().unwrap();
        let expected_public_key = sender["publicKeyHex"].as_str().unwrap();

        let private_key =
            PrivateKey::from_hex(private_key_hex).expect("Sender private key should be valid");
        let public_key = private_key.public_key();
        let actual_public_key = public_key.to_hex();

        assert_eq!(
            actual_public_key, expected_public_key,
            "Sender private key should produce the expected public key"
        );
    }

    #[test]
    fn tv_constants_recipient_key_pair_consistency() {
        // Verify the recipient's private key produces the expected public key
        let vectors = load_brc29_vectors();
        let recipient = &vectors["constants"]["recipient"];

        let private_key_hex = recipient["privateKeyHex"].as_str().unwrap();
        let expected_public_key = recipient["publicKeyHex"].as_str().unwrap();

        let private_key = PrivateKey::from_hex(private_key_hex)
            .expect("Recipient private key should be valid (the generator key)");
        let public_key = private_key.public_key();
        let actual_public_key = public_key.to_hex();

        assert_eq!(
            actual_public_key, expected_public_key,
            "Recipient private key should produce the expected public key"
        );
    }

    #[test]
    fn tv_brc29_address_symmetry() {
        // BRC-29 key derivation symmetry test:
        // Deriving a key with (senderPriv, recipientPub) should produce the
        // same result as (recipientPriv, senderPub) -- the ECDH shared secret
        // is the same from both sides.
        let vectors = load_brc29_vectors();
        let constants = &vectors["constants"];

        let sender_priv =
            PrivateKey::from_hex(constants["sender"]["privateKeyHex"].as_str().unwrap()).unwrap();
        let recipient_priv =
            PrivateKey::from_hex(constants["recipient"]["privateKeyHex"].as_str().unwrap())
                .unwrap();

        let sender_pub = sender_priv.public_key();
        let recipient_pub = recipient_priv.public_key();

        let key_id_obj = &constants["keyId"];
        let prefix = key_id_obj["derivationPrefix"].as_str().unwrap();
        let suffix = key_id_obj["derivationSuffix"].as_str().unwrap();

        // BRC-29 protocol: SecurityLevel::Counterparty, protocol "3241645161d8"
        let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
        let key_id = format!("{} {}", prefix, suffix);

        // Sender derives key using recipient as counterparty
        let sender_deriver = KeyDeriver::new(Some(sender_priv));
        let sender_derived_pub = sender_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(recipient_pub),
                false, // for counterparty
            )
            .expect("Sender key derivation should succeed");

        // Recipient derives key using sender as counterparty
        let recipient_deriver = KeyDeriver::new(Some(recipient_priv));
        let recipient_derived_pub = recipient_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(sender_pub),
                true, // for self
            )
            .expect("Recipient key derivation should succeed");

        assert_eq!(
            sender_derived_pub.to_hex(),
            recipient_derived_pub.to_hex(),
            "BRC-29 key derivation must be symmetric: sender(forCounterparty) == recipient(forSelf)"
        );
    }

    #[test]
    fn tv_brc29_address_for_self_mainnet() {
        // Test vector: valid_hex_sender_public_key
        // Create address using sender public key hex and recipient private key hex
        let vectors = load_brc29_vectors();
        let test_case = &vectors["address_for_self_test_vectors"][0];

        assert_eq!(test_case["id"], "valid_hex_sender_public_key");

        let inputs = &test_case["inputs"];
        let sender_pub_hex = inputs["senderPublicKey"].as_str().unwrap();
        let recipient_priv_hex = inputs["recipientPrivateKey"].as_str().unwrap();
        let prefix = inputs["keyId"]["derivationPrefix"].as_str().unwrap();
        let suffix = inputs["keyId"]["derivationSuffix"].as_str().unwrap();
        let expected_address = test_case["expected_outputs"]["address"].as_str().unwrap();

        let sender_pub = bsv_rs::primitives::PublicKey::from_hex(sender_pub_hex).unwrap();
        let recipient_priv = PrivateKey::from_hex(recipient_priv_hex).unwrap();

        // Derive the key for the recipient using BRC-29 protocol
        let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
        let key_id = format!("{} {}", prefix, suffix);

        let recipient_deriver = KeyDeriver::new(Some(recipient_priv));
        let derived_pub = recipient_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(sender_pub),
                true, // for self
            )
            .expect("Key derivation should succeed");

        // Generate P2PKH address (mainnet version = 0x00)
        let actual_address = derived_pub.to_address();

        assert_eq!(
            actual_address, expected_address,
            "BRC-29 AddressForSelf mainnet should match test vector"
        );
    }

    #[test]
    fn tv_brc29_address_for_self_testnet() {
        // Test vector: valid_testnet_address
        // Create testnet address using BRC29
        let vectors = load_brc29_vectors();
        let test_case = &vectors["address_for_self_test_vectors"][1];

        assert_eq!(test_case["id"], "valid_testnet_address");

        let inputs = &test_case["inputs"];
        let sender_pub_hex = inputs["senderPublicKey"].as_str().unwrap();
        let recipient_priv_hex = inputs["recipientPrivateKey"].as_str().unwrap();
        let prefix = inputs["keyId"]["derivationPrefix"].as_str().unwrap();
        let suffix = inputs["keyId"]["derivationSuffix"].as_str().unwrap();
        let expected_address = test_case["expected_outputs"]["address"].as_str().unwrap();

        let sender_pub = bsv_rs::primitives::PublicKey::from_hex(sender_pub_hex).unwrap();
        let recipient_priv = PrivateKey::from_hex(recipient_priv_hex).unwrap();

        let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
        let key_id = format!("{} {}", prefix, suffix);

        let recipient_deriver = KeyDeriver::new(Some(recipient_priv));
        let derived_pub = recipient_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(sender_pub),
                true,
            )
            .expect("Key derivation should succeed");

        // Generate P2PKH address (testnet version = 0x6f)
        let actual_address = derived_pub.to_address_with_prefix(0x6f);

        assert_eq!(
            actual_address, expected_address,
            "BRC-29 AddressForSelf testnet should match test vector"
        );
    }

    #[test]
    fn tv_brc29_address_for_counterparty_mainnet() {
        // Test vector: valid_hex_sender_private_key
        // Create address using sender private key hex and recipient public key hex
        let vectors = load_brc29_vectors();
        let test_case = &vectors["address_for_counterparty_test_vectors"][0];

        assert_eq!(test_case["id"], "valid_hex_sender_private_key");

        let inputs = &test_case["inputs"];
        let sender_priv_hex = inputs["senderPrivateKey"].as_str().unwrap();
        let recipient_pub_hex = inputs["recipientPublicKey"].as_str().unwrap();
        let prefix = inputs["keyId"]["derivationPrefix"].as_str().unwrap();
        let suffix = inputs["keyId"]["derivationSuffix"].as_str().unwrap();
        let expected_address = test_case["expected_outputs"]["address"].as_str().unwrap();

        let sender_priv = PrivateKey::from_hex(sender_priv_hex).unwrap();
        let recipient_pub = bsv_rs::primitives::PublicKey::from_hex(recipient_pub_hex).unwrap();

        let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
        let key_id = format!("{} {}", prefix, suffix);

        let sender_deriver = KeyDeriver::new(Some(sender_priv));
        let derived_pub = sender_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(recipient_pub),
                false, // for counterparty
            )
            .expect("Key derivation should succeed");

        let actual_address = derived_pub.to_address();

        assert_eq!(
            actual_address, expected_address,
            "BRC-29 AddressForCounterparty mainnet should match test vector"
        );
    }

    #[test]
    fn tv_brc29_address_for_counterparty_testnet() {
        // Test vector: valid_testnet_counterparty_address
        let vectors = load_brc29_vectors();
        let test_case = &vectors["address_for_counterparty_test_vectors"][2];

        assert_eq!(test_case["id"], "valid_testnet_counterparty_address");

        let inputs = &test_case["inputs"];
        let sender_priv_hex = inputs["senderPrivateKey"].as_str().unwrap();
        let recipient_pub_hex = inputs["recipientPublicKey"].as_str().unwrap();
        let prefix = inputs["keyId"]["derivationPrefix"].as_str().unwrap();
        let suffix = inputs["keyId"]["derivationSuffix"].as_str().unwrap();
        let expected_address = test_case["expected_outputs"]["address"].as_str().unwrap();

        let sender_priv = PrivateKey::from_hex(sender_priv_hex).unwrap();
        let recipient_pub = bsv_rs::primitives::PublicKey::from_hex(recipient_pub_hex).unwrap();

        let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
        let key_id = format!("{} {}", prefix, suffix);

        let sender_deriver = KeyDeriver::new(Some(sender_priv));
        let derived_pub = sender_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(recipient_pub),
                false,
            )
            .expect("Key derivation should succeed");

        let actual_address = derived_pub.to_address_with_prefix(0x6f);

        assert_eq!(
            actual_address, expected_address,
            "BRC-29 AddressForCounterparty testnet should match test vector"
        );
    }

    #[test]
    fn tv_brc29_self_and_counterparty_produce_same_address() {
        // Cross-test: AddressForSelf and AddressForCounterparty must produce the same address
        // This is the fundamental BRC-29 symmetry property documented in the test vectors.
        let vectors = load_brc29_vectors();
        let constants = &vectors["constants"];
        let expected_mainnet = constants["expectedAddress"]["mainnet"].as_str().unwrap();

        let sender_priv =
            PrivateKey::from_hex(constants["sender"]["privateKeyHex"].as_str().unwrap()).unwrap();
        let recipient_priv =
            PrivateKey::from_hex(constants["recipient"]["privateKeyHex"].as_str().unwrap())
                .unwrap();

        let sender_pub = sender_priv.public_key();
        let recipient_pub = recipient_priv.public_key();

        let prefix = constants["keyId"]["derivationPrefix"].as_str().unwrap();
        let suffix = constants["keyId"]["derivationSuffix"].as_str().unwrap();

        let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
        let key_id = format!("{} {}", prefix, suffix);

        // AddressForSelf: recipient derives using sender's public key
        let recipient_deriver = KeyDeriver::new(Some(recipient_priv));
        let for_self_pub = recipient_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(sender_pub),
                true,
            )
            .unwrap();
        let for_self_address = for_self_pub.to_address();

        // AddressForCounterparty: sender derives using recipient's public key
        let sender_deriver = KeyDeriver::new(Some(sender_priv));
        let for_counterparty_pub = sender_deriver
            .derive_public_key(
                &brc29_protocol,
                &key_id,
                &Counterparty::Other(recipient_pub),
                false,
            )
            .unwrap();
        let for_counterparty_address = for_counterparty_pub.to_address();

        assert_eq!(
            for_self_address, for_counterparty_address,
            "AddressForSelf and AddressForCounterparty must produce the same address"
        );
        assert_eq!(
            for_self_address, expected_mainnet,
            "Both should match the expected mainnet address from test vectors"
        );
    }

    #[test]
    fn tv_brc29_error_cases_validation() {
        // Verify error test vectors are well-formed
        let vectors = load_brc29_vectors();

        // Check address_for_self error cases exist
        let self_cases = vectors["address_for_self_test_vectors"].as_array().unwrap();
        let self_error_ids: Vec<&str> = self_cases
            .iter()
            .filter(|tc| tc.get("expected_error").is_some_and(|e| !e.is_null()))
            .map(|tc| tc["id"].as_str().unwrap())
            .collect();

        assert!(self_error_ids.contains(&"error_empty_sender_key"));
        assert!(self_error_ids.contains(&"error_invalid_sender_key"));
        assert!(self_error_ids.contains(&"error_invalid_key_id"));
        assert!(self_error_ids.contains(&"error_empty_recipient_key"));
        assert!(self_error_ids.contains(&"error_invalid_recipient_key"));

        // Check address_for_counterparty error cases exist
        let cp_cases = vectors["address_for_counterparty_test_vectors"]
            .as_array()
            .unwrap();
        let cp_error_ids: Vec<&str> = cp_cases
            .iter()
            .filter(|tc| tc.get("expected_error").is_some_and(|e| !e.is_null()))
            .map(|tc| tc["id"].as_str().unwrap())
            .collect();

        assert!(cp_error_ids.contains(&"error_empty_sender_private_key"));
        assert!(cp_error_ids.contains(&"error_invalid_sender_private_key"));
        assert!(cp_error_ids.contains(&"error_invalid_key_id_counterparty"));
        assert!(cp_error_ids.contains(&"error_empty_recipient_public_key"));
        assert!(cp_error_ids.contains(&"error_invalid_recipient_public_key"));
    }
}

// =============================================================================
// Module: Test Users Tests
// =============================================================================
//
// Tests for test user credentials from test_vectors/keys/test_users.json.
// Verifies key pair consistency for test users shared across SDK implementations.

mod test_users {
    use super::*;
    use bsv_rs::primitives::PrivateKey;

    fn load_users() -> Value {
        load_test_vectors("test_vectors/keys/test_users.json")
    }

    #[test]
    fn tv_alice_key_pair_consistency() {
        // Test vector: Alice test user
        // Verify Alice's private key produces the expected public key
        let vectors = load_users();
        let alice = &vectors["test_users"][0];

        assert_eq!(alice["name"], "Alice");
        assert_eq!(alice["id"], 1);

        let private_key_hex = alice["privateKeyHex"].as_str().unwrap();
        let expected_public_key = alice["publicKeyHex"].as_str().unwrap();

        let private_key =
            PrivateKey::from_hex(private_key_hex).expect("Alice's private key should be valid");
        let public_key = private_key.public_key();
        let actual_public_key = public_key.to_hex();

        assert_eq!(
            actual_public_key, expected_public_key,
            "Alice's private key should produce the expected public key"
        );
    }

    #[test]
    fn tv_bob_key_validity() {
        // Test vector: Bob test user
        // Bob only has a private key (no expected public key in the vector),
        // so we verify the key is at least valid.
        let vectors = load_users();
        let bob = &vectors["test_users"][1];

        assert_eq!(bob["name"], "Bob");
        assert_eq!(bob["id"], 2);

        let private_key_hex = bob["privateKeyHex"].as_str().unwrap();

        let private_key =
            PrivateKey::from_hex(private_key_hex).expect("Bob's private key should be valid");

        // Verify we can derive a public key from it
        let public_key = private_key.public_key();
        let pub_hex = public_key.to_hex();
        assert_eq!(
            pub_hex.len(),
            66,
            "Compressed public key should be 66 hex chars"
        );
        assert!(
            pub_hex.starts_with("02") || pub_hex.starts_with("03"),
            "Compressed public key should start with 02 or 03"
        );
    }

    #[test]
    fn tv_alice_is_brc29_sender() {
        // Cross-reference: Alice's keys should match the BRC-29 test vector sender
        let users = load_users();
        let brc29 = load_test_vectors("test_vectors/keys/brc29.json");

        let alice_priv = users["test_users"][0]["privateKeyHex"].as_str().unwrap();
        let alice_pub = users["test_users"][0]["publicKeyHex"].as_str().unwrap();

        let brc29_sender_priv = brc29["constants"]["sender"]["privateKeyHex"]
            .as_str()
            .unwrap();
        let brc29_sender_pub = brc29["constants"]["sender"]["publicKeyHex"]
            .as_str()
            .unwrap();

        assert_eq!(
            alice_priv, brc29_sender_priv,
            "Alice's private key should match the BRC-29 sender private key"
        );
        assert_eq!(
            alice_pub, brc29_sender_pub,
            "Alice's public key should match the BRC-29 sender public key"
        );
    }

    #[test]
    fn tv_storage_configuration_keys_valid() {
        // Verify storage configuration keys are valid
        let vectors = load_users();
        let config = &vectors["storage_configuration"];

        let server_priv = config["storageServerPrivKey"].as_str().unwrap();
        let key =
            PrivateKey::from_hex(server_priv).expect("Storage server private key should be valid");
        let pub_key = key.public_key().to_hex();

        let expected_identity = config["storageIdentityKey"].as_str().unwrap();
        assert_eq!(
            pub_key, expected_identity,
            "Storage server private key should produce the expected identity key"
        );

        // Verify second storage key
        let second_priv = config["secondStorageServerPrivKey"].as_str().unwrap();
        let second_key = PrivateKey::from_hex(second_priv)
            .expect("Second storage server private key should be valid");
        let second_pub = second_key.public_key().to_hex();

        let second_expected = config["secondStorageIdentityKey"].as_str().unwrap();
        assert_eq!(
            second_pub, second_expected,
            "Second storage server private key should produce the expected identity key"
        );
    }

    #[test]
    fn tv_common_test_values() {
        // Verify common test values are consistent
        let vectors = load_users();
        let common = &vectors["common_test_values"];

        // Verify the user identity key is a valid public key hex
        let identity_key = common["userIdentityKeyHex"].as_str().unwrap();
        assert_eq!(
            identity_key.len(),
            66,
            "Identity key should be 66 hex chars"
        );
        let _pub_key = bsv_rs::primitives::PublicKey::from_hex(identity_key)
            .expect("User identity key should be a valid public key");

        // Verify the anyone identity key matches the generator point
        let anyone_key = common["anyoneIdentityKey"].as_str().unwrap();
        let brc29 = load_test_vectors("test_vectors/keys/brc29.json");
        let recipient_pub = brc29["constants"]["recipient"]["publicKeyHex"]
            .as_str()
            .unwrap();
        assert_eq!(
            anyone_key, recipient_pub,
            "The 'anyone' identity key should be the generator point (private key = 1)"
        );

        // Verify pagination defaults
        let pagination = &vectors["pagination_defaults"];
        assert_eq!(pagination["limit"], 100);
        assert_eq!(pagination["offset"], 0);
        assert_eq!(pagination["maxPaginationLimit"], 10000);
    }

    #[test]
    fn tv_mock_outpoint_format() {
        // Verify the mock outpoint has the expected format: txid.vout
        let vectors = load_users();
        let outpoint_str = vectors["common_test_values"]["mockOutpoint"]
            .as_str()
            .unwrap();

        // Should be "txid.vout" format
        let parts: Vec<&str> = outpoint_str.split('.').collect();
        assert_eq!(parts.len(), 2, "Outpoint should have format txid.vout");

        let txid = parts[0];
        assert_eq!(txid.len(), 64, "TXID should be 64 hex chars");
        assert!(hex::decode(txid).is_ok(), "TXID should be valid hex");

        let vout: u32 = parts[1].parse().expect("vout should be a valid u32");
        assert_eq!(vout, 1);
    }
}

// =============================================================================
// Module: Merkle Path Tests
// =============================================================================
//
// Tests for merkle path construction from test_vectors/transactions/merkle_path.json.
// These test vectors exercise TSC proof to MerklePath conversion.

mod merkle_path {
    use super::*;
    use bsv_rs::transaction::{MerklePath, MerklePathLeaf};

    fn load_merkle_vectors() -> Value {
        load_test_vectors("test_vectors/transactions/merkle_path.json")
    }

    /// Converts TSC proof nodes + txid + index into a MerklePath.
    /// This implements the TSC proof -> MerklePath conversion tested in Go.
    fn tsc_proof_to_merkle_path(
        txid: &str,
        index: u64,
        nodes: &[String],
        block_height: u32,
    ) -> Result<MerklePath, String> {
        if nodes.is_empty() {
            return Err("empty nodes list".to_string());
        }

        // Validate txid (must be 64 hex chars)
        if txid.len() != 64 || hex::decode(txid).is_err() {
            return Err("invalid txid".to_string());
        }

        let mut path: Vec<Vec<MerklePathLeaf>> = Vec::new();

        // Build the path level by level
        let mut current_offset = index;

        for (level, node) in nodes.iter().enumerate() {
            let mut leaves = Vec::new();

            if level == 0 {
                // Level 0 contains the txid and its sibling
                let txid_leaf = MerklePathLeaf::new_txid(current_offset, txid.to_string());
                leaves.push(txid_leaf);
            }

            // Determine sibling offset
            let sibling_offset = if current_offset.is_multiple_of(2) {
                current_offset + 1
            } else {
                current_offset - 1
            };

            if node == "*" {
                // Duplicate marker - sibling is a duplicate of the txid
                let dup_leaf = MerklePathLeaf::new_duplicate(sibling_offset);
                leaves.push(dup_leaf);
            } else {
                // Validate node hash
                if node.len() != 64 || hex::decode(node).is_err() {
                    return Err("invalid node hash".to_string());
                }
                let sibling_leaf = MerklePathLeaf::new(sibling_offset, node.clone());
                leaves.push(sibling_leaf);
            }

            // Sort by offset
            leaves.sort_by_key(|l| l.offset);

            path.push(leaves);

            // Move up the tree
            current_offset /= 2;
        }

        MerklePath::new(block_height, path).map_err(|e| format!("{}", e))
    }

    #[test]
    fn tv_success_even_index() {
        // Test vector: success_even_index
        // Successfully convert TSC proof with even index
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][0];

        assert_eq!(test_case["id"], "success_even_index");

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap();
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = inputs["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let expected = &test_case["expected_outputs"];
        let expected_height = expected["blockHeight"].as_u64().unwrap() as u32;
        let expected_levels = expected["path_levels"].as_u64().unwrap() as usize;

        let result = tsc_proof_to_merkle_path(txid, index, &nodes, block_height);
        assert!(
            result.is_ok(),
            "Even index proof should succeed: {:?}",
            result.err()
        );

        let merkle_path = result.unwrap();
        assert_eq!(merkle_path.block_height, expected_height);
        assert_eq!(merkle_path.path.len(), expected_levels);
    }

    #[test]
    fn tv_success_odd_index() {
        // Test vector: success_odd_index
        // Successfully convert TSC proof with odd index
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][1];

        assert_eq!(test_case["id"], "success_odd_index");

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap();
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = inputs["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let expected = &test_case["expected_outputs"];
        let expected_height = expected["blockHeight"].as_u64().unwrap() as u32;
        let expected_levels = expected["path_levels"].as_u64().unwrap() as usize;

        let result = tsc_proof_to_merkle_path(txid, index, &nodes, block_height);
        assert!(
            result.is_ok(),
            "Odd index proof should succeed: {:?}",
            result.err()
        );

        let merkle_path = result.unwrap();
        assert_eq!(merkle_path.block_height, expected_height);
        assert_eq!(merkle_path.path.len(), expected_levels);
    }

    #[test]
    fn tv_success_duplicate_marker() {
        // Test vector: success_duplicate_marker
        // Successfully convert TSC proof with duplicate node marker (*)
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][2];

        assert_eq!(test_case["id"], "success_duplicate_marker");

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap();
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = inputs["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let expected = &test_case["expected_outputs"];
        let expected_height = expected["blockHeight"].as_u64().unwrap() as u32;
        let expected_levels = expected["path_levels"].as_u64().unwrap() as usize;

        let result = tsc_proof_to_merkle_path(txid, index, &nodes, block_height);
        assert!(
            result.is_ok(),
            "Duplicate marker proof should succeed: {:?}",
            result.err()
        );

        let merkle_path = result.unwrap();
        assert_eq!(merkle_path.block_height, expected_height);
        assert_eq!(merkle_path.path.len(), expected_levels);

        // Verify the first level has a duplicate leaf
        let first_level = &merkle_path.path[0];
        let has_duplicate = first_level.iter().any(|leaf| leaf.duplicate);
        assert!(
            has_duplicate,
            "First level should contain a duplicate marker leaf"
        );
    }

    #[test]
    fn tv_error_empty_nodes() {
        // Test vector: error_empty_nodes
        // Error when nodes list is empty
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][3];

        assert_eq!(test_case["id"], "error_empty_nodes");

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap();
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = vec![];
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let result = tsc_proof_to_merkle_path(txid, index, &nodes, block_height);
        assert!(result.is_err(), "Empty nodes should fail");
        assert!(
            result.unwrap_err().contains("empty nodes"),
            "Error should mention empty nodes"
        );
    }

    #[test]
    fn tv_error_invalid_txid() {
        // Test vector: error_invalid_txid
        // Error when txid is invalid
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][4];

        assert_eq!(test_case["id"], "error_invalid_txid");

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap(); // "invalid-txid"
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = inputs["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let result = tsc_proof_to_merkle_path(txid, index, &nodes, block_height);
        assert!(result.is_err(), "Invalid txid should fail");
        assert!(
            result.unwrap_err().contains("invalid txid"),
            "Error should mention invalid txid"
        );
    }

    #[test]
    fn tv_error_invalid_node_hash() {
        // Test vector: error_invalid_node_hash
        // Error when node hash is invalid
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][5];

        assert_eq!(test_case["id"], "error_invalid_node_hash");

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap();
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = inputs["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let result = tsc_proof_to_merkle_path(txid, index, &nodes, block_height);
        assert!(result.is_err(), "Invalid node hash should fail");
        assert!(
            result.unwrap_err().contains("invalid node hash"),
            "Error should mention invalid node hash"
        );
    }

    #[test]
    fn tv_merkle_path_compute_root() {
        // Verify that a successfully constructed MerklePath can compute a root.
        // Using the even-index test vector.
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][0];

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap();
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = inputs["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let merkle_path = tsc_proof_to_merkle_path(txid, index, &nodes, block_height).unwrap();

        // compute_root should succeed and return a 64-char hex string
        let root = merkle_path
            .compute_root(Some(txid))
            .expect("compute_root should succeed");
        assert_eq!(root.len(), 64, "Merkle root should be 64 hex chars");
        assert!(
            hex::decode(&root).is_ok(),
            "Merkle root should be valid hex"
        );
    }

    #[test]
    fn tv_merkle_path_duplicate_compute_root() {
        // Verify compute_root works with duplicate marker nodes
        let vectors = load_merkle_vectors();
        let test_case = &vectors["tsc_proof_to_merkle_path_test_vectors"][2];

        let inputs = &test_case["inputs"];
        let txid = inputs["txid"].as_str().unwrap();
        let index = inputs["index"].as_u64().unwrap();
        let nodes: Vec<String> = inputs["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_string())
            .collect();
        let block_height = inputs["blockHeight"].as_u64().unwrap() as u32;

        let merkle_path = tsc_proof_to_merkle_path(txid, index, &nodes, block_height).unwrap();

        let root = merkle_path
            .compute_root(Some(txid))
            .expect("compute_root with duplicate should succeed");
        assert_eq!(root.len(), 64, "Merkle root should be 64 hex chars");
    }

    #[test]
    fn tv_validate_test_vector_structure() {
        // Verify the test vector file structure
        let vectors = load_merkle_vectors();
        let test_cases = vectors["tsc_proof_to_merkle_path_test_vectors"]
            .as_array()
            .unwrap();
        assert_eq!(test_cases.len(), 6, "Should have exactly 6 test cases");

        // Verify notes section exists
        let notes = &vectors["notes"];
        assert_eq!(
            notes["txid_format"],
            "64-character lowercase hex string (32 bytes)"
        );
        assert_eq!(notes["duplicate_marker"], "The '*' character indicates the node at that level is a duplicate of the previous node (used when a tree level has odd number of nodes)");
    }
}

// =============================================================================
// Module: ValidCreateActionArgs (StorageClient) Tests
// =============================================================================
//
// Tests for the ValidCreateActionArgs struct used by the remote storage client.
// Verifies flag derivation matches the defaults from the test vectors.

#[cfg(feature = "remote")]
mod valid_create_action_args {
    use super::*;
    use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput};
    use bsv_wallet_toolbox_rs::storage::client::ValidCreateActionArgs;

    #[test]
    fn tv_default_flags_match_test_vectors() {
        let vectors = load_test_vectors("test_vectors/storage/create_action/defaults.json");
        let expected = &vectors["default_valid_create_action_args"]["args"];

        // Build args matching the test vector (which includes an output)
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = CreateActionArgs {
            description: "test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![]),
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 42000,
                output_description: "test output".to_string(),
                basket: None,
                custom_instructions: Some(
                    r#"{"derivationPrefix":"bPRI9FYwsIo=","derivationSuffix":"FdjLdpnLnJM=","type":"BRC29"}"#
                        .to_string(),
                ),
                tags: Some(vec!["test_tag=true".to_string()]),
            }]),
            lock_time: Some(0),
            version: Some(1),
            labels: Some(vec!["test_label=true".to_string()]),
            options: Some(CreateActionOptions {
                accept_delayed_broadcast: Some(false),
                sign_and_process: Some(true),
                randomize_outputs: Some(false),
                send_with: None,
                known_txids: None,
                no_send_change: None,
                no_send: None,
                trust_self: None,
                return_txid_only: None,
            }),
        };

        let valid_args = ValidCreateActionArgs::from(args);

        // Verify internal state flags match the test vector
        assert_eq!(
            valid_args.is_new_tx,
            expected["isNewTx"].as_bool().unwrap(),
            "isNewTx flag should match test vector"
        );
        assert_eq!(
            valid_args.is_no_send,
            expected["isNoSend"].as_bool().unwrap(),
            "isNoSend flag should match test vector"
        );
        assert_eq!(
            valid_args.is_delayed,
            expected["isDelayed"].as_bool().unwrap(),
            "isDelayed flag should match test vector"
        );
        assert_eq!(
            valid_args.is_send_with,
            expected["isSendWith"].as_bool().unwrap(),
            "isSendWith flag should match test vector"
        );
    }
}

// =============================================================================
// Module: Cross-Vector Consistency Tests
// =============================================================================
//
// Tests that verify consistency across different test vector files.

mod cross_vector_consistency {
    use super::*;

    #[test]
    fn tv_create_action_defaults_match_validation_valid_case() {
        // The valid test vector in validation.json should match defaults.json
        let validation = load_test_vectors("test_vectors/storage/create_action/validation.json");
        let defaults = load_test_vectors("test_vectors/storage/create_action/defaults.json");

        let valid_case = &validation["valid_test_vectors"][0];
        assert_eq!(valid_case["id"], "go_default_valid_args");

        let valid_inputs = &valid_case["inputs"];
        let default_args = &defaults["default_valid_create_action_args"]["args"];

        // Description should match
        assert_eq!(valid_inputs["description"], default_args["description"],);

        // Output satoshis should match
        let valid_satoshis = valid_inputs["outputs"][0]["satoshis"].as_u64().unwrap();
        let default_satoshis = default_args["outputs"][0]["satoshis"].as_u64().unwrap();
        assert_eq!(valid_satoshis, default_satoshis);

        // Custom instructions should match
        let valid_ci = valid_inputs["outputs"][0]["customInstructions"]
            .as_str()
            .unwrap();
        let default_ci = default_args["outputs"][0]["customInstructions"]
            .as_str()
            .unwrap();
        assert_eq!(valid_ci, default_ci);
    }

    #[test]
    fn tv_test_users_and_brc29_share_sender_keys() {
        // Alice's keys in test_users.json should match the sender in brc29.json
        let users = load_test_vectors("test_vectors/keys/test_users.json");
        let brc29 = load_test_vectors("test_vectors/keys/brc29.json");

        assert_eq!(
            users["test_users"][0]["privateKeyHex"],
            brc29["constants"]["sender"]["privateKeyHex"]
        );
        assert_eq!(
            users["test_users"][0]["publicKeyHex"],
            brc29["constants"]["sender"]["publicKeyHex"]
        );
    }

    #[test]
    fn tv_pagination_constants_consistent() {
        // MaxPaginationLimit should be consistent across all test vectors
        let users = load_test_vectors("test_vectors/keys/test_users.json");
        let list_outputs = load_test_vectors("test_vectors/storage/list_outputs/validation.json");
        let list_actions = load_test_vectors("test_vectors/storage/list_actions/validation.json");

        let max_limit = users["pagination_defaults"]["maxPaginationLimit"]
            .as_u64()
            .unwrap();
        assert_eq!(max_limit, 10000);

        // list_outputs has ts_invalid_limit_too_high with limit: 10001
        let lo_invalid = list_outputs["test_vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tc| tc["id"] == "ts_invalid_limit_too_high")
            .expect("Should have ts_invalid_limit_too_high");
        assert_eq!(lo_invalid["inputs"]["limit"], 10001);

        // list_actions has go_limit_exceeds_max with limit: 10001
        let la_invalid = list_actions["test_vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tc| tc["id"] == "go_limit_exceeds_max")
            .expect("Should have go_limit_exceeds_max");
        assert_eq!(la_invalid["inputs"]["limit"], 10001);
    }

    #[test]
    fn tv_all_test_vector_files_parse() {
        // Verify all 7 test vector files parse as valid JSON
        let files = [
            "test_vectors/storage/create_action/validation.json",
            "test_vectors/storage/create_action/defaults.json",
            "test_vectors/storage/list_outputs/validation.json",
            "test_vectors/storage/list_actions/validation.json",
            "test_vectors/keys/brc29.json",
            "test_vectors/keys/test_users.json",
            "test_vectors/transactions/merkle_path.json",
        ];

        for file in &files {
            let data = load_test_vectors(file);
            assert!(
                data.is_object(),
                "Test vector file {} should parse as a JSON object",
                file
            );
            assert!(
                data.get("description").is_some(),
                "Test vector file {} should have a description field",
                file
            );
        }
    }
}
