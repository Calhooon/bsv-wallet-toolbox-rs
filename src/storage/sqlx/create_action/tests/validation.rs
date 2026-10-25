use super::*;
use bsv_rs::wallet::CreateActionOutput;

#[test]
fn test_validate_description_too_short() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "abc".to_string(),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("description length"));
}

#[test]
fn test_validate_description_too_long() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "a".repeat(2001),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("description length"));
}

#[test]
fn test_validate_description_valid() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn test_validate_empty_label() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: Some(vec!["".to_string()]),
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("label cannot be empty"));
}

#[test]
fn test_validate_label_too_long() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: Some(vec!["a".repeat(301)]),
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("label exceeds maximum length"));
}

#[test]
fn test_validate_output_empty_locking_script() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![],
            satoshis: 42000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("locking script cannot be empty"));
}

#[test]
fn test_validate_output_satoshis_too_high() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x76, 0xa9, 0x14], // Partial P2PKH
            satoshis: MAX_SATOSHIS + 1,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("satoshis exceeds maximum value"));
}

#[test]
fn test_validate_output_description_too_short() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x76, 0xa9, 0x14],
            satoshis: 42000,
            output_description: "abc".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("output description length"));
}

#[test]
fn test_validate_output_empty_basket() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x76, 0xa9, 0x14],
            satoshis: 42000,
            output_description: "Test output".to_string(),
            basket: Some("".to_string()),
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("basket cannot be empty"));
}

#[test]
fn test_validate_output_empty_tag() {
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x76, 0xa9, 0x14],
            satoshis: 42000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: Some(vec!["".to_string()]),
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("tag cannot be empty"));
}

#[test]
fn test_validate_valid_output() {
    // Standard P2PKH locking script
    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script,
            satoshis: 42000,
            output_description: "Test output".to_string(),
            basket: Some("payments".to_string()),
            custom_instructions: Some("{\"type\":\"BRC29\"}".to_string()),
            tags: Some(vec!["test_tag".to_string()]),
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["test_label".to_string()]),
        options: None,
    };
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn test_validate_max_possible_satoshis_allowed() {
    // MAX_POSSIBLE_SATOSHIS is a special sentinel value that should be allowed
    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script,
            satoshis: MAX_POSSIBLE_SATOSHIS,
            output_description: "Max possible output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn test_validate_input_missing_unlocking_script() {
    use bsv_rs::wallet::{CreateActionInput, Outpoint};

    let txid =
        hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
    let mut txid_arr = [0u8; 32];
    txid_arr.copy_from_slice(&txid);

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test transaction".to_string(),
        input_beef: None,
        inputs: Some(vec![CreateActionInput {
            outpoint: Outpoint {
                txid: txid_arr,
                vout: 0,
            },
            input_description: "Test input".to_string(),
            unlocking_script: None,
            unlocking_script_length: None,
            sequence_number: None,
        }]),
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err
        .to_string()
        .contains("unlockingScript or unlockingScriptLength required"));
}

#[test]
fn test_validate_input_unlocking_script_length_mismatch() {
    use bsv_rs::wallet::{CreateActionInput, Outpoint};

    let txid =
        hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
    let mut txid_arr = [0u8; 32];
    txid_arr.copy_from_slice(&txid);

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test transaction".to_string(),
        input_beef: None,
        inputs: Some(vec![CreateActionInput {
            outpoint: Outpoint {
                txid: txid_arr,
                vout: 0,
            },
            input_description: "Test input".to_string(),
            unlocking_script: Some(vec![0x00]), // 1 byte
            unlocking_script_length: Some(2),   // but says 2
            sequence_number: None,
        }]),
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("unlocking script length mismatch"));
}

#[test]
fn test_validate_duplicate_input_outpoints() {
    use bsv_rs::wallet::{CreateActionInput, Outpoint};

    let txid =
        hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
    let mut txid_arr = [0u8; 32];
    txid_arr.copy_from_slice(&txid);

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test transaction".to_string(),
        input_beef: None,
        inputs: Some(vec![
            CreateActionInput {
                outpoint: Outpoint {
                    txid: txid_arr,
                    vout: 0,
                },
                input_description: "Input 1".to_string(),
                unlocking_script: Some(vec![0x00]),
                unlocking_script_length: None,
                sequence_number: None,
            },
            CreateActionInput {
                outpoint: Outpoint {
                    txid: txid_arr,
                    vout: 0,
                }, // Same outpoint
                input_description: "Input 2".to_string(),
                unlocking_script: Some(vec![0x00]),
                unlocking_script_length: None,
                sequence_number: None,
            },
        ]),
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    let err = validate_create_action_args(&args).unwrap_err();
    assert!(err.to_string().contains("duplicate outpoint"));
}

#[test]
fn test_validate_valid_input() {
    use bsv_rs::wallet::{CreateActionInput, Outpoint};

    let txid =
        hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
    let mut txid_arr = [0u8; 32];
    txid_arr.copy_from_slice(&txid);

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test transaction".to_string(),
        input_beef: None,
        inputs: Some(vec![CreateActionInput {
            outpoint: Outpoint {
                txid: txid_arr,
                vout: 0,
            },
            input_description: "Valid input".to_string(),
            unlocking_script: None,
            unlocking_script_length: Some(107), // P2PKH unlocking script length
            sequence_number: None,
        }]),
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };
    assert!(validate_create_action_args(&args).is_ok());
}
