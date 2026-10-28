//! The reference's array bounds at the create door (#309): ts-stack@edf6e03
//! validationHelpers.ts `validateArray` (105-114, `MAXIMUM_WALLET_COLLECTION_ITEMS`
//! 100000 at 67) and `validateSendWith` (136-144, `MAXIMUM_SEND_WITH_TRANSACTIONS`
//! 1000 at 69), rendered through WERR_INVALID_PARAMETER.ts:14.

use super::*;
use bsv_rs::wallet::{CreateActionInput, CreateActionOptions, CreateActionOutput, Outpoint};

const LIMIT: usize = 100_000;
const SEND_WITH_LIMIT: usize = 1000;

fn args() -> bsv_rs::wallet::CreateActionArgs {
    bsv_rs::wallet::CreateActionArgs {
        description: "Valid description".to_string(),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    }
}

fn txid(i: usize) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[..8].copy_from_slice(&(i as u64).to_le_bytes());
    txid
}

fn input(i: usize) -> CreateActionInput {
    CreateActionInput {
        outpoint: Outpoint {
            txid: [7u8; 32],
            vout: i as u32,
        },
        input_description: "input".to_string(),
        unlocking_script: None,
        unlocking_script_length: Some(107),
        sequence_number: None,
    }
}

fn output() -> CreateActionOutput {
    CreateActionOutput {
        locking_script: vec![0x51],
        satoshis: 1,
        output_description: "valid output".to_string(),
        basket: None,
        custom_instructions: None,
        tags: None,
    }
}

fn outpoints(n: usize) -> Vec<Outpoint> {
    (0..n)
        .map(|i| Outpoint {
            txid: [9u8; 32],
            vout: i as u32,
        })
        .collect()
}

fn refusal(args: &bsv_rs::wallet::CreateActionArgs) -> String {
    match validate_create_action_args(args) {
        Err(Error::ValidationError(text)) => text,
        other => panic!("expected a ValidationError, got {other:?}"),
    }
}

fn bound_text(name: &str, maximum: usize) -> String {
    format!("The {name} parameter must be an array of at most {maximum} items")
}

// inputs: validateArray(args.inputs, 'inputs') at 758, ahead of each
// validateCreateActionInput.

#[test]
fn inputs_over_the_bound_are_refused_by_the_bound() {
    let mut args = args();
    args.inputs = Some((0..=LIMIT).map(input).collect());
    assert_eq!(refusal(&args), bound_text("inputs", LIMIT));
}

#[test]
fn inputs_at_the_bound_pass_it() {
    let mut args = args();
    args.inputs = Some((0..LIMIT).map(input).collect());
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn inputs_bound_runs_ahead_of_an_item() {
    let mut args = args();
    let mut inputs: Vec<_> = (0..=LIMIT).map(input).collect();
    inputs[0].unlocking_script_length = None;
    args.inputs = Some(inputs);
    assert_eq!(refusal(&args), bound_text("inputs", LIMIT));
}

#[test]
fn description_runs_ahead_of_the_inputs_bound() {
    // description at 755 ahead of inputs at 758.
    let mut args = args();
    args.description = "abc".to_string();
    args.inputs = Some((0..=LIMIT).map(input).collect());
    assert_eq!(
        refusal(&args),
        "The description parameter must be at least 5 length."
    );
}

// outputs: validateArray(args.outputs, 'outputs') at 759.

#[test]
fn outputs_over_the_bound_are_refused_by_the_bound() {
    let mut args = args();
    args.outputs = Some(vec![output(); LIMIT + 1]);
    assert_eq!(refusal(&args), bound_text("outputs", LIMIT));
}

#[test]
fn outputs_at_the_bound_pass_it() {
    let mut args = args();
    args.outputs = Some(vec![output(); LIMIT]);
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn outputs_bound_runs_ahead_of_an_output() {
    // The bound at 759 ahead of each validateCreateActionOutput.
    let mut args = args();
    let mut outputs = vec![output(); LIMIT + 1];
    outputs[0].locking_script = vec![];
    args.outputs = Some(outputs);
    assert_eq!(refusal(&args), bound_text("outputs", LIMIT));
}

#[test]
fn an_input_item_runs_ahead_of_the_outputs_bound() {
    // Each input (758) is checked before the outputs bound (759).
    let mut args = args();
    let mut bad = input(0);
    bad.unlocking_script_length = None;
    args.inputs = Some(vec![bad]);
    args.outputs = Some(vec![output(); LIMIT + 1]);
    assert_eq!(
        refusal(&args),
        "inputs[0]: unlockingScript or unlockingScriptLength required"
    );
}

// An output's tags: validateArray(o.tags, 'tags') at 639, the last field of
// validateCreateActionOutput (631-642).

#[test]
fn output_tags_over_the_bound_are_refused_by_the_bound() {
    let mut args = args();
    let mut o = output();
    o.tags = Some(vec!["tag".to_string(); LIMIT + 1]);
    args.outputs = Some(vec![o]);
    assert_eq!(refusal(&args), bound_text("tags", LIMIT));
}

#[test]
fn output_tags_at_the_bound_pass_it() {
    let mut args = args();
    let mut o = output();
    o.tags = Some(vec!["tag".to_string(); LIMIT]);
    args.outputs = Some(vec![o]);
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn output_tags_bound_runs_ahead_of_a_tag() {
    let mut args = args();
    let mut o = output();
    let mut tags = vec!["tag".to_string(); LIMIT + 1];
    tags[0] = String::new();
    o.tags = Some(tags);
    args.outputs = Some(vec![o]);
    assert_eq!(refusal(&args), bound_text("tags", LIMIT));
}

#[test]
fn output_description_runs_ahead_of_the_tags_bound() {
    // outputDescription at 636 ahead of tags at 639.
    let mut args = args();
    let mut o = output();
    o.output_description = "abc".to_string();
    o.tags = Some(vec!["tag".to_string(); LIMIT + 1]);
    args.outputs = Some(vec![o]);
    assert!(refusal(&args).starts_with("outputs[0]: output description length"));
}

// labels: validateArray(args.labels, 'labels') at 762, after inputs (758)
// and outputs (759).

#[test]
fn labels_over_the_bound_are_refused_by_the_bound() {
    let mut args = args();
    args.labels = Some(vec!["label".to_string(); LIMIT + 1]);
    assert_eq!(refusal(&args), bound_text("labels", LIMIT));
}

#[test]
fn labels_at_the_bound_pass_it() {
    let mut args = args();
    args.labels = Some(vec!["label".to_string(); LIMIT]);
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn labels_bound_runs_ahead_of_a_label() {
    let mut args = args();
    let mut labels = vec!["label".to_string(); LIMIT + 1];
    labels[0] = String::new();
    args.labels = Some(labels);
    assert_eq!(refusal(&args), bound_text("labels", LIMIT));
}

#[test]
fn an_output_item_runs_ahead_of_the_labels() {
    // Outputs (759) ahead of labels (762): a bad output and a bad label are
    // refused by the output's text.
    let mut args = args();
    let mut o = output();
    o.locking_script = vec![];
    args.outputs = Some(vec![o]);
    args.labels = Some(vec![String::new()]);
    assert_eq!(refusal(&args), "outputs[0]: locking script cannot be empty");
}

// options.knownTxids: validateArray(o.knownTxids, 'knownTxids') at 662,
// options validated at 763 after labels.

fn options() -> CreateActionOptions {
    CreateActionOptions::default()
}

#[test]
fn known_txids_over_the_bound_are_refused_by_the_bound() {
    let mut args = args();
    let mut o = options();
    o.known_txids = Some(vec![[1u8; 32]; LIMIT + 1]);
    args.options = Some(o);
    assert_eq!(refusal(&args), bound_text("knownTxids", LIMIT));
}

#[test]
fn known_txids_at_the_bound_pass_it() {
    let mut args = args();
    let mut o = options();
    o.known_txids = Some(vec![[1u8; 32]; LIMIT]);
    args.options = Some(o);
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn a_label_runs_ahead_of_the_known_txids_bound() {
    let mut args = args();
    args.labels = Some(vec![String::new()]);
    let mut o = options();
    o.known_txids = Some(vec![[1u8; 32]; LIMIT + 1]);
    args.options = Some(o);
    assert_eq!(refusal(&args), "label cannot be empty");
}

// options.noSendChange: validateArray(o.noSendChange, 'noSendChange') at 667.

#[test]
fn no_send_change_over_the_bound_is_refused_by_the_bound() {
    let mut args = args();
    let mut o = options();
    o.no_send_change = Some(outpoints(LIMIT + 1));
    args.options = Some(o);
    assert_eq!(refusal(&args), bound_text("noSendChange", LIMIT));
}

#[test]
fn no_send_change_at_the_bound_passes_it() {
    let mut args = args();
    let mut o = options();
    o.no_send_change = Some(outpoints(LIMIT));
    args.options = Some(o);
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn no_send_change_bound_runs_ahead_of_a_duplicate() {
    let mut args = args();
    let mut o = options();
    let mut nsc = outpoints(LIMIT + 1);
    nsc[1] = nsc[0].clone();
    o.no_send_change = Some(nsc);
    args.options = Some(o);
    assert_eq!(refusal(&args), bound_text("noSendChange", LIMIT));
}

#[test]
fn known_txids_bound_runs_ahead_of_the_no_send_change_bound() {
    // knownTxids at 662 ahead of noSendChange at 667.
    let mut args = args();
    let mut o = options();
    o.known_txids = Some(vec![[1u8; 32]; LIMIT + 1]);
    o.no_send_change = Some(outpoints(LIMIT + 1));
    args.options = Some(o);
    assert_eq!(refusal(&args), bound_text("knownTxids", LIMIT));
}

// options.sendWith: validateSendWith at 670, its bound
// MAXIMUM_SEND_WITH_TRANSACTIONS (1000, at 69) at 137.

#[test]
fn send_with_over_its_bound_is_refused_by_the_bound() {
    let mut args = args();
    let mut o = options();
    o.send_with = Some((0..=SEND_WITH_LIMIT).map(txid).collect());
    args.options = Some(o);
    assert_eq!(refusal(&args), bound_text("sendWith", SEND_WITH_LIMIT));
}

#[test]
fn send_with_at_its_bound_passes_it() {
    // No inputs and no outputs: not a new transaction, so 795-800 does not run.
    let mut args = args();
    let mut o = options();
    o.send_with = Some((0..SEND_WITH_LIMIT).map(txid).collect());
    args.options = Some(o);
    assert!(validate_create_action_args(&args).is_ok());
}

#[test]
fn no_send_change_bound_runs_ahead_of_the_send_with_bound() {
    // noSendChange at 667 ahead of sendWith at 670.
    let mut args = args();
    let mut o = options();
    o.no_send_change = Some(outpoints(LIMIT + 1));
    o.send_with = Some((0..=SEND_WITH_LIMIT).map(txid).collect());
    args.options = Some(o);
    assert_eq!(refusal(&args), bound_text("noSendChange", LIMIT));
}
