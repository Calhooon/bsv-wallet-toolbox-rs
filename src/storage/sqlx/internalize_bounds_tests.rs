//! The reference's array bounds at the internalize door (#309):
//! ts-stack@edf6e03 validationHelpers.ts validateInternalizeActionArgs
//! (1046-1074) calls validateArray (105-114, 100000 at 67) for `outputs`
//! (1052) and `labels` (1054), and validateBasketInsertion (937-951) for an
//! insertion's `tags` (948), each ahead of its items.

use super::*;
use crate::storage::traits::WalletStorageWriter;
use crate::storage::AuthId;
use bsv_rs::wallet::InternalizeOutput;

const LIMIT: usize = 100_000;

fn insertion_output(i: usize) -> InternalizeOutput {
    InternalizeOutput {
        output_index: i as u32,
        protocol: BASKET_INSERTION_PROTOCOL.to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: "tokens".to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }
}

fn args(outputs: Vec<InternalizeOutput>) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx: vec![],
        outputs,
        description: "internalize bounds".to_string(),
        labels: None,
        seek_permission: None,
    }
}

fn refusal(args: &InternalizeActionArgs) -> String {
    match validate_internalize_action_args(args) {
        Err(Error::ValidationError(text)) => text,
        other => panic!("expected a ValidationError, got {other:?}"),
    }
}

fn bound_text(name: &str) -> String {
    format!("The {name} parameter must be an array of at most {LIMIT} items")
}

fn too_long_instructions() -> Option<String> {
    Some("x".repeat(1001))
}

const INSTRUCTIONS_TEXT: &str =
    "Invalid outputs[0].insertionRemittance.customInstructions: must be no more than 1000 bytes, got 1001";

// outputs: validateArray(args.outputs, 'outputs') at 1052.

#[test]
fn outputs_over_the_bound_are_refused_by_the_bound() {
    let args = args((0..=LIMIT).map(insertion_output).collect());
    assert_eq!(refusal(&args), bound_text("outputs"));
}

#[test]
fn outputs_at_the_bound_pass_it() {
    let args = args((0..LIMIT).map(insertion_output).collect());
    assert!(validate_internalize_action_args(&args).is_ok());
}

#[test]
fn outputs_bound_runs_ahead_of_an_output() {
    // The bound (1052) runs before validateInternalizeOutput maps the items,
    // so an over-long customInstructions (946) in output 0 is not reached.
    let mut outputs: Vec<_> = (0..=LIMIT).map(insertion_output).collect();
    outputs[0]
        .insertion_remittance
        .as_mut()
        .unwrap()
        .custom_instructions = too_long_instructions();
    assert_eq!(refusal(&args(outputs)), bound_text("outputs"));
}

#[tokio::test]
async fn outputs_over_the_bound_are_refused_at_the_storage_door() {
    // Step 0 of internalize_action_internal: nothing is read or written.
    let storage = StorageSqlx::in_memory().await.unwrap();
    let auth = AuthId::with_user_id("a".repeat(66), 1);
    let args = args((0..=LIMIT).map(insertion_output).collect());
    match storage.internalize_action(&auth, args).await {
        Err(Error::ValidationError(text)) => assert_eq!(text, bound_text("outputs")),
        other => panic!("expected the outputs bound, got {other:?}"),
    }
}

// An insertion's tags: validateArray(args.tags, 'tags') at 948, after its
// customInstructions (942-946).

fn with_tags(n: usize) -> InternalizeOutput {
    let mut output = insertion_output(0);
    output.insertion_remittance.as_mut().unwrap().tags = Some(vec!["tag".to_string(); n]);
    output
}

#[test]
fn insertion_tags_over_the_bound_are_refused_by_the_bound() {
    assert_eq!(
        refusal(&args(vec![with_tags(LIMIT + 1)])),
        bound_text("tags")
    );
}

#[test]
fn insertion_tags_at_the_bound_pass_it() {
    assert!(validate_internalize_action_args(&args(vec![with_tags(LIMIT)])).is_ok());
}

#[test]
fn custom_instructions_run_ahead_of_the_tags_bound() {
    // 946 ahead of 948 in the same insertion.
    let mut output = with_tags(LIMIT + 1);
    output
        .insertion_remittance
        .as_mut()
        .unwrap()
        .custom_instructions = too_long_instructions();
    assert_eq!(refusal(&args(vec![output])), INSTRUCTIONS_TEXT);
}

#[test]
fn an_earlier_outputs_tags_bound_runs_ahead_of_a_later_outputs_instructions() {
    // Outputs are mapped in order (1052): output 0's tags (948) are refused
    // before output 1's customInstructions (946) are read.
    let mut second = insertion_output(1);
    second
        .insertion_remittance
        .as_mut()
        .unwrap()
        .custom_instructions = too_long_instructions();
    assert_eq!(
        refusal(&args(vec![with_tags(LIMIT + 1), second])),
        bound_text("tags")
    );
}

// labels: validateArray(args.labels, 'labels') at 1054, after the outputs.

#[test]
fn labels_over_the_bound_are_refused_by_the_bound() {
    let mut args = args(vec![insertion_output(0)]);
    args.labels = Some(vec!["label".to_string(); LIMIT + 1]);
    assert_eq!(refusal(&args), bound_text("labels"));
}

#[test]
fn labels_at_the_bound_pass_it() {
    let mut args = args(vec![insertion_output(0)]);
    args.labels = Some(vec!["label".to_string(); LIMIT]);
    assert!(validate_internalize_action_args(&args).is_ok());
}

#[test]
fn an_output_runs_ahead_of_the_labels_bound() {
    let mut output = insertion_output(0);
    output
        .insertion_remittance
        .as_mut()
        .unwrap()
        .custom_instructions = too_long_instructions();
    let mut args = args(vec![output]);
    args.labels = Some(vec!["label".to_string(); LIMIT + 1]);
    assert_eq!(refusal(&args), INSTRUCTIONS_TEXT);
}
