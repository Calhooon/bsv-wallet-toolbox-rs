//! The description bound at the internalize door (#380). The reference's
//! validateInternalizeActionArgs (ts-stack@edf6e03 validationHelpers.ts:1046-1056)
//! checks `description` through validateStringLength(args.description,
//! 'description', 5, 2000) at 1053, after the outputs (1052) and ahead of
//! the labels (1054). validateStringLength (348-356) counts UTF-8 bytes of
//! the string as given, untrimmed, in `WERR_INVALID_PARAMETER`'s text.

use super::bounds_tests::{args, refusal};
use super::*;
use crate::storage::traits::WalletStorageWriter;
use crate::storage::AuthId;
use bsv_rs::wallet::InternalizeOutput;

const TOO_SHORT: &str = "The description parameter must be at least 5 length.";
const TOO_LONG: &str = "The description parameter must be no more than 2000 length.";

fn described(description: &str) -> InternalizeActionArgs {
    let mut args = args(vec![InternalizeOutput {
        output_index: 0,
        protocol: BASKET_INSERTION_PROTOCOL.to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: "tokens".to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }]);
    args.description = description.to_string();
    args
}

#[test]
fn four_byte_description_is_refused() {
    assert_eq!(refusal(&described("abcd")), TOO_SHORT);
    assert_eq!(refusal(&described("")), TOO_SHORT);
}

#[test]
fn over_long_description_is_refused() {
    assert_eq!(refusal(&described(&"a".repeat(2001))), TOO_LONG);
}

#[test]
fn description_at_either_end_passes() {
    assert!(validate_internalize_action_args(&described("abcde")).is_ok());
    assert!(validate_internalize_action_args(&described(&"a".repeat(2000))).is_ok());
}

#[test]
fn description_bound_is_utf8_bytes_untrimmed() {
    // Two two-byte characters are 4 bytes; 1001 of them are 2002.
    assert_eq!(refusal(&described("\u{e9}\u{e9}")), TOO_SHORT);
    assert_eq!(refusal(&described(&"\u{e9}".repeat(1001))), TOO_LONG);
    assert!(validate_internalize_action_args(&described(&"\u{e9}".repeat(1000))).is_ok());
    // Not trimmed: " abc " is 5 bytes.
    assert!(validate_internalize_action_args(&described(" abc ")).is_ok());
}

#[test]
fn short_description_answers_ahead_of_an_over_long_label() {
    // 1053 ahead of validateLabel at 1054.
    let mut args = described("abcd");
    args.labels = Some(vec!["a".repeat(301)]);
    assert_eq!(refusal(&args), TOO_SHORT);
}

#[test]
fn short_description_answers_ahead_of_the_nosend_refusal() {
    // 1053 ahead of Wallet.ts:1248-1250.
    let mut args = described("abcd");
    args.labels = Some(vec!["p nosend expiry seconds 300".to_string()]);
    assert_eq!(refusal(&args), TOO_SHORT);
}

#[test]
fn an_output_runs_ahead_of_the_description() {
    // The outputs (1052) ahead of 1053.
    let mut args = described("abcd");
    args.outputs[0]
        .insertion_remittance
        .as_mut()
        .unwrap()
        .custom_instructions = Some("x".repeat(1001));
    assert_eq!(
        refusal(&args),
        "Invalid outputs[0].insertionRemittance.customInstructions: must be no more than 1000 bytes, got 1001"
    );
}

#[tokio::test]
async fn short_description_is_refused_at_the_storage_door() {
    // Step 0 of internalize_action_internal: nothing is read or written.
    let storage = StorageSqlx::in_memory().await.unwrap();
    let auth = AuthId::with_user_id("a".repeat(66), 1);
    match storage.internalize_action(&auth, described("abcd")).await {
        Err(Error::ValidationError(text)) => assert_eq!(text, TOO_SHORT),
        other => panic!("expected the description bound, got {other:?}"),
    }
}
