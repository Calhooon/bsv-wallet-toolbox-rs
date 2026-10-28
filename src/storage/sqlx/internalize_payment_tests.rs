//! The reference's validateWalletPayment at the internalize door (#377):
//! ts-stack@edf6e03 validationHelpers.ts validateInternalizeOutput (966-985)
//! runs it (981) after the one-remittance check (971-977); it refuses a
//! `derivationPrefix` or `derivationSuffix` that validateBase64String
//! (488-498) refuses, then a `senderIdentityKey` that validatePublicKey
//! (185-191, validateHexString at 522-534 with 66 to 66) refuses (914-923).
//! The outputs map (1052) before the description (1053) and the labels (1054).

use super::bounds_tests::{args, refusal};
use super::*;
use bsv_rs::wallet::InternalizeOutput;

const SENDER: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

const PREFIX_TEXT: &str = "The derivationPrefix parameter must be valid base64 string";
const SUFFIX_TEXT: &str = "The derivationSuffix parameter must be valid base64 string";

fn key_text(mustbe: &str) -> String {
    format!("The senderIdentityKey parameter must be {mustbe}")
}

fn payment(prefix: &str, suffix: &str, sender: &str) -> InternalizeOutput {
    let remittance = WalletPayment {
        derivation_prefix: prefix.to_string(),
        derivation_suffix: suffix.to_string(),
        sender_identity_key: sender.to_string(),
    };
    InternalizeOutput {
        output_index: 0,
        protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
        payment_remittance: Some(remittance),
        insertion_remittance: None,
    }
}

fn valid() -> InternalizeOutput {
    payment("cHJlZml4", "c3VmZml4", SENDER)
}

fn instructions_over_the_bound() -> InternalizeOutput {
    InternalizeOutput {
        output_index: 0,
        protocol: BASKET_INSERTION_PROTOCOL.to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: "tokens".to_string(),
            custom_instructions: Some("x".repeat(1001)),
            tags: None,
        }),
    }
}

#[test]
fn valid_payment_passes() {
    assert!(validate_internalize_action_args(&args(vec![valid()])).is_ok());
}

#[test]
fn padded_base64_and_upper_case_key_pass() {
    let out = payment("Pg==", "bPRI9FYwsIo=", &SENDER.to_uppercase());
    assert!(validate_internalize_action_args(&args(vec![out])).is_ok());
}

#[test]
fn non_base64_prefix_is_refused() {
    let out = payment("test_prefix", "c3VmZml4", SENDER);
    assert_eq!(refusal(&args(vec![out])), PREFIX_TEXT);
}

#[test]
fn empty_and_blank_prefix_are_refused() {
    // validateBase64String trims, then refuses an empty string (490-491).
    for prefix in ["", "   "] {
        let out = payment(prefix, "c3VmZml4", SENDER);
        assert_eq!(refusal(&args(vec![out])), PREFIX_TEXT, "{prefix:?}");
    }
}

#[test]
fn base64_length_and_padding_shapes_are_refused() {
    // Unpadded length not a multiple of 4, padding past two, padding on a
    // length not a multiple of 4, an `=` ahead of the last two (443-462).
    for prefix in ["prefix", "P===", "Pg=", "P=gg"] {
        let out = payment(prefix, "c3VmZml4", SENDER);
        assert_eq!(refusal(&args(vec![out])), PREFIX_TEXT, "{prefix:?}");
    }
}

#[test]
fn non_base64_suffix_is_refused() {
    let out = payment("cHJlZml4", "test_suffix", SENDER);
    assert_eq!(refusal(&args(vec![out])), SUFFIX_TEXT);
}

#[test]
fn prefix_answers_ahead_of_suffix_and_key() {
    let out = payment("test_prefix", "test_suffix", "sender_key");
    assert_eq!(refusal(&args(vec![out])), PREFIX_TEXT);
}

#[test]
fn suffix_answers_ahead_of_key() {
    let out = payment("cHJlZml4", "test_suffix", "sender_key");
    assert_eq!(refusal(&args(vec![out])), SUFFIX_TEXT);
}

#[test]
fn non_hex_key_is_refused() {
    let out = payment("cHJlZml4", "c3VmZml4", "sender_key");
    assert_eq!(refusal(&args(vec![out])), key_text("hexadecimal string."));
}

#[test]
fn empty_key_is_refused_as_not_hex() {
    // An empty string is of even length and fails the hex pattern (525-528).
    let out = payment("cHJlZml4", "c3VmZml4", "");
    assert_eq!(refusal(&args(vec![out])), key_text("hexadecimal string."));
}

#[test]
fn odd_length_key_is_refused() {
    let out = payment("cHJlZml4", "c3VmZml4", &SENDER[..65]);
    assert_eq!(refusal(&args(vec![out])), key_text("even length, not 65."));
}

#[test]
fn short_and_long_keys_are_refused() {
    let short = payment("cHJlZml4", "c3VmZml4", "0211");
    assert_eq!(refusal(&args(vec![short])), key_text("at least 66 length."));
    let long = payment("cHJlZml4", "c3VmZml4", &format!("{SENDER}00"));
    assert_eq!(
        refusal(&args(vec![long])),
        key_text("no more than 66 length.")
    );
}

#[test]
fn key_off_the_curve_or_uncompressed_is_refused() {
    // isValidCompressedPublicKey (Secp256k1Validation.ts:15-22): an 02 or 03
    // prefix and an x on the curve.
    let text = key_text("a valid compressed secp256k1 public key");
    let off = format!("02{}", "11".repeat(32));
    let other = format!("04{}", &SENDER[2..]);
    for key in [off, other] {
        let out = payment("cHJlZml4", "c3VmZml4", &key);
        assert_eq!(refusal(&args(vec![out])), text, "{key}");
    }
}

#[test]
fn exclusivity_answers_ahead_of_the_payment_fields() {
    let mut out = payment("test_prefix", "c3VmZml4", SENDER);
    out.insertion_remittance = Some(BasketInsertion {
        basket: "tokens".to_string(),
        custom_instructions: None,
        tags: None,
    });
    assert_eq!(
        refusal(&args(vec![out])),
        "The output remittance parameter must be only paymentRemittance for wallet payment"
    );
}

#[test]
fn payment_answers_ahead_of_the_description_and_labels() {
    let mut args = args(vec![payment("cHJlZml4", "c3VmZml4", "sender_key")]);
    args.description = "abcd".to_string();
    args.labels = Some(vec!["a".repeat(301)]);
    assert_eq!(refusal(&args), key_text("hexadecimal string."));
}

#[test]
fn earlier_custom_instructions_answer_ahead_of_a_later_payment() {
    let later = payment("test_prefix", "c3VmZml4", SENDER);
    assert_eq!(
        refusal(&args(vec![instructions_over_the_bound(), later])),
        "Invalid outputs[0].insertionRemittance.customInstructions: must be no more than 1000 bytes, got 1001"
    );
}

#[test]
fn earlier_payment_answers_ahead_of_later_custom_instructions() {
    let earlier = payment("cHJlZml4", "c3VmZml4", "sender_key");
    assert_eq!(
        refusal(&args(vec![earlier, instructions_over_the_bound()])),
        key_text("hexadecimal string.")
    );
}
