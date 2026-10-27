//! The reference's remittance exclusivity at the internalize door (#337):
//! ts-stack@edf6e03 validationHelpers.ts validateInternalizeOutput (966-985)
//! refuses a wallet payment carrying an insertionRemittance (971-973) and a
//! basket insertion carrying a paymentRemittance (974-976) as `output
//! remittance`, one remittance per output, before validateWalletPayment and
//! validateBasketInsertion read any field of either remittance (981-982).

use super::bounds_tests::{args, refusal};
use super::*;
use bsv_rs::wallet::InternalizeOutput;

const LIMIT: usize = 100_000;

const PAYMENT_TEXT: &str =
    "The output remittance parameter must be only paymentRemittance for wallet payment";
const INSERTION_TEXT: &str =
    "The output remittance parameter must be only insertionRemittance for basket insertion";

fn payment() -> WalletPayment {
    WalletPayment {
        derivation_prefix: "cHJlZml4".to_string(),
        derivation_suffix: "c3VmZml4".to_string(),
        sender_identity_key: "02".to_string() + &"11".repeat(32),
    }
}

fn insertion(tags: usize) -> BasketInsertion {
    BasketInsertion {
        basket: "tokens".to_string(),
        custom_instructions: None,
        tags: Some((0..tags).map(|i| format!("tag{i}")).collect()),
    }
}

fn output(
    protocol: &str,
    payment: Option<WalletPayment>,
    insertion: Option<BasketInsertion>,
) -> InternalizeOutput {
    InternalizeOutput {
        output_index: 0,
        protocol: protocol.to_string(),
        payment_remittance: payment,
        insertion_remittance: insertion,
    }
}

fn tags_text() -> String {
    format!("The tags parameter must be an array of at most {LIMIT} items")
}

#[test]
fn payment_with_insertion_over_the_tags_bound_answers_exclusivity() {
    // 971-973 runs before validateBasketInsertion's tags bound (948).
    let out = output(
        WALLET_PAYMENT_PROTOCOL,
        Some(payment()),
        Some(insertion(LIMIT + 1)),
    );
    assert_eq!(refusal(&args(vec![out])), PAYMENT_TEXT);
}

#[test]
fn payment_with_well_formed_insertion_answers_exclusivity() {
    let out = output(WALLET_PAYMENT_PROTOCOL, Some(payment()), Some(insertion(1)));
    assert_eq!(refusal(&args(vec![out])), PAYMENT_TEXT);
}

#[test]
fn payment_with_insertion_over_long_instructions_answers_exclusivity() {
    // 971-973 runs before validateBasketInsertion's customInstructions (942-946).
    let mut extra = insertion(1);
    extra.custom_instructions = Some("x".repeat(1001));
    let out = output(WALLET_PAYMENT_PROTOCOL, Some(payment()), Some(extra));
    assert_eq!(refusal(&args(vec![out])), PAYMENT_TEXT);
}

#[test]
fn insertion_with_payment_answers_exclusivity() {
    let out = output(
        BASKET_INSERTION_PROTOCOL,
        Some(payment()),
        Some(insertion(LIMIT + 1)),
    );
    assert_eq!(refusal(&args(vec![out])), INSERTION_TEXT);
}

#[test]
fn single_insertion_over_the_tags_bound_answers_the_bound() {
    let out = output(BASKET_INSERTION_PROTOCOL, None, Some(insertion(LIMIT + 1)));
    assert_eq!(refusal(&args(vec![out])), tags_text());
}

#[test]
fn earlier_output_refusal_runs_ahead_of_a_later_exclusivity() {
    // The outputs map in order: output 0's tags bound answers before output
    // 1's exclusivity is reached.
    let first = output(BASKET_INSERTION_PROTOCOL, None, Some(insertion(LIMIT + 1)));
    let second = output(WALLET_PAYMENT_PROTOCOL, Some(payment()), Some(insertion(1)));
    assert_eq!(refusal(&args(vec![first, second])), tags_text());
}

#[test]
fn earlier_exclusivity_runs_ahead_of_a_later_tags_bound() {
    let first = output(WALLET_PAYMENT_PROTOCOL, Some(payment()), Some(insertion(1)));
    let second = output(BASKET_INSERTION_PROTOCOL, None, Some(insertion(LIMIT + 1)));
    assert_eq!(refusal(&args(vec![first, second])), PAYMENT_TEXT);
}
