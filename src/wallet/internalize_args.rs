//! Internalize argument validation shared by the wallet and storage doors.

use super::label;
use crate::error::{Error, Result};
use bsv_rs::primitives::PublicKey;
use bsv_rs::wallet::{InternalizeActionArgs, WalletPayment};

/// Protocol identifier for wallet payments.
pub(crate) const WALLET_PAYMENT_PROTOCOL: &str = "wallet payment";

/// Protocol identifier for basket insertions.
pub(crate) const BASKET_INSERTION_PROTOCOL: &str = "basket insertion";

/// `MAXIMUM_WALLET_COLLECTION_ITEMS` at ts-stack@edf6e03
/// validationHelpers.ts:67, validateArray's default bound (105-114).
pub(crate) const MAXIMUM_WALLET_COLLECTION_ITEMS: usize = 100_000;

/// validateArray's bound (validationHelpers.ts:112-114) in
/// WERR_INVALID_PARAMETER's words (WERR_INVALID_PARAMETER.ts:14). Shared by
/// the storage doors (create, internalize, the three list finders), each
/// calling it where the reference calls validateArray, ahead of the items.
pub(crate) fn validate_array_bound(len: usize, name: &str, maximum: usize) -> Result<()> {
    if len > maximum {
        return Err(Error::ValidationError(format!(
            "The {name} parameter must be an array of at most {maximum} items"
        )));
    }
    Ok(())
}

/// The reference's `WERR_INVALID_PARAMETER` text for a `p nosend ` label on
/// internalizeAction (`Wallet.ts:1249`, built by `WERR_errors.ts:105`).
const BRC177_INCOMING_REFUSAL: &str =
    "The labels parameter must be BRC-177 noSend expiry labels only on outgoing createAction requests";

/// Validates internalize action arguments at both doors with bsv-rs's
/// validator (#143): a basket insertion's `customInstructions` is at most
/// 1000 UTF-8 bytes (BRC-100 wallet/0100.md:867 at BRCs@5119a9b; ts-stack
/// `validateBasketInsertion`). Its refusal is `Error::ValidationError` in
/// bsv-rs's words, so the door's text and bsv-rs's cannot drift.
///
/// The array bounds (#309) sit where ts-stack@edf6e03 validationHelpers.ts
/// validateInternalizeActionArgs (1046-1074) calls validateArray: `outputs`
/// (1052) ahead of every output, an insertion's `tags` (948) after its
/// `customInstructions` (942-946), `labels` (1054) after the outputs.
///
/// Returns, one per output, the wallet payment's `senderIdentityKey` as the
/// validated args carry it (#453): the key validatePublicKey parsed from the
/// trimmed value (920, 185-191). The wallet derives the BRC-29 counterparty
/// from it, as the reference's signer does from `vargs` (signer/methods/
/// internalizeAction.ts:56, 86), and the sqlx door stores its hex, the
/// trimmed and lowercased value the reference's storage writes (storage/
/// methods/internalizeAction.ts:240, 778, 804).
pub(crate) fn validate_internalize_action_args(
    args: &InternalizeActionArgs,
) -> Result<Vec<Option<PublicKey>>> {
    validate_array_bound(
        args.outputs.len(),
        "outputs",
        MAXIMUM_WALLET_COLLECTION_ITEMS,
    )?;
    let senders = validate_outputs(args)?;
    validate_description(&args.description)?;
    validate_array_bound(
        args.labels.as_ref().map_or(0, Vec::len),
        "labels",
        MAXIMUM_WALLET_COLLECTION_ITEMS,
    )?;
    for label in args.labels.iter().flatten() {
        validate_label(label)?;
    }

    // BRC-177 (#357): the reference refuses a `p nosend ` label on
    // internalizeAction once the arguments are valid (ts-stack@edf6e03
    // Wallet.ts:1248-1250, `startsWith` at brc177NoSendExpiry.ts:119), so it
    // never lands as metadata on an incoming action. The label is matched
    // as the SDK's validateLabel leaves it, trimmed with ECMAScript's whitespace set and lowercased
    // (validationHelpers.ts:1054).
    if has_brc177_no_send_label(args.labels.as_deref()) {
        return Err(Error::ValidationError(BRC177_INCOMING_REFUSAL.to_string()));
    }
    Ok(senders)
}

/// The outputs' checks in the reference's order, returning each wallet
/// payment's validated sender.
fn validate_outputs(args: &InternalizeActionArgs) -> Result<Vec<Option<PublicKey>>> {
    // The earliest output refused by its remittance exclusivity, its wallet
    // payment or its tags bound is refused only once every output reached
    // before it has passed its customInstructions, as the reference maps the
    // outputs in order; bsv-rs reads only the outputs. Exclusivity and the
    // payment run ahead of the output's own customInstructions, the tags
    // bound after them.
    let mut senders = Vec::with_capacity(args.outputs.len());
    let over = args.outputs.iter().enumerate().find_map(|(index, output)| {
        let payment = output.payment_remittance.as_ref();
        match validate_remittance_exclusivity(output)
            .and_then(|()| payment.map(validate_wallet_payment).transpose())
        {
            Ok(sender) => senders.push(sender),
            Err(refusal) => return Some((index, refusal)),
        }
        let tags = output.insertion_remittance.as_ref()?.tags.as_ref()?;
        validate_array_bound(tags.len(), "tags", MAXIMUM_WALLET_COLLECTION_ITEMS)
            .err()
            .map(|refusal| (index + 1, refusal))
    });
    if let Some((reached, refusal)) = over {
        let earlier = InternalizeActionArgs {
            tx: Vec::new(),
            outputs: args.outputs[..reached].to_vec(),
            description: String::new(),
            labels: None,
            seek_permission: None,
        };
        validate_insertion_instructions(&earlier)?;
        return Err(refusal);
    }
    validate_insertion_instructions(args)?;
    Ok(senders)
}

/// The reference's `validateStringLength` for a description (#380, #383,
/// ts-stack@edf6e03 validationHelpers.ts:348-356): 5 to 2000 UTF-8 bytes as
/// given, untrimmed, in its texts. Create calls it before inputs (755);
/// internalize after outputs and before labels (1053).
pub(crate) fn validate_description(description: &str) -> Result<()> {
    let mustbe = if description.len() < 5 {
        "at least 5 length."
    } else if description.len() > 2000 {
        "no more than 2000 length."
    } else {
        return Ok(());
    };
    Err(Error::ValidationError(format!(
        "The description parameter must be {mustbe}"
    )))
}

/// The reference's validateLabel (#365, ts-stack@edf6e03
/// validationHelpers.ts:385-387, validateIdentifier at 409-417): 1 to 300
/// UTF-8 bytes once trimmed and lowercased, in its texts. Every label meets
/// it before the BRC-177 refusal, as validateInternalizeActionArgs maps the
/// labels (1054) before Wallet.ts:1248-1250 reads them.
pub(crate) fn validate_label(label: &str) -> Result<()> {
    let bytes = label::normalize_label(label).len();
    let mustbe = if bytes < 1 {
        "at least 1 length."
    } else if bytes > 300 {
        "no more than 300 length."
    } else {
        return Ok(());
    };
    Err(Error::ValidationError(format!(
        "The label parameter must be {mustbe}"
    )))
}

/// The reference's `hasBrc177NoSendExpiryLabel` (brc177NoSendExpiry.ts:118-120)
/// over labels normalized as validateLabel leaves them.
fn has_brc177_no_send_label(labels: Option<&[String]>) -> bool {
    labels.into_iter().flatten().any(|label| {
        let normalized = label::normalize_label(label);
        label::is_brc177_no_send_label(&normalized)
    })
}

/// One remittance per output (#337, #371): the reference refuses a wallet
/// payment with no paymentRemittance or carrying an insertionRemittance
/// (ts-stack@edf6e03 validationHelpers.ts:972), and a basket insertion with
/// no insertionRemittance or carrying a paymentRemittance (975), before
/// either remittance's fields (981-982).
fn validate_remittance_exclusivity(output: &bsv_rs::wallet::InternalizeOutput) -> Result<()> {
    let payment = output.payment_remittance.is_some();
    let insertion = output.insertion_remittance.is_some();
    let mustbe = match output.protocol.as_str() {
        WALLET_PAYMENT_PROTOCOL if !payment || insertion => {
            "only paymentRemittance for wallet payment"
        }
        BASKET_INSERTION_PROTOCOL if !insertion || payment => {
            "only insertionRemittance for basket insertion"
        }
        _ => return Ok(()),
    };
    Err(Error::ValidationError(format!(
        "The output remittance parameter must be {mustbe}"
    )))
}

/// The reference's validateWalletPayment (#377, ts-stack@edf6e03
/// validationHelpers.ts:914-923), run after the one-remittance check (972-977)
/// at 981: the base64 `derivationPrefix`, then `derivationSuffix`, then the
/// `senderIdentityKey` as a compressed public key, in its texts; returns the
/// sender's key.
fn validate_wallet_payment(payment: &WalletPayment) -> Result<PublicKey> {
    validate_base64(&payment.derivation_prefix, "derivationPrefix")?;
    validate_base64(&payment.derivation_suffix, "derivationSuffix")?;
    validate_public_key(&payment.sender_identity_key, "senderIdentityKey")
}

/// validateBase64String with no bounds (488-498, 443-462): trimmed as
/// ECMAScript trims (`label.rs`), not empty, the base64 alphabet with `=`
/// only in the last two places, and a length that is a multiple of 4 (462
/// refuses every other remainder).
fn validate_base64(value: &str, name: &str) -> Result<()> {
    let value = label::trim_ecmascript(value).as_bytes();
    let alphabet = |c: u8| c.is_ascii_alphanumeric() || c == b'+' || c == b'/';
    let refused = value.is_empty()
        || !value.len().is_multiple_of(4)
        || value
            .iter()
            .enumerate()
            .any(|(i, &c)| !alphabet(c) && (c != b'=' || i + 2 < value.len()));
    if refused {
        return Err(invalid_parameter(name, "valid base64 string"));
    }
    Ok(())
}

/// validatePublicKey (185-191) through bsv-rs's public helper (bsv-rs #579):
/// validateHexString with 66 to 66 (522-534), then isValidCompressedPublicKey
/// (Secp256k1Validation.ts:15-22), trimmed as ECMAScript trims. Its refusal's
/// words (`The <name> parameter must be ...`) are kept as this door's
/// `ValidationError`, as before the beta.6 re-pin.
fn validate_public_key(value: &str, name: &str) -> Result<PublicKey> {
    bsv_rs::wallet::validate_public_key(value, name)
        .map_err(|refusal| Error::ValidationError(refusal.to_string()))
}

/// The reference's `WERR_INVALID_PARAMETER` text.
fn invalid_parameter(name: &str, mustbe: &str) -> Error {
    Error::ValidationError(format!("The {name} parameter must be {mustbe}"))
}

/// bsv-rs's customInstructions check (#143), its refusal in bsv-rs's words.
fn validate_insertion_instructions(args: &InternalizeActionArgs) -> Result<()> {
    bsv_rs::wallet::validation::validate_internalize_action_args(args).map_err(|e| match e {
        bsv_rs::Error::WalletError(text) => Error::ValidationError(text),
        other => Error::ValidationError(other.to_string()),
    })
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use crate::services::mock::MockWalletServices;
    use crate::{StorageSqlx, Wallet, WalletStorageWriter};
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::script::{LockingScript, UnlockingScript};
    use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
    use bsv_rs::wallet::{InternalizeActionArgs, InternalizeOutput, WalletInterface};
    use bsv_rs::WalletRefusalClass;

    fn payment() -> Vec<u8> {
        let transaction = |outpoint: String| {
            let mut tx = Transaction::new();
            let mut input = TransactionInput::new(outpoint, 0);
            input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
            tx.inputs.push(input);
            tx.outputs.push(TransactionOutput {
                satoshis: Some(1_000),
                locking_script: LockingScript::from_hex("7551").unwrap(),
                change: false,
            });
            tx
        };
        let parent = transaction("11".repeat(32));
        let child = transaction(parent.id());
        let txid = child.id();
        let mut beef = Beef::new();
        // Height unjudged: `validate_internalize_action_args` refuses before any script.
        let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent.id(), 100_001));
        beef.merge_raw_tx(parent.to_binary(), Some(bump));
        beef.merge_transaction(child);
        beef.to_binary_atomic(&txid).unwrap()
    }

    /// bsv-rs #384: the protocol refusal precedes description and labels,
    /// as validateInternalizeOutput does at ts-stack@edf6e03 `packages/sdk/src/wallet/validationHelpers.ts:968-970`.
    #[tokio::test]
    async fn unknown_internalize_protocol_precedes_description_and_labels_at_wallet_door() {
        let root = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("protocol-door", &root.public_key().to_hex())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let wallet = Wallet::new(Some(root), storage, MockWalletServices::new())
            .await
            .unwrap();
        let tx = payment();
        let mut refusals = Vec::new();
        for (description, labels) in [
            ("x", None),
            ("a valid description", None),
            ("a valid description", Some(vec![String::new()])),
        ] {
            let args = InternalizeActionArgs {
                tx: tx.clone(),
                outputs: vec![InternalizeOutput {
                    output_index: 0,
                    protocol: "foo".to_string(),
                    payment_remittance: None,
                    insertion_remittance: None,
                }],
                description: description.to_string(),
                labels,
                seek_permission: None,
            };
            let error = WalletInterface::internalize_action(&wallet, args, None)
                .await
                .unwrap_err();
            refusals.push(error);
        }
        for error in refusals {
            assert!(matches!(
                &error,
                bsv_rs::Error::WalletRefusal {
                    class: WalletRefusalClass::InvalidParameter,
                    ..
                }
            ));
            assert_eq!(
                error.to_string(),
                "Validation error: The protocol parameter must be 'basket insertion' or 'wallet payment'"
            );
        }
    }
}

/// `02` or `03` and 64 hex digits of either case, untrimmed, naming a point
/// with x below p, read through `PublicKey::from_bytes`: `toBinary`'s subject
/// and certifier rule (ts-stack@edf6e03 `Certificate.ts:240-254`), which the
/// issued certificate's keys (`certificate_issuance::certificate_key`) and the
/// overlay record's keys (`lookup_record.rs`) read. No public bsv-rs helper is
/// that rule (bsv-rs's own `read_to_binary_key`, #585, is crate-private):
/// `wallet::validate_public_key` trims a space-padded text `toBinary` refuses,
/// `auth::assert_auth_identity_key` refuses upper case `toBinary` reads.
/// `PublicKey::from_hex` reads the reference's lenient `fromString` forms
/// (bsv-rs #498), an x at or above p reduced among them, which this refuses.
pub(crate) fn compressed_key(key_hex: &str) -> Option<PublicKey> {
    if key_hex.len() != 66 || !matches!(key_hex.get(..2), Some("02" | "03")) {
        return None;
    }
    PublicKey::from_bytes(&hex::decode(key_hex).ok()?).ok()
}
