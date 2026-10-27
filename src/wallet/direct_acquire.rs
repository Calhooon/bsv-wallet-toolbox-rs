//! `validateAcquireDirectCertificateArgs` (ts-stack@edf6e03
//! `sdk/src/wallet/validationHelpers.ts:1401-1450`), which the reference's
//! direct acquire runs before anything else (`wallet-toolbox/src/Wallet.ts:697`, through
//! `validateAuthAndArgs`), before any row is written (#311). The checks run in
//! its order with its `WERR_INVALID_PARAMETER` texts; the typed arguments make
//! `validateRecord(args)` (1404), the protocol check (1406-1408, the caller's
//! match), the revealer's key (1438, a parsed `PublicKey`) and `privileged`
//! (1440) unfailing, and 1441 repeats 1405.

use std::collections::HashMap;

use bsv_rs::wallet::{AcquireCertificateArgs, KeyringRevealer, WalletCertificate};

use super::{
    invalid_certificate_argument, validate_base64_structure_arg, validate_certificate_base64_arg,
    validate_certificate_fields_arg, validate_certificate_signature_arg, validate_outpoint_arg,
    validate_privilege_arg, validate_public_key_arg, validate_record_arg, validate_record_name_arg,
};

/// The validated, normalized arguments of a direct acquire.
pub(super) struct ValidDirectAcquire {
    /// The certificate as stored, its subject still empty (1447).
    pub(super) certificate: WalletCertificate,
    /// The normalized certifier, or the revealer's key
    /// (`acquireDirectCertificate.ts:19`).
    pub(super) verifier: String,
    /// The subject's master keyring.
    pub(super) keyring: HashMap<String, String>,
}

/// The presence checks at 1409-1430: a falsy string (absent or empty) or an
/// absent revealer or keyring is refused; an empty keyring is present (1425).
struct Present<'a> {
    serial_number: &'a str,
    signature: &'a str,
    revocation_outpoint: &'a str,
    revealer: &'a KeyringRevealer,
    keyring: &'a HashMap<String, String>,
}

const DIRECT: &str = "valid when acquisitionProtocol is \"direct\"";

fn required<'a, T: ?Sized>(
    value: Option<&'a T>,
    present: impl Fn(&T) -> bool,
    name: &str,
) -> bsv_rs::Result<&'a T> {
    value
        .filter(|value| present(value))
        .ok_or_else(|| invalid_certificate_argument(name, DIRECT))
}

fn require_direct_args(args: &AcquireCertificateArgs) -> bsv_rs::Result<Present<'_>> {
    let text = |value: &str| !value.is_empty();
    Ok(Present {
        serial_number: required(args.serial_number.as_deref(), text, "serialNumber")?,
        signature: required(args.signature.as_deref(), text, "signature")?,
        revocation_outpoint: required(
            args.revocation_outpoint.as_deref(),
            text,
            "revocationOutpoint",
        )?,
        revealer: required(args.keyring_revealer.as_ref(), |_| true, "keyringRevealer")?,
        keyring: required(
            args.keyring_for_subject.as_ref(),
            |_| true,
            "keyringForSubject",
        )?,
    })
}

// validateKeyringForSubject at 1248-1259: validateRecord on the map, then per
// name its length and its value's base64 structure with no size bound. The
// names in sorted order, as `validate_record_arg` gives them.
fn validate_keyring_for_subject_arg(keyring: &HashMap<String, String>) -> bsv_rs::Result<()> {
    for name in validate_record_arg(keyring, "keyringForSubject")? {
        validate_record_name_arg(name, "keyringForSubject field name")?;
        validate_base64_structure_arg(&keyring[name], "keyringForSubject field value")?;
    }
    Ok(())
}

/// The direct acquire's argument checks, 1405 to 1439 in order.
pub(super) fn validate_acquire_direct_args(
    args: &AcquireCertificateArgs,
) -> bsv_rs::Result<ValidDirectAcquire> {
    validate_privilege_arg(args.privileged, args.privileged_reason.as_deref())?;
    let present = require_direct_args(args)?;
    let certificate_type = validate_certificate_base64_arg(&args.certificate_type, "type")?;
    let serial_number = validate_certificate_base64_arg(present.serial_number, "serialNumber")?;
    let (certifier, _) = validate_public_key_arg(&args.certifier, "certifier")?;
    let revocation_outpoint =
        validate_outpoint_arg(present.revocation_outpoint, "revocationOutpoint")?;
    validate_certificate_fields_arg(&args.fields)?;
    let signature = validate_certificate_signature_arg(present.signature, "signature")?;
    let verifier = match present.revealer {
        KeyringRevealer::Certifier => certifier.clone(),
        KeyringRevealer::PublicKey(key) => key.to_hex(),
    };
    validate_keyring_for_subject_arg(present.keyring)?;
    Ok(ValidDirectAcquire {
        certificate: WalletCertificate {
            certificate_type,
            subject: String::new(),
            serial_number,
            certifier,
            revocation_outpoint,
            signature,
            fields: args.fields.clone(),
        },
        verifier,
        keyring: present.keyring.clone(),
    })
}
