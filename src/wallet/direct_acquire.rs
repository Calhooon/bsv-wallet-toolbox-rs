//! `validateAcquireDirectCertificateArgs` (ts-stack@edf6e03
//! `sdk/src/wallet/validationHelpers.ts:1401-1450`), which the reference's
//! direct acquire runs before anything else (`wallet-toolbox/src/Wallet.ts:697`, through
//! `validateAuthAndArgs`), before any row is written (#311). The checks run in
//! its order with its `WERR_INVALID_PARAMETER` texts; the typed arguments make
//! `validateRecord(args)` (1404), the protocol check (1406-1408, the caller's
//! match), the revealer's key (1438, a parsed `PublicKey`) and `privileged`
//! (1440) unfailing, and 1441 repeats 1405.

use indexmap::IndexMap;

use bsv_rs::auth::certificates::{Certificate, MasterCertificate};
use bsv_rs::primitives::{from_base64, from_hex, PublicKey};
use bsv_rs::wallet::{
    AcquireCertificateArgs, KeyringRevealer, Outpoint, ProtoWallet, WalletCertificate,
};

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
    pub(super) keyring: IndexMap<String, String>,
}

/// The presence checks at 1409-1430: a falsy string (absent or empty) or an
/// absent revealer or keyring is refused; an empty keyring is present (1425).
struct Present<'a> {
    serial_number: &'a str,
    signature: &'a str,
    revocation_outpoint: &'a str,
    revealer: &'a KeyringRevealer,
    keyring: &'a IndexMap<String, String>,
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
// names in insertion order, as `validate_record_arg` gives them.
fn validate_keyring_for_subject_arg(keyring: &IndexMap<String, String>) -> bsv_rs::Result<()> {
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

// Wallet.ts:705-731: proof errors use the same outer WERR_INVALID_PARAMETER
// text as the validator. WalletError.fromUnknown names a plain Error
// WERR_UNKNOWN and keeps its message without a Rust Display prefix.
fn invalid_proof(name: &str, message: &str) -> bsv_rs::Error {
    invalid_certificate_argument(
        "args",
        &format!(
            "valid encrypted and signed certificate and keyring from revealer. {name}: {message}"
        ),
    )
}

fn proof_error(error: bsv_rs::Error) -> bsv_rs::Error {
    let message = match error {
        bsv_rs::Error::CertificateValidationError(message)
        | bsv_rs::Error::InvalidBase64(message)
        | bsv_rs::Error::InvalidHex(message)
        | bsv_rs::Error::InvalidPublicKey(message) => message,
        other => other.to_string(),
    };
    invalid_proof("WERR_UNKNOWN", &message)
}

fn decode_32(value: &str) -> bsv_rs::Result<[u8; 32]> {
    from_base64(value)?
        .try_into()
        .map_err(|bytes: Vec<u8>| bsv_rs::Error::InvalidDataLength {
            expected: 32,
            actual: bytes.len(),
        })
}

fn master_certificate(
    certificate: &WalletCertificate,
    keyring: &IndexMap<String, String>,
) -> bsv_rs::Result<MasterCertificate> {
    let base = Certificate::new(
        decode_32(&certificate.certificate_type)?,
        decode_32(&certificate.serial_number)?,
        PublicKey::from_hex(&certificate.subject)?,
        PublicKey::from_hex(&certificate.certifier)?,
    )
    .with_revocation_outpoint(Some(Outpoint::from_string(
        &certificate.revocation_outpoint,
    )?))
    .with_fields(certificate.fields.clone())
    .with_signature(Some(from_hex(&certificate.signature)?));
    MasterCertificate::new(base, keyring.clone())
}

/// Verify the signature and prove every field can be decrypted through the
/// acquiring wallet's internal crypto, before storage. The proof carries no
/// caller originator and needs no protocol permission. The plaintext is
/// discarded; the caller stores the encrypted fields and keyring unchanged (#374).
pub(super) async fn verify_acquire_direct(
    wallet: &ProtoWallet,
    certificate: &WalletCertificate,
    keyring: &IndexMap<String, String>,
    args: &AcquireCertificateArgs,
) -> bsv_rs::Result<()> {
    let master = master_certificate(certificate, keyring).map_err(proof_error)?;
    if !master.verify().map_err(proof_error)? {
        // WalletError.fromUnknown maps the reference's plain Error to this name.
        return Err(invalid_proof(
            "WERR_UNKNOWN",
            "Certificate verification failed",
        ));
    }
    master
        .decrypt_fields(
            wallet,
            &master.certifier,
            args.privileged,
            args.privileged_reason.as_deref(),
            None,
        )
        .await
        .map_err(proof_error)?;
    Ok(())
}
