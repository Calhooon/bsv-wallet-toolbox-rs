//! bsv-rs #660 and #657 at the live internalize door (`verify_beef_spends`,
//! the spend's word at tip + 1).
//!
//! #660: the encoding check reads a key's length and prefix alone, as the
//! node's `IsCompressedOrUncompressedPubKey` does (bitcoin-sv v1.2.3
//! `interpreter.cpp:132-152`); a key that is no curve point is the signature
//! check's false. With an empty signature `<key> OP_CHECKSIG OP_NOT` and its
//! 1-of-1 `OP_CHECKMULTISIG` form are valid. At bsv-rs 0.9.0-beta.6 the door
//! refused both (`The public key is in an unknown format.` and `OP_CHECKMULTISIG
//! requires correct encoding for the public key and signature.`); at the pin a
//! payment it refused is admitted, in both eras, at versions 1 and 2.
//!
//! #657: `OP_CHECKMULTISIG` refuses a badly encoded signature in the encoding
//! check's own words, where beta.6 wrote its wrapper sentence; the door's
//! refusal carries the interpreter's text.

use super::{payment, script_words, verify_at_height};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{ProtocolEra, UnlockingScript};

/// `02` with x = 5: the right length and prefix, off the curve.
fn off_curve_key() -> String {
    format!("02{}05", "00".repeat(31))
}

const ERAS: [u32; 2] = [
    ProtocolEra::MAINNET_GENESIS_HEIGHT + 100,
    ProtocolEra::MAINNET_CHRONICLE_HEIGHT,
];

/// The door's verdict on a spend of `lock` unlocked by `unlock`, both
/// transactions at `version`, the parent proven at `height`.
async fn door(lock: &str, unlock: &str, version: u32, height: u32) -> crate::Result<()> {
    let (mut parent, mut child) = payment(lock);
    parent.version = version;
    child.inputs[0].source_txid = Some(parent.id());
    child.version = version;
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_hex(unlock).unwrap());
    child.invalidate_caches();
    verify_at_height(parent, child, height).await
}

/// `<off-curve key> OP_CHECKSIG OP_NOT` with an empty signature: refused at
/// beta.6 (`The public key is in an unknown format.`), valid at the pin.
#[tokio::test]
async fn the_door_admits_an_off_curve_key_checksig_not_with_an_empty_signature_660() {
    let lock = format!("21{}ac91", off_curve_key());
    for height in ERAS {
        for version in [1, 2] {
            let result = door(&lock, "00", version, height).await;
            assert!(
                result.is_ok(),
                "CHECKSIG v{version} at {height}: {result:?}"
            );
        }
    }
}

/// `1 <off-curve key> 1 OP_CHECKMULTISIG OP_NOT` with an empty signature:
/// refused at beta.6 (`OP_CHECKMULTISIG requires correct encoding for the
/// public key and signature.`), valid at the pin.
#[tokio::test]
async fn the_door_admits_an_off_curve_key_checkmultisig_not_with_an_empty_signature_660() {
    let lock = format!("5121{}51ae91", off_curve_key());
    for height in ERAS {
        for version in [1, 2] {
            let result = door(&lock, "0000", version, height).await;
            assert!(
                result.is_ok(),
                "CHECKMULTISIG v{version} at {height}: {result:?}"
            );
        }
    }
}

/// A signature that is no DER into `1 <key> 1 OP_CHECKMULTISIG`: refused at
/// beta.6 in the wrapper's words (`OP_CHECKMULTISIG requires correct encoding
/// for the public key and signature.`), at the pin in the encoding check's.
#[tokio::test]
async fn the_door_refuses_a_bad_der_checkmultisig_signature_in_the_check_words_657() {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7))
        .unwrap()
        .public_key()
        .to_hex();
    let lock = format!("5121{key}51ae");
    for height in ERAS {
        script_words(
            door(&lock, "0003300141", 1, height).await,
            "The signature format is invalid.",
        );
    }
}
