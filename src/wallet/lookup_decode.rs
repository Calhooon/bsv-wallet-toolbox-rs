//! The overlay's identity outputs read as certificates: each output of an
//! output-list answer decoded, an entry that fails dropped
//! (`decodeIdentityOutput`, ts-stack@edf6e03 `identityUtils.ts:493-557`).

use bsv_rs::overlay::LookupAnswer;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;

use super::{record, OverlayCertificate};
use crate::Result;

/// Parse a `LookupAnswer` into a list of `OverlayCertificate`s.
///
/// Iterates over output-list results, decodes PushDrop from each transaction
/// output, and deserializes the first field as a JSON certificate.
/// Invalid outputs are silently skipped (matching TypeScript behavior).
pub(super) fn parse_overlay_answer(answer: &LookupAnswer) -> Result<Vec<OverlayCertificate>> {
    match answer {
        LookupAnswer::OutputList { outputs } => {
            let mut certs = Vec::new();
            for output in outputs {
                match parse_single_output(&output.beef, output.output_index) {
                    Ok(cert) => certs.push(cert),
                    Err(e) => {
                        tracing::debug!("Skipping invalid overlay output: {}", e);
                    }
                }
            }
            Ok(certs)
        }
        _ => Ok(vec![]),
    }
}

/// Parse a single BEEF output into an `OverlayCertificate`.
fn parse_single_output(beef: &[u8], output_index: u32) -> Result<OverlayCertificate> {
    // Parse BEEF to get transaction
    let tx = Transaction::from_beef(beef, None)
        .map_err(|e| crate::Error::ServiceError(format!("Failed to parse BEEF: {}", e)))?;

    // Get the output's locking script
    let output = tx
        .outputs
        .get(output_index as usize)
        .ok_or_else(|| crate::Error::ServiceError("Output index out of bounds".into()))?;

    // Decode PushDrop from the locking script
    let decoded = PushDrop::decode(&output.locking_script)
        .map_err(|e| crate::Error::ServiceError(format!("Failed to decode PushDrop: {}", e)))?;

    // Parse the first field as JSON certificate
    let cert_json = String::from_utf8(decoded.fields[0].clone())
        .map_err(|e| crate::Error::ServiceError(format!("Invalid UTF-8 in certificate: {}", e)))?;

    record::parse(&cert_json)
}
