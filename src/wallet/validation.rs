use bsv_rs::wallet::CreateActionArgs;

use crate::error::{Error, Result};

/// Check caller input scripts before the create door calls storage.
pub(super) fn validate_create_action_inputs(args: &CreateActionArgs) -> Result<()> {
    for input in args.inputs.iter().flatten() {
        // ts-stack@edf6e03 validationHelpers.ts:522-530,595 requires non-empty hex.
        if input.unlocking_script.as_ref().is_some_and(Vec::is_empty) {
            return Err(Error::ValidationError(
                "The unlockingScript parameter must be hexadecimal string.".to_string(),
            ));
        }
        // BRCs@5119a9b wallet/0100.md:649 requires a positive length.
        if input.unlocking_script_length == Some(0) {
            return Err(Error::ValidationError(
                "The unlockingScriptLength parameter must be a positive integer.".to_string(),
            ));
        }
    }
    Ok(())
}
