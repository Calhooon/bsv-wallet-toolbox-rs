//! Shared refusal assertion for the unit and external-library witnesses.

use super::Error;

pub(super) fn assert_isolated(error: Error) {
    assert!(matches!(error, Error::NetworkError(_)), "{error:?}");
    let message = error.to_string();
    assert!(
        message.contains("provider.invalid")
            && message.contains("#182")
            && message.contains("rule 28"),
        "{message}"
    );
}
