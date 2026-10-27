//! Destination check for the services transport.

pub(super) fn check_url(_url: &reqwest::Url) -> crate::Result<()> {
    Ok(())
}
