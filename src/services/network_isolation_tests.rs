//! #182: capture a default post destination without sending a request.

use super::{http_client::check_url, Chain, Services};

#[test]
fn default_provider_request_is_refused_before_transport() {
    for chain in [Chain::Main, Chain::Test] {
        let services = Services::new(chain).unwrap();
        assert_eq!(
            services.post_beef_mode,
            super::services::PostBeefMode::default()
        );
        let request = reqwest::Client::new()
            .post(&services.options.arc_url)
            .build()
            .unwrap();
        assert!(!matches!(
            request.url().host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        ));
        let result = check_url(request.url());
        assert!(
            matches!(result, Err(crate::Error::NetworkError(ref message)) if message.contains("#182") && message.contains("rule 28")),
            "#182 rule 28: the captured default post destination must be refused before transport"
        );
    }
}
