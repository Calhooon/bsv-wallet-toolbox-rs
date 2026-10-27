//! Shared HTTP transport. Production uses reqwest directly; test and bench
//! builds reject non-loopback destinations before executing any request.

#[cfg(test)]
pub(crate) use isolated::check_url;
#[cfg(any(test, feature = "bench-support"))]
pub(crate) use isolated::{Client, RequestBuilder, RequestError};
#[cfg(not(any(test, feature = "bench-support")))]
pub(crate) use reqwest::{Client, Error as RequestError, RequestBuilder};

#[cfg(any(test, feature = "bench-support"))]
mod isolated {
    use std::net::{Ipv6Addr, SocketAddr};
    use std::time::Duration;

    use crate::{Error, Result};

    #[derive(Debug, thiserror::Error)]
    pub(crate) enum RequestError {
        #[error(transparent)]
        Transport(#[from] reqwest::Error),
        #[error(transparent)]
        Isolated(#[from] Error),
    }

    impl From<RequestError> for Error {
        fn from(error: RequestError) -> Self {
            match error {
                RequestError::Transport(error) => error.into(),
                RequestError::Isolated(error) => error,
            }
        }
    }

    pub(crate) fn check_url(url: &reqwest::Url) -> Result<()> {
        match url.host_str() {
            Some("127.0.0.1" | "localhost" | "[::1]") => Ok(()),
            host => Err(Error::NetworkError(format!(
                "#182 rule 28: test/bench transport refuses non-loopback host {}; use a loopback fixture URL",
                host.unwrap_or("<missing>")
            ))),
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct Client(reqwest::Client);

    impl Client {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn builder() -> ClientBuilder {
            // Ignore environment/system proxies, and pin localhost instead
            // of trusting DNS or the machine's hosts file. The policy checks
            // every redirect before reqwest can follow it.
            ClientBuilder(
                reqwest::Client::builder()
                    .no_proxy()
                    .resolve_to_addrs(
                        "localhost",
                        &[
                            SocketAddr::from(([127, 0, 0, 1], 0)),
                            SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
                        ],
                    )
                    .redirect(reqwest::redirect::Policy::custom(|attempt| {
                        if let Err(error) = check_url(attempt.url()) {
                            attempt.error(error)
                        } else if attempt.previous().len() >= 10 {
                            attempt.error("too many redirects")
                        } else {
                            attempt.follow()
                        }
                    })),
            )
        }

        pub(crate) fn get(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
            RequestBuilder(self.0.get(url))
        }

        pub(crate) fn post(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
            RequestBuilder(self.0.post(url))
        }
    }

    impl Default for Client {
        fn default() -> Self {
            match Self::builder().build() {
                Ok(client) => client,
                Err(error) => panic!("#182 rule 28: cannot build isolated HTTP client: {error}"),
            }
        }
    }

    pub(crate) struct ClientBuilder(reqwest::ClientBuilder);

    impl ClientBuilder {
        pub(crate) fn timeout(self, timeout: Duration) -> Self {
            Self(self.0.timeout(timeout))
        }

        pub(crate) fn connect_timeout(self, timeout: Duration) -> Self {
            Self(self.0.connect_timeout(timeout))
        }

        pub(crate) fn build(self) -> std::result::Result<Client, reqwest::Error> {
            self.0.build().map(Client)
        }
    }

    pub(crate) struct RequestBuilder(reqwest::RequestBuilder);

    impl RequestBuilder {
        pub(crate) fn header(self, key: &str, value: impl AsRef<str>) -> Self {
            Self(self.0.header(key, value.as_ref()))
        }

        pub(crate) fn headers(self, headers: reqwest::header::HeaderMap) -> Self {
            Self(self.0.headers(headers))
        }

        pub(crate) fn timeout(self, timeout: Duration) -> Self {
            Self(self.0.timeout(timeout))
        }

        pub(crate) fn json<T: serde::Serialize + ?Sized>(self, json: &T) -> Self {
            Self(self.0.json(json))
        }

        pub(crate) fn body(self, body: impl Into<reqwest::Body>) -> Self {
            Self(self.0.body(body))
        }

        pub(crate) async fn send(self) -> std::result::Result<reqwest::Response, RequestError> {
            let (client, request) = self.0.build_split();
            let request = request?;
            check_url(request.url())?;
            client.execute(request).await.map_err(|error| {
                if error.is_redirect() {
                    // reqwest's Display omits the policy error. Keep its
                    // host and rule in the typed error returned to the test.
                    RequestError::Isolated(Error::NetworkError(format!(
                        "{error}: {}",
                        std::error::Error::source(&error)
                            .map(ToString::to_string)
                            .unwrap_or_default()
                    )))
                } else {
                    RequestError::Transport(error)
                }
            })
        }
    }
}
