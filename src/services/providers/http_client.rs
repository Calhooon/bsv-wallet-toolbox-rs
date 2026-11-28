//! Shared HTTP transport. Production uses reqwest directly; test and bench
//! builds reject non-loopback destinations before executing any request.

#[cfg(test)]
pub(crate) use isolated::check_url;
#[cfg(test)]
pub(crate) use isolated::shared::{share_clients_on_this_thread, SharedClients};
pub(crate) use isolated::{AnswerRead, Client, RequestBuilder, RequestError, Response};
// A host's tests reach the table through `benchmark_support` (#683).
#[cfg(feature = "bench-support")]
pub use loopback_table::{LoopbackAnswer, LoopbackTable};
// URL parsing for argument guards outside providers (issuance certifierUrl).
pub(crate) use reqwest::Url;

/// Every door's connect time: bsv-rs's own for its transport, and
/// #334's public client's.
pub(crate) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A door's client: its total time, the connect time and its answer bound,
/// the one constructor the provider doors build with.
pub(crate) fn door_client(
    total: std::time::Duration,
    answer_limit: usize,
) -> crate::Result<Client> {
    Client::builder()
        .timeout(total)
        .connect_timeout(CONNECT_TIMEOUT)
        .answer_limit(answer_limit)
        .build()
        .map_err(|e| crate::Error::NetworkError(format!("Failed to create HTTP client: {}", e)))
}

/// A header service's answer (BHS, Chaintracks, the explorers' header
/// fallback): a height, a header, a short run of headers. The
/// reference's bound for its BHS and Chaintracks clients.
pub(crate) const HEADER_ANSWER_MAX_BYTES: usize = 4 * 1024 * 1024;

/// An explorer's answer (`WhatsOnChain`, `Bitails`, `BananaBlocks`), which may
/// be one transaction's bytes as hex: twice the reference's 32 MiB
/// `MAX_RAW_TRANSACTION_BYTES`. A transaction has no small bound, so this
/// is generous; an answer past it is a fault of that rung (could not
/// look) and the ladder asks the next.
pub(crate) const TX_ANSWER_MAX_BYTES: usize = 64 * 1024 * 1024;

#[cfg(any(test, feature = "bench-support"))]
pub(crate) fn check_url_str(url: &str) -> crate::Result<()> {
    let url = reqwest::Url::parse(url)
        .map_err(|error| crate::Error::NetworkError(format!("Invalid transport URL: {error}")))?;
    isolated::check_url(&url)
}

/// The policy's client for the SDK transport that takes one (bsv-rs #320's
/// `SimplifiedFetchTransport::with_client`), so the authenticated storage
/// client's own hops come under #182 too (#263).
#[cfg(any(test, feature = "bench-support"))]
pub(crate) fn sdk_transport_client() -> reqwest::Client {
    isolated::sdk_transport_client()
}

/// The production issuance client (#334): bsv-rs's default (the request
/// timeout given, 10 s connect, no idle reuse), no redirect followed, no
/// proxy, and `host` pinned to the addresses the wallet approved
/// (`wallet/certificate_issuance_public.rs`). Built in production only; the
/// test build has it for the witnesses, against a loopback pin.
#[cfg(any(test, not(feature = "bench-support")))]
pub(crate) type PublicClient = reqwest::Client;

#[cfg(any(test, not(feature = "bench-support")))]
pub(crate) fn public_pinned_client(
    host: &str,
    addresses: &[std::net::SocketAddr],
    timeout: std::time::Duration,
) -> std::result::Result<PublicClient, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(std::time::Duration::from_secs(10))
        .pool_max_idle_per_host(0)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .resolve_to_addrs(host, addresses)
        .build()
}

/// The BRC-104 Peer transport both authenticated clients use (the storage
/// client and certificate issuance): bsv-rs's own in production, and in test
/// and bench builds a wrapper that checks its destination before SDK I/O.
#[cfg(not(any(test, feature = "bench-support")))]
pub(crate) use bsv_rs::auth::SimplifiedFetchTransport;
#[cfg(any(test, feature = "bench-support"))]
pub(crate) use isolated_peer_transport::SimplifiedFetchTransport;

#[cfg(any(test, feature = "bench-support"))]
mod isolated_peer_transport {
    use async_trait::async_trait;
    use bsv_rs::auth::transports::{Transport, TransportCallback};
    use bsv_rs::auth::{AuthMessage, SimplifiedFetchTransport as SdkTransport};

    use super::{check_url_str, sdk_transport_client};

    /// A test/bench Peer transport that checks its destination before SDK I/O
    /// and hands the SDK the #182 policy's client (bsv-rs #320).
    pub struct SimplifiedFetchTransport(SdkTransport);

    impl SimplifiedFetchTransport {
        pub fn new(url: &str) -> Self {
            Self(SdkTransport::with_client(url, sdk_transport_client()))
        }
    }

    #[async_trait]
    impl Transport for SimplifiedFetchTransport {
        async fn send(&self, message: &AuthMessage) -> bsv_rs::Result<()> {
            // Check every send, including handshakes and calls through peer().
            // The SDK's client is the policy's: no proxy, localhost pinned,
            // and a redirect refused by the SDK before its body.
            check_url_str(self.0.base_url())?;
            self.0.send(message).await
        }

        fn set_callback(&self, callback: Box<TransportCallback>) {
            self.0.set_callback(callback);
        }

        fn clear_callback(&self) {
            self.0.clear_callback();
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/common/transport_isolation.rs"]
pub(crate) mod test_support;

// #423: the BRC-104 bound cases both doors on this transport replay.
#[cfg(all(test, any(feature = "sqlite", feature = "remote")))]
#[path = "../../../tests/common/brc104_door_cases.rs"]
pub(crate) mod brc104_door_cases;

// #683: the test's table, answered without a socket.
#[cfg(any(test, feature = "bench-support"))]
#[path = "loopback_table.rs"]
mod loopback_table;

/// Every door's client, in every build (#334's public client aside): the
/// connect time, the total time and the bound on the answer it reads are
/// set here, once, by the door's builder. In test and bench builds the
/// same client also checks each destination (#182) and may answer from
/// the loopback table (#683).
mod isolated {
    #[cfg(any(test, feature = "bench-support"))]
    use std::net::{Ipv6Addr, SocketAddr};
    #[cfg(any(test, feature = "bench-support"))]
    use std::sync::Arc;
    use std::time::Duration;

    use futures::stream::BoxStream;
    use futures::StreamExt;

    #[cfg(any(test, feature = "bench-support"))]
    use super::loopback_table::{self, LoopbackTable};
    use crate::Error;
    #[cfg(any(test, feature = "bench-support"))]
    use crate::Result;

    /// reqwest 0.12's text for a body that fails to decode.
    const DECODE_TEXT: &str = "error decoding response body";

    #[derive(Debug, thiserror::Error)]
    pub(crate) enum RequestError {
        #[error(transparent)]
        Transport(#[from] reqwest::Error),
        #[cfg(any(test, feature = "bench-support"))]
        #[error(transparent)]
        Isolated(#[from] Error),
        /// A text the loopback table gives where reqwest gives its own
        /// error: reqwest's Display, and the same `Error::HttpError` (#683).
        #[cfg(any(test, feature = "bench-support"))]
        #[error("{0}")]
        Loopback(String),
        /// The answer passed the door's bound: the read stopped there. A
        /// service fault (`Error::NetworkError`), as a timeout is.
        #[error("the answer passed this door's bound of {limit} bytes; the read stopped there")]
        TooLarge { limit: usize },
        /// A bounded body that is not the JSON asked for: reqwest's text.
        #[error("{}", DECODE_TEXT)]
        Decode,
    }

    impl From<RequestError> for Error {
        fn from(error: RequestError) -> Self {
            match error {
                RequestError::Transport(error) => error.into(),
                #[cfg(any(test, feature = "bench-support"))]
                RequestError::Isolated(error) => error,
                #[cfg(any(test, feature = "bench-support"))]
                RequestError::Loopback(text) => Error::HttpError(text),
                error @ RequestError::TooLarge { .. } => Error::NetworkError(error.to_string()),
                RequestError::Decode => Error::HttpError(DECODE_TEXT.to_string()),
            }
        }
    }

    /// A door's read of an answer, mapped by the door's own closure, except
    /// an answer past the door's bound: that is the fault a timeout is
    /// (`From<RequestError>`), never "invalid", never "not found".
    pub(crate) trait AnswerRead<T> {
        fn or_door<E: From<RequestError>>(
            self,
            door: impl FnOnce(RequestError) -> E,
        ) -> std::result::Result<T, E>;
    }

    impl<T> AnswerRead<T> for std::result::Result<T, RequestError> {
        fn or_door<E: From<RequestError>>(
            self,
            door: impl FnOnce(RequestError) -> E,
        ) -> std::result::Result<T, E> {
            self.map_err(|error| match error {
                error @ RequestError::TooLarge { .. } => error.into(),
                error => door(error),
            })
        }
    }

    #[cfg(any(test, feature = "bench-support"))]
    pub(crate) fn check_url(url: &reqwest::Url) -> Result<()> {
        match url.host_str() {
            Some("127.0.0.1" | "localhost" | "[::1]") => Ok(()),
            host => Err(Error::NetworkError(format!(
                "#182 rule 28: test/bench transport refuses non-loopback host {}; use a loopback fixture URL",
                host.unwrap_or("<missing>")
            ))),
        }
    }

    /// A door's client: reqwest's, the door's answer bound (`None` reads
    /// whole, for the doors bounded elsewhere), and in test and bench
    /// builds the table it answers from when it was built under
    /// [`LoopbackTable::building`] (#683).
    #[derive(Clone, Debug)]
    pub(crate) struct Client {
        inner: reqwest::Client,
        limit: Option<usize>,
        #[cfg(any(test, feature = "bench-support"))]
        table: Option<Arc<LoopbackTable>>,
    }

    impl Client {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        #[cfg(not(any(test, feature = "bench-support")))]
        pub(crate) fn builder() -> ClientBuilder {
            ClientBuilder {
                inner: reqwest::Client::builder(),
                limit: None,
            }
        }

        #[cfg(any(test, feature = "bench-support"))]
        pub(crate) fn builder() -> ClientBuilder {
            // Ignore environment/system proxies, and pin localhost instead
            // of trusting DNS or the machine's hosts file. The policy checks
            // every redirect before reqwest can follow it.
            ClientBuilder {
                inner: reqwest::Client::builder()
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
                limit: None,
                #[cfg(test)]
                timeouts: (None, None),
            }
        }

        pub(crate) fn get(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
            self.request(self.inner.get(url))
        }

        pub(crate) fn post(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
            self.request(self.inner.post(url))
        }

        fn request(&self, inner: reqwest::RequestBuilder) -> RequestBuilder {
            RequestBuilder {
                inner,
                limit: self.limit,
                #[cfg(any(test, feature = "bench-support"))]
                table: self.table.clone(),
            }
        }
    }

    impl Default for Client {
        #[cfg(not(any(test, feature = "bench-support")))]
        fn default() -> Self {
            Self {
                inner: reqwest::Client::new(),
                limit: None,
            }
        }

        #[cfg(any(test, feature = "bench-support"))]
        fn default() -> Self {
            match Self::builder().build() {
                Ok(client) => client,
                Err(error) => panic!("#182 rule 28: cannot build isolated HTTP client: {error}"),
            }
        }
    }

    /// No proxy and `localhost` pinned as [`Client::builder`]; no redirect
    /// followed, since the SDK refuses a redirect status before its body
    /// (bsv-rs #320); the timeouts and idle pool of bsv-rs's own default.
    #[cfg(any(test, feature = "bench-support"))]
    pub(crate) fn sdk_transport_client() -> reqwest::Client {
        match Client::builder()
            .inner
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(0)
            .build()
        {
            Ok(client) => client,
            Err(error) => panic!("#182 rule 28: cannot build isolated SDK client: {error}"),
        }
    }

    pub(crate) struct ClientBuilder {
        inner: reqwest::ClientBuilder,
        limit: Option<usize>,
        /// The two settings a builder can differ in (`timeout`,
        /// `connect_timeout`): the key of a shared client. The answer
        /// bound is the wrapper's, not reqwest's, so it is not in the key.
        #[cfg(test)]
        timeouts: (Option<Duration>, Option<Duration>),
    }

    impl ClientBuilder {
        pub(crate) fn timeout(self, timeout: Duration) -> Self {
            Self {
                inner: self.inner.timeout(timeout),
                limit: self.limit,
                #[cfg(test)]
                timeouts: (Some(timeout), self.timeouts.1),
            }
        }

        pub(crate) fn connect_timeout(self, timeout: Duration) -> Self {
            Self {
                inner: self.inner.connect_timeout(timeout),
                limit: self.limit,
                #[cfg(test)]
                timeouts: (self.timeouts.0, Some(timeout)),
            }
        }

        /// The door's bound on the answer it reads: `text`, `json` and
        /// `bytes` stop at `limit` bytes with [`RequestError::TooLarge`].
        pub(crate) fn answer_limit(self, limit: usize) -> Self {
            Self {
                limit: Some(limit),
                ..self
            }
        }

        #[cfg(not(any(test, feature = "bench-support")))]
        pub(crate) fn build(self) -> std::result::Result<Client, reqwest::Error> {
            Ok(Client {
                inner: self.inner.build()?,
                limit: self.limit,
            })
        }

        #[cfg(any(test, feature = "bench-support"))]
        pub(crate) fn build(self) -> std::result::Result<Client, reqwest::Error> {
            let table = loopback_table::current();
            #[cfg(test)]
            if let Some(client) = shared::lookup(self.timeouts) {
                return Ok(Client {
                    inner: client.inner,
                    limit: self.limit,
                    table,
                });
            }
            let client = Client {
                inner: self.inner.build()?,
                limit: self.limit,
                table,
            };
            #[cfg(test)]
            shared::keep(self.timeouts, &client);
            Ok(client)
        }
    }

    pub(crate) struct RequestBuilder {
        inner: reqwest::RequestBuilder,
        limit: Option<usize>,
        #[cfg(any(test, feature = "bench-support"))]
        table: Option<Arc<LoopbackTable>>,
    }

    impl RequestBuilder {
        pub(crate) fn header(mut self, key: &str, value: impl AsRef<str>) -> Self {
            self.inner = self.inner.header(key, value.as_ref());
            self
        }

        pub(crate) fn headers(mut self, headers: reqwest::header::HeaderMap) -> Self {
            self.inner = self.inner.headers(headers);
            self
        }

        pub(crate) fn timeout(mut self, timeout: Duration) -> Self {
            self.inner = self.inner.timeout(timeout);
            self
        }

        pub(crate) fn json<T: serde::Serialize + ?Sized>(mut self, json: &T) -> Self {
            self.inner = self.inner.json(json);
            self
        }

        pub(crate) fn body(mut self, body: impl Into<reqwest::Body>) -> Self {
            self.inner = self.inner.body(body);
            self
        }

        #[cfg(not(any(test, feature = "bench-support")))]
        pub(crate) async fn send(self) -> std::result::Result<Response, RequestError> {
            let inner = self.inner.send().await?;
            Ok(Response {
                inner,
                limit: self.limit,
            })
        }

        #[cfg(any(test, feature = "bench-support"))]
        pub(crate) async fn send(self) -> std::result::Result<Response, RequestError> {
            let limit = self.limit;
            let (client, request) = self.inner.build_split();
            let request = request?;
            // A table client opens no socket for any host, so nothing it
            // sends can leave the machine: #182 holds without the check.
            if let Some(table) = self.table {
                let inner = table.answer(request.method(), request.url().clone())?;
                return Ok(Response { inner, limit });
            }
            check_url(request.url())?;
            client
                .execute(request)
                .await
                .map(|inner| Response {
                    inner: loopback_table::Response::Socket(inner),
                    limit,
                })
                .map_err(|error| {
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

    #[cfg(not(any(test, feature = "bench-support")))]
    type Inner = reqwest::Response;
    #[cfg(any(test, feature = "bench-support"))]
    type Inner = loopback_table::Response;

    /// An answer, read through the door's bound.
    #[derive(Debug)]
    pub(crate) struct Response {
        inner: Inner,
        limit: Option<usize>,
    }

    /// The body as chunks, whatever the inner response is.
    fn chunks(inner: Inner) -> BoxStream<'static, std::result::Result<Vec<u8>, RequestError>> {
        #[cfg(not(any(test, feature = "bench-support")))]
        {
            inner
                .bytes_stream()
                .map(|chunk| chunk.map(|chunk| chunk.to_vec()).map_err(Into::into))
                .boxed()
        }
        #[cfg(any(test, feature = "bench-support"))]
        {
            inner.bytes_stream()
        }
    }

    impl Response {
        pub(crate) fn status(&self) -> reqwest::StatusCode {
            self.inner.status()
        }

        pub(crate) fn headers(&self) -> &reqwest::header::HeaderMap {
            self.inner.headers()
        }

        pub(crate) fn error_for_status(self) -> std::result::Result<Self, RequestError> {
            Ok(Self {
                inner: self.inner.error_for_status()?,
                limit: self.limit,
            })
        }

        /// The one read of an answer: whole when the door names no bound,
        /// else chunk by chunk until the bound is passed, where it stops
        /// (a declared length past the bound stops it before any read).
        pub(crate) async fn bytes(self) -> std::result::Result<Vec<u8>, RequestError> {
            let Some(limit) = self.limit else {
                #[cfg(not(any(test, feature = "bench-support")))]
                return Ok(self.inner.bytes().await?.to_vec());
                #[cfg(any(test, feature = "bench-support"))]
                return self.inner.bytes().await;
            };
            let declared = self
                .inner
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            if declared.is_some_and(|declared| declared > limit as u64) {
                return Err(RequestError::TooLarge { limit });
            }
            let mut body = Vec::new();
            let mut stream = chunks(self.inner);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if chunk.len() > limit - body.len() {
                    return Err(RequestError::TooLarge { limit });
                }
                body.extend_from_slice(&chunk);
            }
            Ok(body)
        }

        /// reqwest's `text` for UTF-8: replacement for bad sequences, a
        /// leading BOM dropped.
        pub(crate) async fn text(self) -> std::result::Result<String, RequestError> {
            if self.limit.is_none() {
                #[cfg(not(any(test, feature = "bench-support")))]
                return Ok(self.inner.text().await?);
                #[cfg(any(test, feature = "bench-support"))]
                return self.inner.text().await;
            }
            let body = self.bytes().await?;
            let bom = usize::from(body.get(..3) == Some(&[0xEF, 0xBB, 0xBF][..])) * 3;
            Ok(String::from_utf8_lossy(&body[bom..]).into_owned())
        }

        pub(crate) async fn json<T: serde::de::DeserializeOwned>(
            self,
        ) -> std::result::Result<T, RequestError> {
            if self.limit.is_none() {
                #[cfg(not(any(test, feature = "bench-support")))]
                return Ok(self.inner.json().await?);
                #[cfg(any(test, feature = "bench-support"))]
                return self.inner.json().await;
            }
            let body = self.bytes().await?;
            serde_json::from_slice(&body).map_err(|_| RequestError::Decode)
        }

        /// The body as a stream of chunks, unbounded: a stream's door
        /// bounds what it parses (the Arcade event stream caps a line).
        pub(crate) fn bytes_stream(
            self,
        ) -> BoxStream<'static, std::result::Result<Vec<u8>, RequestError>> {
            chunks(self.inner)
        }
    }

    /// A test's opt-in to one client per setting on its thread (#634).
    /// Building a client is dear on the ubuntu runner (native-tls there is
    /// OpenSSL; cheap on macOS): in run 41578469987 a test that builds one
    /// `Services` took 0.28 s, the 27-row status table 11.2 s for its 54,
    /// and the 81-row table 164 s for its 648. Every client this builder
    /// makes is the same but for its two timeouts, so a table's providers
    /// can share one; each row still builds its own `Services`, gates and
    /// collections.
    #[cfg(test)]
    pub(crate) mod shared {
        use std::cell::RefCell;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use super::Client;

        type Key = (Option<Duration>, Option<Duration>);

        thread_local! {
            static CLIENTS: RefCell<Option<SharedClients>> = const { RefCell::new(None) };
        }

        /// One client per setting, for the builds made under it.
        #[derive(Clone, Default)]
        pub(crate) struct SharedClients(Arc<Mutex<HashMap<Key, Client>>>);

        impl SharedClients {
            /// `build` (no await inside, so on one thread) with every client
            /// it makes taken from this set: a multi-thread test's builds
            /// share one set from whichever worker they run on.
            pub(crate) fn building<R>(&self, build: impl FnOnce() -> R) -> R {
                let before = CLIENTS.with(|clients| clients.replace(Some(self.clone())));
                let _restore = ShareClients(before);
                build()
            }
        }

        /// While held, every client this thread builds with one setting is
        /// the same client. Dropped, the thread builds as it did before.
        pub(crate) struct ShareClients(Option<SharedClients>);

        /// Share clients on this thread until the guard drops (the table
        /// tests run on `#[tokio::test]`'s current thread).
        pub(crate) fn share_clients_on_this_thread() -> ShareClients {
            ShareClients(CLIENTS.with(|clients| clients.replace(Some(SharedClients::default()))))
        }

        impl Drop for ShareClients {
            fn drop(&mut self) {
                let before = self.0.take();
                CLIENTS.with(|clients| *clients.borrow_mut() = before);
            }
        }

        pub(super) fn lookup(key: Key) -> Option<Client> {
            CLIENTS.with(|clients| {
                clients
                    .borrow()
                    .as_ref()?
                    .0
                    .lock()
                    .unwrap()
                    .get(&key)
                    .cloned()
            })
        }

        pub(super) fn keep(key: Key, client: &Client) {
            CLIENTS.with(|clients| {
                if let Some(set) = clients.borrow().as_ref() {
                    set.0.lock().unwrap().insert(key, client.clone());
                }
            });
        }
    }
}
