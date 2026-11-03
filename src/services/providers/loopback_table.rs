//! The loopback table (#683): in test and bench builds, a client built
//! under [`LoopbackTable::building`] answers every request from the test's
//! own table, with no socket, no TLS, no DNS and no real timeout. The
//! providers reach it through the same `Client`/`RequestBuilder` they
//! already use under the seam; the ladders' failover, pacing and call
//! history run over it unchanged.
//!
//! The texts a host sees are the socket transport's for the same answer:
//! a recorded answer is read by the providers as a socket's would be, and
//! the texts the transport itself produces (a send fault, an error status,
//! a body that fails to decode) are reqwest 0.12's, byte for byte. The
//! differences are named in `docs/census/ci-speed-loop-4.md`.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use super::isolated::RequestError;

thread_local! {
    static TABLE: RefCell<Option<Arc<LoopbackTable>>> = const { RefCell::new(None) };
}

/// One answer the table gives.
#[derive(Clone, Debug)]
pub enum LoopbackAnswer {
    /// A recorded answer: the status, the headers that matter, the body.
    Respond {
        /// The HTTP status.
        status: u16,
        /// The response headers (name, value).
        headers: Vec<(String, String)>,
        /// The response body.
        body: Vec<u8>,
    },
    /// The send fault the socket transport reports for a refused
    /// connection, a timeout, a DNS or a TLS fault, all one text in
    /// reqwest 0.12: `error sending request for url (<url>)`.
    SendFault,
}

impl LoopbackAnswer {
    /// A recorded answer with no headers.
    pub fn status(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self::Respond {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    /// A recorded JSON answer (`Content-Type: application/json`).
    pub fn json(status: u16, body: &serde_json::Value) -> Self {
        Self::status(status, body.to_string()).with_header("content-type", "application/json")
    }

    /// The answer with one more header; a send fault is unchanged.
    pub fn with_header(self, name: &str, value: &str) -> Self {
        match self {
            Self::Respond {
                status,
                mut headers,
                body,
            } => {
                headers.push((name.to_string(), value.to_string()));
                Self::Respond {
                    status,
                    headers,
                    body,
                }
            }
            Self::SendFault => Self::SendFault,
        }
    }
}

#[derive(Debug)]
struct Row {
    method: Method,
    host: String,
    path: String,
    answer: LoopbackAnswer,
}

impl Row {
    /// The host is the URL's host, or its host and port; the path is the
    /// URL's path exactly, its path and query when the row's has a `?`, or
    /// a prefix of it when the row's ends in `*`.
    fn matches(&self, method: &Method, url: &Url) -> bool {
        let host = url.host_str().unwrap_or_default();
        let authority = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        if self.method != *method || (self.host != host && self.host != authority) {
            return false;
        }
        if let Some(prefix) = self.path.strip_suffix('*') {
            url.path().starts_with(prefix)
        } else if self.path.contains('?') {
            match url.query() {
                Some(query) => self.path == format!("{}?{query}", url.path()),
                None => false,
            }
        } else {
            self.path == url.path()
        }
    }
}

/// The test's table: the rows (method, host, path, answer) and every
/// request the clients built under it sent, in order.
#[derive(Debug, Default)]
pub struct LoopbackTable {
    rows: Mutex<Vec<Row>>,
    requests: Mutex<Vec<String>>,
}

impl LoopbackTable {
    /// An empty table: every request is a send fault until a row answers.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Answer `method host path` with `answer`. Rows are tried in the order
    /// added; the earliest that matches answers.
    pub fn on(&self, method: &str, host: &str, path: &str, answer: LoopbackAnswer) -> &Self {
        let method = match Method::from_bytes(method.as_bytes()) {
            Ok(method) => method,
            Err(error) => panic!("#683 loopback table: bad method {method:?}: {error}"),
        };
        lock(&self.rows).push(Row {
            method,
            host: host.to_string(),
            path: path.to_string(),
            answer,
        });
        self
    }

    /// Every request sent through this table, as `METHOD url`, in order.
    pub fn requests(&self) -> Vec<String> {
        lock(&self.requests).clone()
    }

    /// `build` (no await inside, so on one thread) with every client it
    /// builds answered from this table, wherever the client is later used.
    /// Clients built outside it keep the socket transport and #182's check.
    pub fn building<R>(self: &Arc<Self>, build: impl FnOnce() -> R) -> R {
        let before = TABLE.with(|table| table.replace(Some(self.clone())));
        let _restore = Restore(before);
        build()
    }

    pub(super) fn answer(&self, method: &Method, url: Url) -> Result<Response, RequestError> {
        lock(&self.requests).push(format!("{method} {url}"));
        let answer = lock(&self.rows)
            .iter()
            .find(|row| row.matches(method, &url))
            .map_or(LoopbackAnswer::SendFault, |row| row.answer.clone());
        match answer {
            LoopbackAnswer::SendFault => Err(RequestError::Loopback(format!(
                "error sending request for url ({url})"
            ))),
            LoopbackAnswer::Respond {
                status,
                headers,
                body,
            } => {
                let status = match StatusCode::from_u16(status) {
                    Ok(status) => status,
                    Err(error) => panic!("#683 loopback table: bad status {status}: {error}"),
                };
                let mut map = HeaderMap::new();
                for (name, value) in headers {
                    match (
                        HeaderName::try_from(name.as_str()),
                        HeaderValue::from_str(&value),
                    ) {
                        (Ok(name), Ok(value)) => {
                            map.append(name, value);
                        }
                        _ => panic!("#683 loopback table: bad header {name:?}: {value:?}"),
                    }
                }
                Ok(Response::Table(TableResponse {
                    status,
                    headers: map,
                    url,
                    body,
                }))
            }
        }
    }
}

/// A test that panicked holding the table leaves its rows as they were.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The table clients built now take, if any.
pub(super) fn current() -> Option<Arc<LoopbackTable>> {
    TABLE.with(|table| table.borrow().clone())
}

struct Restore(Option<Arc<LoopbackTable>>);

impl Drop for Restore {
    fn drop(&mut self) {
        let before = self.0.take();
        TABLE.with(|table| *table.borrow_mut() = before);
    }
}

/// A response from the socket or from the table, read through reqwest's
/// own methods. A body is bytes (`Vec<u8>`), since this crate does not
/// name the `bytes` crate.
#[derive(Debug)]
pub(crate) enum Response {
    Socket(reqwest::Response),
    Table(TableResponse),
}

#[derive(Debug)]
pub(crate) struct TableResponse {
    status: StatusCode,
    headers: HeaderMap,
    url: Url,
    body: Vec<u8>,
}

/// reqwest 0.12's text for a body that fails to decode (`Kind::Decode`,
/// no URL).
const DECODE_TEXT: &str = "error decoding response body";

impl Response {
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::Socket(response) => response.status(),
            Self::Table(response) => response.status,
        }
    }

    pub(crate) fn headers(&self) -> &HeaderMap {
        match self {
            Self::Socket(response) => response.headers(),
            Self::Table(response) => &response.headers,
        }
    }

    /// reqwest's `error_for_status`: an error status is reqwest's
    /// `HTTP status client error (404 Not Found) for url (<url>)`.
    pub(crate) fn error_for_status(self) -> Result<Self, RequestError> {
        match self {
            Self::Socket(response) => Ok(Self::Socket(response.error_for_status()?)),
            Self::Table(response) => {
                let status = response.status;
                let prefix = if status.is_client_error() {
                    "HTTP status client error"
                } else if status.is_server_error() {
                    "HTTP status server error"
                } else {
                    return Ok(Self::Table(response));
                };
                Err(RequestError::Loopback(format!(
                    "{prefix} ({status}) for url ({})",
                    response.url
                )))
            }
        }
    }

    pub(crate) async fn bytes(self) -> Result<Vec<u8>, RequestError> {
        match self {
            Self::Socket(response) => Ok(response.bytes().await?.to_vec()),
            Self::Table(response) => Ok(response.body),
        }
    }

    /// reqwest's `text`: UTF-8 with replacement, a leading BOM dropped
    /// (the table reads no other charset).
    pub(crate) async fn text(self) -> Result<String, RequestError> {
        match self {
            Self::Socket(response) => Ok(response.text().await?),
            Self::Table(response) => {
                let body = response.body;
                let bom = usize::from(body.get(..3) == Some(&[0xEF, 0xBB, 0xBF][..])) * 3;
                Ok(String::from_utf8_lossy(&body[bom..]).into_owned())
            }
        }
    }

    pub(crate) async fn json<T: serde::de::DeserializeOwned>(self) -> Result<T, RequestError> {
        match self {
            Self::Socket(response) => Ok(response.json().await?),
            Self::Table(response) => serde_json::from_slice(&response.body)
                .map_err(|_| RequestError::Loopback(DECODE_TEXT.to_string())),
        }
    }

    /// The body as a stream of chunks; the table's is one chunk.
    pub(crate) fn bytes_stream(
        self,
    ) -> futures::stream::BoxStream<'static, Result<Vec<u8>, RequestError>> {
        use futures::StreamExt;
        match self {
            Self::Socket(response) => response
                .bytes_stream()
                .map(|chunk| chunk.map(|chunk| chunk.to_vec()).map_err(Into::into))
                .boxed(),
            Self::Table(response) => futures::stream::iter([Ok(response.body)]).boxed(),
        }
    }
}

#[cfg(test)]
#[path = "loopback_table_tests.rs"]
mod tests;
