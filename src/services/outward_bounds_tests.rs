//! Every outward door's times and answer bound, through the real provider
//! code against a raw loopback listener: (a) a server that accepts and
//! never answers, (b) an answer that never ends, counted on the server's
//! side, (c) an honest answer at the bound.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc as StdArc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::services::providers::http_client::{self, Client, RequestError};

/// What the listener does with each connection.
#[derive(Clone)]
enum Answer {
    /// Accept, read the request, never answer.
    Stall,
    /// A 200 whose chunked body never ends; the bytes written are counted.
    Endless,
    /// A 200 with this chunked body (no declared length).
    Body(StdArc<Vec<u8>>),
}

struct Listener {
    url: String,
    written: StdArc<AtomicUsize>,
    connections: StdArc<AtomicUsize>,
}

/// A raw listener answering every connection with `answers[i]` (the last
/// repeated): mockito cannot hold a connection open without an answer.
async fn listen(answers: Vec<Answer>) -> Listener {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let written = StdArc::new(AtomicUsize::new(0));
    let connections = StdArc::new(AtomicUsize::new(0));
    let (w, c) = (written.clone(), connections.clone());
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let n = c.fetch_add(1, Ordering::SeqCst);
            let answer = answers[n.min(answers.len() - 1)].clone();
            let w = w.clone();
            tokio::spawn(async move {
                let mut head = vec![0u8; 64 * 1024];
                let _ = socket.read(&mut head).await;
                let start = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
                match answer {
                    Answer::Stall => {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                    }
                    Answer::Endless => {
                        if socket.write_all(start).await.is_err() {
                            return;
                        }
                        let chunk = vec![b'7'; 64 * 1024];
                        let frame =
                            [format!("{:x}\r\n", chunk.len()).as_bytes(), &chunk, b"\r\n"].concat();
                        while socket.write_all(&frame).await.is_ok() {
                            w.fetch_add(chunk.len(), Ordering::SeqCst);
                        }
                    }
                    Answer::Body(body) => {
                        let _ = socket.write_all(start).await;
                        for chunk in body.chunks(64 * 1024) {
                            let frame =
                                [format!("{:x}\r\n", chunk.len()).as_bytes(), chunk, b"\r\n"]
                                    .concat();
                            if socket.write_all(&frame).await.is_err() {
                                return;
                            }
                            w.fetch_add(chunk.len(), Ordering::SeqCst);
                        }
                        let _ = socket.write_all(b"0\r\n\r\n").await;
                        let _ = socket.shutdown().await;
                    }
                }
            });
        }
    });
    Listener {
        url,
        written,
        connections,
    }
}

/// The test's own bound: a hang fails here, not in the harness.
async fn bounded<T>(limit: Duration, call: impl std::future::Future<Output = T>) -> (T, Duration) {
    let started = Instant::now();
    let out = tokio::time::timeout(limit, call)
        .await
        .unwrap_or_else(|_| panic!("the door did not return within {limit:?}: it hangs"));
    (out, started.elapsed())
}

/// (b)'s reading: the read stopped near the bound (what the kernel buffers
/// beside it aside), and the door's text names the bound.
fn stopped_at(listener: &Listener, limit: usize, text: &str) {
    // Let the server's last writes fail.
    let written = listener.written.load(Ordering::SeqCst);
    assert!(
        written <= limit + 16 * 1024 * 1024,
        "the read did not stop at the bound: {written} bytes written past a bound of {limit}"
    );
    assert!(
        text.contains(&format!("bound of {limit} bytes")),
        "not the bound's fault: {text}"
    );
}

/// The one read every door shares, at a door's bound: exactly the bound
/// is read whole; a byte past it is the bound's fault.
async fn reads_whole_at_and_refuses_past(limit: usize) {
    let body = StdArc::new(vec![b'7'; limit]);
    let over = StdArc::new(vec![b'7'; limit + 1]);
    let l = listen(vec![Answer::Body(body), Answer::Body(over)]).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(50))
        .connect_timeout(http_client::CONNECT_TIMEOUT)
        .answer_limit(limit)
        .build()
        .unwrap();
    let read = client.get(&l.url).send().await.unwrap().bytes().await;
    assert_eq!(read.map(|b| b.len()).ok(), Some(limit), "at {limit}");
    let read = client.get(&l.url).send().await.unwrap().bytes().await;
    assert!(
        matches!(read, Err(RequestError::TooLarge { limit: l }) if l == limit),
        "past {limit}: {read:?}"
    );
    assert_eq!(l.connections.load(Ordering::SeqCst), 2);
}

/// The header services (BHS, Chaintracks) and the explorer header fallback.
mod headers {
    use super::*;
    use crate::services::providers::http_client::HEADER_ANSWER_MAX_BYTES;
    use crate::services::providers::{BlockHeaderService, ChaintracksServiceClient};

    #[tokio::test]
    async fn a_stalled_header_service_returns_within_its_total_time() {
        let l = listen(vec![Answer::Stall]).await;
        let bhs = BlockHeaderService::from_url(&l.url);
        let ct = ChaintracksServiceClient::from_url(&l.url);
        let ((b, c), took) = bounded(Duration::from_secs(45), async {
            tokio::join!(bhs.current_height(), ct.get_present_height())
        })
        .await;
        assert!(took < Duration::from_secs(40), "{took:?}");
        assert!(b.is_err() && c.is_err(), "{b:?} {c:?}");
    }

    #[tokio::test]
    async fn an_endless_header_service_answer_stops_at_its_bound() {
        let l = listen(vec![Answer::Endless]).await;
        let (r, _) = bounded(
            Duration::from_secs(40),
            BlockHeaderService::from_url(&l.url).current_height(),
        )
        .await;
        stopped_at(&l, HEADER_ANSWER_MAX_BYTES, &format!("{r:?}"));
        assert!(matches!(r, Err(crate::Error::NetworkError(_))), "{r:?}");
        let l = listen(vec![Answer::Endless]).await;
        let (r, _) = bounded(
            Duration::from_secs(40),
            ChaintracksServiceClient::from_url(&l.url).get_present_height(),
        )
        .await;
        stopped_at(&l, HEADER_ANSWER_MAX_BYTES, &format!("{r:?}"));
        assert!(matches!(r, Err(crate::Error::NetworkError(_))), "{r:?}");
    }

    #[tokio::test]
    async fn a_header_answer_at_its_bound_is_read_whole() {
        reads_whole_at_and_refuses_past(HEADER_ANSWER_MAX_BYTES).await;
    }
}

/// The explorers: `WhatsOnChain`, `Bitails`, `BananaBlocks`.
mod explorers {
    use super::*;

    const TXID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    use crate::services::providers::http_client::TX_ANSWER_MAX_BYTES;
    use crate::services::providers::{
        BananaBlocks, Bitails, BitailsConfig, WhatsOnChain, WhatsOnChainConfig,
    };
    use crate::services::Chain;

    fn woc(url: &str, secs: u64) -> WhatsOnChain {
        WhatsOnChain::with_base_url_and_config(
            Chain::Main,
            url,
            WhatsOnChainConfig {
                timeout_secs: Some(secs),
                ..Default::default()
            },
        )
    }

    fn bitails(url: &str, secs: u64) -> Bitails {
        Bitails::with_base_url_and_config(
            Chain::Main,
            url,
            BitailsConfig {
                timeout_secs: Some(secs),
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn a_stalled_whatsonchain_returns_within_its_total_time() {
        let l = listen(vec![Answer::Stall]).await;
        let (r, took) = bounded(Duration::from_secs(10), woc(&l.url, 2).get_raw_tx(TXID)).await;
        assert!(took < Duration::from_secs(5), "{took:?}");
        assert!(!matches!(&r, Ok(x) if x.raw_tx.is_some()), "{r:?}");
    }

    #[tokio::test]
    async fn a_stalled_bitails_returns_within_its_total_time() {
        let l = listen(vec![Answer::Stall]).await;
        let (r, took) = bounded(Duration::from_secs(10), bitails(&l.url, 2).get_raw_tx(TXID)).await;
        assert!(took < Duration::from_secs(5), "{took:?}");
        assert!(!matches!(&r, Ok(x) if x.raw_tx.is_some()), "{r:?}");
    }

    #[tokio::test]
    async fn a_stalled_bananablocks_returns_within_its_total_time() {
        let l = listen(vec![Answer::Stall]).await;
        let bb = BananaBlocks::with_base_unshared_timeout(&l.url, Some(2));
        let (r, took) = bounded(Duration::from_secs(10), bb.get_raw_tx(TXID)).await;
        assert!(took < Duration::from_secs(5), "{took:?}");
        assert!(!matches!(&r, Ok(x) if x.raw_tx.is_some()), "{r:?}");
    }

    #[tokio::test]
    async fn an_endless_whatsonchain_answer_stops_at_its_bound() {
        let l = listen(vec![Answer::Endless]).await;
        let (r, _) = bounded(Duration::from_secs(60), woc(&l.url, 50).get_raw_tx(TXID)).await;
        stopped_at(&l, TX_ANSWER_MAX_BYTES, &format!("{r:?}"));
    }

    #[tokio::test]
    async fn an_endless_bitails_answer_stops_at_its_bound() {
        let l = listen(vec![Answer::Endless]).await;
        let (r, _) = bounded(
            Duration::from_secs(60),
            bitails(&l.url, 50).get_raw_tx(TXID),
        )
        .await;
        stopped_at(&l, TX_ANSWER_MAX_BYTES, &format!("{r:?}"));
    }

    #[tokio::test]
    async fn an_endless_bananablocks_answer_stops_at_its_bound() {
        let l = listen(vec![Answer::Endless]).await;
        let bb = BananaBlocks::with_base_unshared_timeout(&l.url, Some(50));
        let (r, _) = bounded(Duration::from_secs(60), bb.get_raw_tx(TXID)).await;
        stopped_at(&l, TX_ANSWER_MAX_BYTES, &format!("{r:?}"));
    }

    #[tokio::test]
    async fn an_explorer_answer_at_its_bound_is_read_whole() {
        reads_whole_at_and_refuses_past(TX_ANSWER_MAX_BYTES).await;
    }

    /// The largest honest explorer answer: one transaction of the reference's
    /// largest size, as hex, through `WhatsOnChain`'s raw-tx read.
    #[tokio::test]
    async fn the_largest_transaction_passes_whatsonchain() {
        // A transaction: version, no input, one output with a script of most
        // of 32 MiB, lock time.
        let script_len: usize = 32 * 1024 * 1024 - 64;
        let mut raw = vec![1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0xfe];
        raw.extend_from_slice(&u32::try_from(script_len).unwrap().to_le_bytes());
        raw.extend(std::iter::repeat_n(0x51u8, script_len));
        raw.extend_from_slice(&[0, 0, 0, 0]);
        assert!(raw.len() * 2 <= TX_ANSWER_MAX_BYTES);
        let txid = crate::services::traits::txid_from_raw_tx(&raw);
        let l = listen(vec![Answer::Body(StdArc::new(
            hex::encode(&raw).into_bytes(),
        ))])
        .await;
        let (r, _) = bounded(Duration::from_secs(60), woc(&l.url, 50).get_raw_tx(&txid)).await;
        assert_eq!(r.unwrap().raw_tx.map(|t| t.len()), Some(raw.len()));
    }
}

/// The broadcasters (ARC, Arcade REST) and the fiat rates.
mod broadcasters {
    use super::*;

    const TXID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    use crate::services::providers::http_client::{
        RATES_ANSWER_MAX_BYTES, STATUS_ANSWER_MAX_BYTES,
    };
    use crate::services::providers::{Arc, ArcConfig, Arcade, ArcadeConfig};

    fn arc(url: &str, secs: u64) -> Arc {
        let config = ArcConfig {
            timeout_secs: Some(secs),
            ..Default::default()
        };
        Arc::new(url, Some(config), Some("arc")).unwrap()
    }

    fn arcade(url: &str, secs: u64) -> Arcade {
        let config = ArcadeConfig {
            timeout_secs: Some(secs),
            ..Default::default()
        };
        Arcade::new(url, Some(config), Some("arcade")).unwrap()
    }

    #[tokio::test]
    async fn a_stalled_arc_returns_within_its_total_time() {
        let l = listen(vec![Answer::Stall]).await;
        let (r, took) = bounded(Duration::from_secs(10), arc(&l.url, 2).get_tx_data(TXID)).await;
        assert!(took < Duration::from_secs(5), "{took:?}");
        assert!(r.is_err(), "{r:?}");
    }

    #[tokio::test]
    async fn a_stalled_arcade_returns_within_its_total_time() {
        let l = listen(vec![Answer::Stall]).await;
        let (r, took) = bounded(
            Duration::from_secs(10),
            arcade(&l.url, 2).get_tx_status(TXID),
        )
        .await;
        assert!(took < Duration::from_secs(5), "{took:?}");
        assert!(r.is_err(), "{r:?}");
    }

    #[tokio::test]
    async fn an_endless_arc_answer_stops_at_its_bound() {
        let l = listen(vec![Answer::Endless]).await;
        let (r, _) = bounded(Duration::from_secs(60), arc(&l.url, 50).get_tx_data(TXID)).await;
        stopped_at(&l, STATUS_ANSWER_MAX_BYTES, &format!("{r:?}"));
        assert!(matches!(r, Err(crate::Error::NetworkError(_))), "{r:?}");
    }

    #[tokio::test]
    async fn an_endless_arcade_answer_stops_at_its_bound() {
        let l = listen(vec![Answer::Endless]).await;
        let (r, _) = bounded(
            Duration::from_secs(60),
            arcade(&l.url, 50).get_tx_status(TXID),
        )
        .await;
        stopped_at(&l, STATUS_ANSWER_MAX_BYTES, &format!("{r:?}"));
        assert!(matches!(r, Err(crate::Error::NetworkError(_))), "{r:?}");
    }

    #[tokio::test]
    async fn a_broadcaster_answer_at_its_bound_is_read_whole() {
        reads_whole_at_and_refuses_past(STATUS_ANSWER_MAX_BYTES).await;
    }

    #[tokio::test]
    async fn a_rates_answer_at_its_bound_is_read_whole() {
        reads_whole_at_and_refuses_past(RATES_ANSWER_MAX_BYTES).await;
    }
}

/// The storage client's unauthenticated door.
#[cfg(feature = "remote")]
mod storage {
    use super::*;
    use crate::services::providers::http_client::STORAGE_ANSWER_MAX_BYTES;
    use crate::storage::client::StorageClient;
    use crate::storage::WalletStorageWriter;
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::wallet::ProtoWallet;

    fn client(url: &str, secs: u64) -> StorageClient<ProtoWallet> {
        let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&[7u8; 32]).unwrap()));
        StorageClient::new_unauthenticated(wallet, url).with_timeout(Duration::from_secs(secs))
    }

    #[tokio::test]
    async fn a_stalled_storage_returns_within_its_total_time() {
        let l = listen(vec![Answer::Stall]).await;
        let (r, took) = bounded(Duration::from_secs(10), client(&l.url, 2).make_available()).await;
        assert!(took < Duration::from_secs(5), "{took:?}");
        assert!(matches!(r, Err(crate::Error::NetworkError(_))), "{r:?}");
    }

    #[tokio::test]
    async fn an_endless_storage_answer_stops_at_its_bound() {
        let l = listen(vec![Answer::Endless]).await;
        let (r, _) = bounded(Duration::from_secs(60), client(&l.url, 50).make_available()).await;
        stopped_at(&l, STORAGE_ANSWER_MAX_BYTES, &format!("{r:?}"));
        assert!(matches!(r, Err(crate::Error::NetworkError(_))), "{r:?}");
    }

    #[tokio::test]
    async fn a_storage_answer_at_its_bound_is_read_whole() {
        reads_whole_at_and_refuses_past(STORAGE_ANSWER_MAX_BYTES).await;
    }
}
