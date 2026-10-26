//! Shared harness for the fuzz targets: one tokio runtime, and one loopback
//! HTTP server that answers a provider's requests with the fuzzer's bytes.
//!
//! A provider target arms a path prefix with [`answer`]; every request whose
//! path starts with it gets the fuzzed status and body, a `/headers` request
//! gets a fixed Chaintracks header (so a ladder that checks a header before
//! the explorer still reaches the explorer), and anything else gets a 404. Nothing leaves
//! the machine: the server is on 127.0.0.1, and every other HTTPS request is
//! sent to a refused local proxy.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Mutex, OnceLock};

use bsv_wallet_toolbox_rs::benchmark_support::local_explorer_services;
use bsv_wallet_toolbox_rs::{Services, ServicesOptions};

/// Statuses a fuzzed answer may carry. Never a 429: it would close the rung
/// for its Retry-After and starve every later input. Never a 5xx: its body
/// is not parsed, and the explorers wait two seconds before their retry.
const STATUSES: [u16; 12] = [200, 200, 200, 200, 201, 202, 400, 404, 409, 422, 460, 465];

/// The Chaintracks frame every `/headers` request is answered with.
const HEADER: &str = r#"{"status":"success","value":{"version":1,"previousHash":"0000000000000000000000000000000000000000000000000000000000000000","merkleRoot":"0000000000000000000000000000000000000000000000000000000000000000","time":1700000000,"bits":486604799,"nonce":1,"height":800000,"hash":"0000000000000000000000000000000000000000000000000000000000000000"}}"#;

struct Armed {
    prefix: &'static str,
    status: u16,
    body: Vec<u8>,
}

static ARMED: Mutex<Armed> = Mutex::new(Armed {
    prefix: "/",
    status: 200,
    body: Vec::new(),
});

/// The runtime every async target blocks on.
pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    })
}

/// The loopback server's base URL, started when a target asks for it.
pub fn base() -> &'static str {
    static BASE: OnceLock<String> = OnceLock::new();
    BASE.get_or_init(|| {
        // Any request not for the loopback server goes to a refused port.
        std::env::set_var("HTTPS_PROXY", "http://127.0.0.1:9");
        std::env::set_var("HTTP_PROXY", "http://127.0.0.1:9");
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let _ = serve(stream);
            }
        });
        base
    })
}

/// Arm `prefix`: the next requests under it get `data`'s answer (its leading
/// byte picks the status, the rest is the body).
pub fn answer(prefix: &'static str, data: &[u8]) {
    let (status, body) = match data.split_first() {
        Some((s, rest)) => (STATUSES[*s as usize % STATUSES.len()], rest.to_vec()),
        None => (200, Vec::new()),
    };
    *ARMED.lock().unwrap_or_else(|e| e.into_inner()) = Armed {
        prefix,
        status,
        body,
    };
}

/// The real service ladders with every explorer, ARC and the header service
/// on the loopback server: `/woc`, `/bitails`, `/bb`, `/arc`, `/headers`.
pub fn services() -> &'static Services {
    static SERVICES: OnceLock<Services> = OnceLock::new();
    SERVICES.get_or_init(|| {
        let base = base();
        let mut options = ServicesOptions::mainnet()
            .with_arc(format!("{base}/arc"), None)
            .with_chaintracks_url(format!("{base}/headers"));
        options.bhs_url = None;
        options.arc_gorillapool_url = None;
        local_explorer_services(
            options,
            &format!("{base}/woc"),
            &format!("{base}/bb"),
            &format!("{base}/bitails"),
        )
        .expect("local services")
    })
}

fn serve(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let length = head
        .lines()
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while buf.len() < head_end + length {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let path = head.split_whitespace().nth(1).unwrap_or("/").to_string();
    let (status, body) = {
        let armed = ARMED.lock().unwrap_or_else(|e| e.into_inner());
        if path.starts_with(armed.prefix) {
            (armed.status, armed.body.clone())
        } else if path.starts_with("/headers") {
            (200, HEADER.as_bytes().to_vec())
        } else {
            (404, Vec::new())
        }
    };
    let head = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(&body)?;
    stream.flush()
}

/// A txid the ladders are asked about (the answers are the fuzzer's).
pub const TXID: &str = "1111111111111111111111111111111111111111111111111111111111111111";

/// One round of every read ladder an explorer serves, with the explorer at
/// `prefix` answering `data`.
pub fn explore(prefix: &'static str, data: &[u8]) {
    use bsv_wallet_toolbox_rs::WalletServices;
    let services = services();
    answer(prefix, data);
    runtime().block_on(async {
        let txids = [TXID.to_string()];
        let _ = services.get_raw_tx(TXID, false).await;
        let _ = services.get_merkle_path(TXID, false).await;
        let _ = services.get_status_for_txids(&txids, false).await;
        let _ = services.get_utxo_status(TXID, None, None, false).await;
        let _ = services.is_utxo_with_spender(TXID, 0, &[0x51]).await;
        let _ = services.get_utxo_leads(TXID, None).await;
    });
}

/// A one-input, one-output transaction: its raw hex, its txid and a BEEF
/// carrying it, for the broadcasters' submit paths (the answers are fuzzed).
pub fn sample() -> &'static (String, String, Vec<u8>) {
    static SAMPLE: OnceLock<(String, String, Vec<u8>)> = OnceLock::new();
    SAMPLE.get_or_init(|| {
        let raw = format!(
            "0100000001{}00000000{}ffffffff01{}015100000000",
            "00".repeat(32),
            "00",
            "00".repeat(8)
        );
        let mut beef = bsv_rs::transaction::Beef::new();
        let txid = beef
            .merge_raw_tx(hex::decode(&raw).expect("hex"), None)
            .txid();
        (raw, txid, beef.to_binary())
    })
}
