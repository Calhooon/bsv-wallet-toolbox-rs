//! Rule 28 witness (bsv-stack-lean `docs/p0/rule-28-explorer-calls.md`,
//! T16 to T19): the embedded header store has no explorer source.
//!
//! Its four ingestors asked WhatsOnChain (REST and websocket) and a CDN for
//! headers and stored them with no proof of work, difficulty, checkpoint or
//! anchor check. The header service (`chaintracks_url`) answers everything
//! the store answers, under those rules. This test reads the module's
//! sources and fails if a third-party host comes back.

use std::path::{Path, PathBuf};

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn the_embedded_header_store_names_no_third_party_host() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/chaintracks");
    let mut files = Vec::new();
    rust_sources(&root, &mut files);
    assert!(!files.is_empty(), "the module's sources were found");

    let needles = [
        "whatsonchain",
        "bitails",
        "babbage",
        "taal.com",
        "gorillapool",
        "https://",
        "http://",
        "wss://",
        "ws://",
        "reqwest",
        "tungstenite",
    ];
    let mut found = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("read").to_lowercase();
        for (n, line) in text.lines().enumerate() {
            for needle in needles {
                if line.contains(needle) {
                    found.push(format!(
                        "{}:{}: {}",
                        file.strip_prefix(&root).unwrap().display(),
                        n + 1,
                        needle
                    ));
                }
            }
        }
    }
    assert!(
        found.is_empty(),
        "the embedded header store reaches for a third party ({} lines):\n{}",
        found.len(),
        found
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
