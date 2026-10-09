//! Rule 28 witness: "unspent" from any provider is a hint.
//!
//! The owner's ruling of 2026-10-09 (bsv-stack-lean `NORTH-STAR.md`): a
//! positive unspent answer from an explorer is a hint and never a verdict;
//! the wallet's shared storage is the verdict for its own devices' spends.
//! No header or proof says an output is unspent, so nothing in this crate
//! can hold "unspent" as a chain fact. The type says so (`UnspentHint`),
//! and no site words an explorer's answer as verified. This test reads the
//! crate's sources and fails where the hint is named as a verdict.

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
fn no_site_names_an_explorers_unspent_as_a_verdict() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_sources(&manifest.join("src"), &mut files);
    assert!(!files.is_empty(), "the crate's sources were found");

    // The variant without its tier, and the wordings that call an
    // explorer's unspent set a verification.
    let needles = [
        "utxoverdict::unspent =>",
        "utxoverdict::unspent)",
        "utxoverdict::unspent,",
        "utxoverdict::unspent |",
        "utxoverdict::unspent {",
        "verifiably unspent",
        "utxo verified",
        "verified unspent",
    ];
    let mut found = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("read").to_lowercase();
        for (n, line) in text.lines().enumerate() {
            for needle in needles {
                if line.contains(needle) {
                    found.push(format!(
                        "{}:{}: {}",
                        file.strip_prefix(manifest).unwrap().display(),
                        n + 1,
                        needle
                    ));
                }
            }
        }
    }
    assert!(
        found.is_empty(),
        "an explorer's unspent answer is named as a verdict ({} lines):\n{}",
        found.len(),
        found.join("\n")
    );
}
