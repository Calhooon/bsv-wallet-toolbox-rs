//! Rule 28 witness: the wallet keeps no header store of its own.
//!
//! The header service is the one headers machine (the owner's ruling of
//! 2026-10-09, bsv-stack-lean `NORTH-STAR.md`). Until 0.6.0 the crate
//! carried an embedded header store (`src/chaintracks`) that checked no
//! proof of work, difficulty rule, checkpoint or ancestry, and that nothing
//! in the crate built. Every header question is asked of the header service
//! (`ServicesOptions::chaintracks_url`) and fails closed without one. This
//! test reads the crate's sources and fails if the store comes back.

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
fn the_crate_carries_no_header_store_of_its_own() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !manifest.join("src/chaintracks").exists(),
        "src/chaintracks is back: the header service is the one headers machine"
    );

    // The store's public names, in no source file of the crate.
    let needles = [
        "mod chaintracks;",
        "crate::chaintracks",
        "ChaintracksStorage",
        "ChaintracksOptions",
        "ChaintracksManagement",
        "BulkIngestor",
        "LiveIngestor",
        "LiveBlockHeader",
        "InsertHeaderResult",
    ];
    let mut files = Vec::new();
    for dir in ["src", "examples"] {
        rust_sources(&manifest.join(dir), &mut files);
    }
    assert!(!files.is_empty(), "the crate's sources were found");
    let mut found = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("read");
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
        "the embedded header store is named ({} lines):\n{}",
        found.len(),
        found
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
