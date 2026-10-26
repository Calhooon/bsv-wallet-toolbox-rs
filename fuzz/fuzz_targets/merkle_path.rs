//! The proof reader: a BUMP from a provider read as the reorg pass and
//! internalize read it (`MerklePath::from_binary`), then its root computed
//! for each leaf it names, and the hex form a provider's
//! `GetMerklePathResult.merkle_path` carries. A refusal is a fine answer.
#![no_main]

use bsv_rs::transaction::MerklePath;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = MerklePath::from_hex(text.trim());
    }
    let Ok(mp) = MerklePath::from_binary(data) else {
        return;
    };
    let _ = mp.compute_root(None);
    let leaves: Vec<String> = mp
        .path
        .first()
        .map(|level| {
            level
                .iter()
                .filter_map(|l| l.hash.clone())
                .take(4)
                .collect()
        })
        .unwrap_or_default();
    for txid in &leaves {
        let _ = mp.compute_root(Some(txid));
    }
    let _ = MerklePath::from_binary(&mp.to_binary());
});
