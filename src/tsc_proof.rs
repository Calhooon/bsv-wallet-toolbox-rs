//! TSC Proof to MerklePath (BRC-74 BUMP) conversion.
//!
//! WhatsOnChain and Bitails return merkle proofs in TSC format (JSON with
//! `index`, `nodes`, `target`, `txOrId`). The wallet needs BRC-74 BUMP binary
//! format for BEEF construction. This module bridges that gap.

use bsv_rs::transaction::{MerklePath, MerklePathLeaf};

/// Convert a TSC proof JSON string to BUMP binary bytes.
///
/// The JSON is expected to have fields: `index` (u64), `nodes` (array of hex
/// strings or `"*"` for duplicates), `txOrId` (hex txid), and optionally
/// `target` (block hash).
///
/// `block_height` must be provided separately (resolved via `hash_to_header`).
///
/// Returns `None` if parsing fails or the proof is malformed.
pub fn tsc_json_to_bump_binary(json_str: &str, block_height: u32) -> Option<Vec<u8>> {
    let json: serde_json::Value = serde_json::from_str(json_str).ok()?;

    let index = json.get("index")?.as_u64()?;
    let txid = json.get("txOrId").and_then(|v| v.as_str())?;
    let nodes: Vec<String> = json
        .get("nodes")?
        .as_array()?
        .iter()
        .filter_map(|n| n.as_str().map(|s| s.to_string()))
        .collect();

    let mp = tsc_proof_to_merkle_path(txid, index, &nodes, block_height).ok()?;
    Some(mp.to_binary())
}

/// Convert a TSC proof JSON string to BUMP hex string.
///
/// Same as [`tsc_json_to_bump_binary`] but returns hex-encoded output suitable
/// for `GetMerklePathResult.merkle_path`.
pub fn tsc_json_to_bump_hex(json_str: &str, block_height: u32) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(json_str).ok()?;

    let index = json.get("index")?.as_u64()?;
    let txid = json.get("txOrId").and_then(|v| v.as_str())?;
    let nodes: Vec<String> = json
        .get("nodes")?
        .as_array()?
        .iter()
        .filter_map(|n| n.as_str().map(|s| s.to_string()))
        .collect();

    let mp = tsc_proof_to_merkle_path(txid, index, &nodes, block_height).ok()?;
    Some(mp.to_hex())
}

/// Convert TSC proof components into a MerklePath.
///
/// Implements the same algorithm as the JS reference's `convertProofToMerklePath()`.
///
/// - `txid`: 64-char hex transaction ID
/// - `index`: leaf position in the merkle tree
/// - `nodes`: sibling hashes at each level (`"*"` = duplicate)
/// - `block_height`: block height containing this transaction
fn tsc_proof_to_merkle_path(
    txid: &str,
    index: u64,
    nodes: &[String],
    block_height: u32,
) -> std::result::Result<MerklePath, String> {
    if nodes.is_empty() {
        return Err("empty nodes list".to_string());
    }

    if txid.len() != 64 || hex::decode(txid).is_err() {
        return Err("invalid txid".to_string());
    }

    let mut path: Vec<Vec<MerklePathLeaf>> = Vec::new();
    let mut current_offset = index;

    for (level, node) in nodes.iter().enumerate() {
        let mut leaves = Vec::new();

        if level == 0 {
            // Level 0 contains the txid
            let txid_leaf = MerklePathLeaf::new_txid(current_offset, txid.to_string());
            leaves.push(txid_leaf);
        }

        // Determine sibling offset
        let sibling_offset = if current_offset % 2 == 0 {
            current_offset + 1
        } else {
            current_offset - 1
        };

        if node == "*" {
            // Duplicate marker — sibling is a duplicate of the current node
            let dup_leaf = MerklePathLeaf::new_duplicate(sibling_offset);
            leaves.push(dup_leaf);
        } else {
            if node.len() != 64 || hex::decode(node).is_err() {
                return Err(format!("invalid node hash at level {}", level));
            }
            let sibling_leaf = MerklePathLeaf::new(sibling_offset, node.clone());
            leaves.push(sibling_leaf);
        }

        // Sort by offset
        leaves.sort_by_key(|l| l.offset);
        path.push(leaves);

        // Move up the tree
        current_offset /= 2;
    }

    MerklePath::new(block_height, path).map_err(|e| format!("{}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_conversion() {
        // Simple TSC proof with 3 levels
        let json = r#"{
            "index": 0,
            "txOrId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "target": "0000000000000000000000000000000000000000000000000000000000000000",
            "nodes": [
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            ]
        }"#;

        let result = tsc_json_to_bump_binary(json, 100);
        assert!(result.is_some());

        let binary = result.unwrap();
        // Should be parseable back
        let mp = MerklePath::from_binary(&binary).expect("should parse back");
        assert_eq!(mp.block_height, 100);
        assert_eq!(mp.path.len(), 3);
    }

    #[test]
    fn test_duplicate_node() {
        let json = r#"{
            "index": 0,
            "txOrId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "target": "0000000000000000000000000000000000000000000000000000000000000000",
            "nodes": ["*"]
        }"#;

        let result = tsc_json_to_bump_binary(json, 500);
        assert!(result.is_some());

        let binary = result.unwrap();
        let mp = MerklePath::from_binary(&binary).expect("should parse back");
        assert_eq!(mp.block_height, 500);
        assert_eq!(mp.path.len(), 1);
    }

    #[test]
    fn test_hex_roundtrip() {
        let json = r#"{
            "index": 5,
            "txOrId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "target": "0000000000000000000000000000000000000000000000000000000000000000",
            "nodes": [
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ]
        }"#;

        let hex_str = tsc_json_to_bump_hex(json, 937800).unwrap();
        // Should be valid hex
        let binary = hex::decode(&hex_str).expect("valid hex");
        let mp = MerklePath::from_binary(&binary).expect("valid BUMP");
        assert_eq!(mp.block_height, 937800);
    }

    #[test]
    fn test_invalid_json() {
        assert!(tsc_json_to_bump_binary("not json", 100).is_none());
        assert!(tsc_json_to_bump_binary("{}", 100).is_none());
    }
}
