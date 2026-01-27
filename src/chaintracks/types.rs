//! Chaintracks type definitions
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Api/BlockHeaderApi.ts`

use serde::{Deserialize, Serialize};

/// Network chain identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Chain {
    Main,
    Test,
}

impl Chain {
    pub fn as_str(&self) -> &'static str {
        match self {
            Chain::Main => "main",
            Chain::Test => "test",
        }
    }
}

impl Default for Chain {
    fn default() -> Self {
        Chain::Main
    }
}

/// Base block header without height or hash (as received from network)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseBlockHeader {
    /// Block version number
    pub version: u32,
    /// Hash of the previous block header
    pub previous_hash: String,
    /// Merkle root of transactions in this block
    pub merkle_root: String,
    /// Block timestamp (Unix time)
    pub time: u32,
    /// Compact target (difficulty bits)
    pub bits: u32,
    /// Nonce used to find valid block hash
    pub nonce: u32,
}

impl BaseBlockHeader {
    /// Serialize header to 80-byte array
    pub fn to_bytes(&self) -> [u8; 80] {
        let mut bytes = [0u8; 80];

        // Version (4 bytes, little-endian)
        bytes[0..4].copy_from_slice(&self.version.to_le_bytes());

        // Previous hash (32 bytes, reversed)
        let prev_hash = hex::decode(&self.previous_hash).unwrap_or_default();
        if prev_hash.len() == 32 {
            bytes[4..36].copy_from_slice(&prev_hash);
        }

        // Merkle root (32 bytes, reversed)
        let merkle = hex::decode(&self.merkle_root).unwrap_or_default();
        if merkle.len() == 32 {
            bytes[36..68].copy_from_slice(&merkle);
        }

        // Time (4 bytes, little-endian)
        bytes[68..72].copy_from_slice(&self.time.to_le_bytes());

        // Bits (4 bytes, little-endian)
        bytes[72..76].copy_from_slice(&self.bits.to_le_bytes());

        // Nonce (4 bytes, little-endian)
        bytes[76..80].copy_from_slice(&self.nonce.to_le_bytes());

        bytes
    }
}

/// Block header with height and computed hash
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    /// Block version number
    pub version: u32,
    /// Hash of the previous block header
    pub previous_hash: String,
    /// Merkle root of transactions in this block
    pub merkle_root: String,
    /// Block timestamp (Unix time)
    pub time: u32,
    /// Compact target (difficulty bits)
    pub bits: u32,
    /// Nonce used to find valid block hash
    pub nonce: u32,
    /// Block height in the chain
    pub height: u32,
    /// Double SHA256 hash of header (hex, reversed for display)
    pub hash: String,
}

impl From<BaseBlockHeader> for BlockHeader {
    fn from(base: BaseBlockHeader) -> Self {
        // Compute hash from base header
        let bytes = base.to_bytes();
        let hash = compute_block_hash(&bytes);

        BlockHeader {
            version: base.version,
            previous_hash: base.previous_hash,
            merkle_root: base.merkle_root,
            time: base.time,
            bits: base.bits,
            nonce: base.nonce,
            height: 0, // Must be set externally
            hash,
        }
    }
}

/// Live block header with additional tracking fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveBlockHeader {
    /// Block version number
    pub version: u32,
    /// Hash of the previous block header
    pub previous_hash: String,
    /// Merkle root of transactions in this block
    pub merkle_root: String,
    /// Block timestamp (Unix time)
    pub time: u32,
    /// Compact target (difficulty bits)
    pub bits: u32,
    /// Nonce used to find valid block hash
    pub nonce: u32,
    /// Block height in the chain
    pub height: u32,
    /// Double SHA256 hash of header (hex)
    pub hash: String,
    /// Cumulative chain work (hex)
    pub chain_work: String,
    /// True if this is the current chain tip
    pub is_chain_tip: bool,
    /// True if this header is on the active chain
    pub is_active: bool,
    /// Internal database ID
    pub header_id: i64,
    /// ID of previous header (if tracked)
    pub previous_header_id: Option<i64>,
}

impl From<LiveBlockHeader> for BlockHeader {
    fn from(live: LiveBlockHeader) -> Self {
        BlockHeader {
            version: live.version,
            previous_hash: live.previous_hash,
            merkle_root: live.merkle_root,
            time: live.time,
            bits: live.bits,
            nonce: live.nonce,
            height: live.height,
            hash: live.hash,
        }
    }
}

/// Result of inserting a header into storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertHeaderResult {
    /// True if header was newly added (not a duplicate)
    pub added: bool,
    /// True if this was a duplicate header
    pub dupe: bool,
    /// True if this header is now the active chain tip
    pub is_active_tip: bool,
    /// Depth of reorg if one occurred (0 = no reorg)
    pub reorg_depth: u32,
    /// Previous chain tip before this insertion
    pub prior_tip: Option<LiveBlockHeader>,
    /// Headers that were deactivated by a reorg
    pub deactivated_headers: Vec<LiveBlockHeader>,
    /// True if previous header was not found
    pub no_prev: bool,
    /// True if previous header reference was invalid
    pub bad_prev: bool,
    /// True if no active ancestor could be found
    pub no_active_ancestor: bool,
    /// True if no chain tip exists
    pub no_tip: bool,
}

impl Default for InsertHeaderResult {
    fn default() -> Self {
        InsertHeaderResult {
            added: false,
            dupe: false,
            is_active_tip: false,
            reorg_depth: 0,
            prior_tip: None,
            deactivated_headers: vec![],
            no_prev: false,
            bad_prev: false,
            no_active_ancestor: false,
            no_tip: false,
        }
    }
}

/// Height range representation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeightRange {
    /// Inclusive start height
    pub low: u32,
    /// Inclusive end height
    pub high: u32,
}

impl HeightRange {
    pub fn new(low: u32, high: u32) -> Self {
        HeightRange { low, high }
    }

    /// Number of headers in this range
    pub fn count(&self) -> u32 {
        if self.high >= self.low {
            self.high - self.low + 1
        } else {
            0
        }
    }

    /// Check if height is within this range
    pub fn contains(&self, height: u32) -> bool {
        height >= self.low && height <= self.high
    }

    /// Check if ranges overlap
    pub fn overlaps(&self, other: &HeightRange) -> bool {
        self.low <= other.high && other.low <= self.high
    }

    /// Merge two overlapping or adjacent ranges
    pub fn merge(&self, other: &HeightRange) -> Option<HeightRange> {
        if self.overlaps(other) || self.high + 1 == other.low || other.high + 1 == self.low {
            Some(HeightRange {
                low: self.low.min(other.low),
                high: self.high.max(other.high),
            })
        } else {
            None
        }
    }

    /// Subtract another range from this one
    pub fn subtract(&self, other: &HeightRange) -> Vec<HeightRange> {
        if !self.overlaps(other) {
            return vec![self.clone()];
        }

        let mut result = vec![];

        // Lower portion
        if self.low < other.low {
            result.push(HeightRange::new(self.low, other.low - 1));
        }

        // Upper portion
        if self.high > other.high {
            result.push(HeightRange::new(other.high + 1, self.high));
        }

        result
    }
}

/// Chaintracks system information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaintracksInfo {
    /// Chain being tracked
    pub chain: Chain,
    /// Storage backend type
    pub storage_type: String,
    /// Number of bulk ingestors
    pub bulk_ingestor_count: usize,
    /// Number of live ingestors
    pub live_ingestor_count: usize,
    /// Current chain tip height
    pub chain_tip_height: Option<u32>,
    /// Lowest height in live storage
    pub live_low_height: Option<u32>,
    /// Highest height in live storage
    pub live_high_height: Option<u32>,
    /// Whether system is currently listening for new headers
    pub is_listening: bool,
    /// Whether initial sync is complete
    pub is_synchronized: bool,
}

/// Compute double SHA256 hash of block header bytes
fn compute_block_hash(header_bytes: &[u8; 80]) -> String {
    use sha2::{Sha256, Digest};

    // First SHA256
    let mut hasher = Sha256::new();
    hasher.update(header_bytes);
    let first_hash = hasher.finalize();

    // Second SHA256
    let mut hasher = Sha256::new();
    hasher.update(&first_hash);
    let second_hash = hasher.finalize();

    // Reverse for display (Bitcoin convention)
    let mut reversed = second_hash.to_vec();
    reversed.reverse();

    hex::encode(reversed)
}

/// Calculate chain work from difficulty bits
///
/// This is a simplified implementation that returns a hex representation of work.
/// For accurate chain work calculations, a big integer library should be used.
pub fn calculate_work(bits: u32) -> String {
    // Extract exponent and mantissa from compact format
    let exponent = (bits >> 24) as u32;
    let mantissa = (bits & 0x007fffff) as u128;

    if mantissa == 0 {
        return "0".repeat(64);
    }

    // The compact target format: target = mantissa * 256^(exponent-3)
    // For exponent > 3, this would require very large shifts that overflow
    // We use saturating arithmetic and provide a simplified approximation

    let shift_amount = if exponent >= 3 {
        8 * (exponent - 3)
    } else {
        0
    };

    // Calculate target with overflow protection
    let target = if exponent <= 3 {
        mantissa >> (8 * (3 - exponent))
    } else if shift_amount >= 128 {
        // Target is very large, work is very small
        return "0".repeat(63) + "1";
    } else {
        // Use checked_shl with fallback to MAX for overflow
        mantissa.checked_shl(shift_amount as u32).unwrap_or(u128::MAX)
    };

    // Work = 2^256 / (target + 1)
    // Since we can't represent 2^256, we use u128::MAX as a proxy
    // This gives a relative work value suitable for comparison
    if target == 0 {
        format!("{:064x}", u128::MAX)
    } else if target >= u128::MAX {
        "0".repeat(63) + "1"
    } else {
        format!("{:064x}", u128::MAX / (target + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_height_range_count() {
        let range = HeightRange::new(100, 200);
        assert_eq!(range.count(), 101);
    }

    #[test]
    fn test_height_range_contains() {
        let range = HeightRange::new(100, 200);
        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(range.contains(200));
        assert!(!range.contains(99));
        assert!(!range.contains(201));
    }

    #[test]
    fn test_height_range_merge() {
        let r1 = HeightRange::new(100, 150);
        let r2 = HeightRange::new(151, 200);
        let merged = r1.merge(&r2);
        assert_eq!(merged, Some(HeightRange::new(100, 200)));
    }

    #[test]
    fn test_height_range_subtract() {
        let r1 = HeightRange::new(100, 200);
        let r2 = HeightRange::new(130, 170);
        let result = r1.subtract(&r2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], HeightRange::new(100, 129));
        assert_eq!(result[1], HeightRange::new(171, 200));
    }
}
