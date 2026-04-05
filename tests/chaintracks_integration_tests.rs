//! End-to-end integration tests for ChaintracksServiceClient against a live
//! Chaintracks server at https://api.calhouninfra.com/.
//!
//! Most tests are `#[ignore]` because they require network access to the live
//! server. Run them explicitly with:
//!
//! ```bash
//! cargo test --test chaintracks_integration_tests -- --ignored
//! ```

use bsv_rs::transaction::ChainTracker;
use bsv_wallet_toolbox_rs::services::{ChaintracksConfig, ChaintracksServiceClient};

/// The live Chaintracks server URL used for all integration tests.
const CHAINTRACKS_URL: &str = "https://api.calhouninfra.com";

/// BSV genesis block hash (block 0).
const GENESIS_BLOCK_HASH: &str = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";

/// BSV genesis block merkle root (same as the coinbase txid for block 0).
const GENESIS_MERKLE_ROOT: &str =
    "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b";

// =============================================================================
// Helper
// =============================================================================

fn make_client(url: &str) -> ChaintracksServiceClient {
    ChaintracksServiceClient::new(ChaintracksConfig {
        url: url.to_string(),
        api_key: None,
    })
}

// =============================================================================
// Test 1: find_header_for_block_hash with a known block
// =============================================================================

#[tokio::test]
#[ignore] // Requires live server
async fn test_chaintracks_find_header_by_hash() {
    let client = make_client(CHAINTRACKS_URL);

    // findHeaderHexForBlockHash only searches live/in-memory storage (recent ~2000 blocks).
    // So first get the chain tip, then look it up by hash — guaranteed to be in live storage.
    let tip = client
        .find_chain_tip_header()
        .await
        .expect("find_chain_tip_header should succeed");

    let header = client
        .find_header_for_block_hash(&tip.hash)
        .await
        .expect("find_header_for_block_hash should succeed for chain tip hash");

    // The returned header should match the tip we just fetched.
    assert_eq!(header.height, tip.height, "height must match chain tip");
    assert_eq!(header.hash, tip.hash, "hash must match the queried hash");
    assert!(
        !header.merkle_root.is_empty(),
        "merkle root should be populated"
    );

    // Sanity-check that the other header fields are populated.
    assert!(header.version > 0, "version must be positive");
    assert!(header.time > 0, "time must be non-zero");
}

// =============================================================================
// Test 2: is_valid_root_for_height
// =============================================================================

#[tokio::test]
#[ignore] // Requires live server
async fn test_chaintracks_is_valid_root_for_height() {
    let client = make_client(CHAINTRACKS_URL);

    // The genesis merkle root should be valid at height 0.
    let valid = client
        .is_valid_root_for_height(GENESIS_MERKLE_ROOT, 0)
        .await
        .expect("is_valid_root_for_height should succeed");
    assert!(valid, "genesis merkle root must be valid at height 0");

    // An obviously wrong merkle root should NOT be valid at height 0.
    let invalid = client
        .is_valid_root_for_height(
            "0000000000000000000000000000000000000000000000000000000000000000",
            0,
        )
        .await
        .expect("is_valid_root_for_height should succeed even for invalid root");
    assert!(!invalid, "all-zero root must NOT be valid at height 0");
}

// =============================================================================
// Test 3: find_chain_tip_header (verifies chain info)
// =============================================================================

#[tokio::test]
#[ignore] // Requires live server
async fn test_chaintracks_chain_tip() {
    let client = make_client(CHAINTRACKS_URL);

    // Fetch the chain tip header — this verifies the server is tracking mainnet
    // and is reasonably up to date.
    let tip = client
        .find_chain_tip_header()
        .await
        .expect("find_chain_tip_header should succeed");

    // The chain tip height should be well past 900,000 by now (April 2026).
    assert!(
        tip.height > 900_000,
        "chain tip height {} should be > 900,000",
        tip.height
    );

    // The tip should have a valid-looking hash (64 hex chars).
    assert_eq!(tip.hash.len(), 64, "tip hash must be 64 hex characters");

    // The tip should have a valid-looking merkle root.
    assert_eq!(
        tip.merkle_root.len(),
        64,
        "tip merkle_root must be 64 hex characters"
    );
}

// =============================================================================
// Test 4: get_present_height (current_height)
// =============================================================================

#[tokio::test]
#[ignore] // Requires live server
async fn test_chaintracks_current_height() {
    let client = make_client(CHAINTRACKS_URL);

    let height = client
        .get_present_height()
        .await
        .expect("get_present_height should succeed");

    // Height should be well past 900,000.
    assert!(
        height > 900_000,
        "current height {} should be > 900,000",
        height
    );
}

// =============================================================================
// Test 5: graceful error when server is unreachable
// =============================================================================

#[tokio::test]
async fn test_chaintracks_server_down_graceful() {
    // Point to a port where nothing is running — this must not panic.
    let client = make_client("http://localhost:19999");

    let result = client
        .find_header_for_block_hash(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .await;

    // Must be an Err, not a panic.
    assert!(result.is_err(), "unreachable server must return Err");

    // The error should be a network/connection error.
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("Network error")
            || err_msg.contains("network")
            || err_msg.contains("connect"),
        "error message should indicate a network/connection problem, got: {}",
        err_msg
    );
}

// =============================================================================
// Test 6: ChainTracker trait usage
// =============================================================================

#[tokio::test]
#[ignore] // Requires live server
async fn test_chaintracks_implements_chain_tracker() {
    let client = make_client(CHAINTRACKS_URL);

    // Use the client through the ChainTracker trait to prove it works
    // via dynamic dispatch.
    let tracker: &dyn ChainTracker = &client;

    // is_valid_root_for_height through the trait
    let valid = tracker
        .is_valid_root_for_height(GENESIS_MERKLE_ROOT, 0)
        .await
        .expect("ChainTracker::is_valid_root_for_height should succeed");
    assert!(
        valid,
        "genesis merkle root must be valid at height 0 via trait"
    );

    // current_height through the trait
    let height = tracker
        .current_height()
        .await
        .expect("ChainTracker::current_height should succeed");
    assert!(
        height > 900_000,
        "ChainTracker::current_height {} should be > 900,000",
        height
    );

    // Negative case: invalid root through the trait
    let invalid = tracker
        .is_valid_root_for_height(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            0,
        )
        .await
        .expect("ChainTracker::is_valid_root_for_height should succeed for invalid root");
    assert!(
        !invalid,
        "fake merkle root must NOT be valid at height 0 via trait"
    );
}

// =============================================================================
// Test 7: find_header_for_height (bonus — exercises another endpoint)
// =============================================================================

#[tokio::test]
#[ignore] // Requires live server
async fn test_chaintracks_find_header_for_height() {
    let client = make_client(CHAINTRACKS_URL);

    // Look up the genesis block by height.
    let header = client
        .find_header_for_height(0)
        .await
        .expect("find_header_for_height(0) should succeed");

    assert_eq!(header.height, 0);
    assert_eq!(header.hash, GENESIS_BLOCK_HASH);
    assert_eq!(header.merkle_root, GENESIS_MERKLE_ROOT);

    // Also look up a block we know exists (block 1).
    let block_1 = client
        .find_header_for_height(1)
        .await
        .expect("find_header_for_height(1) should succeed");

    assert_eq!(block_1.height, 1);
    // Block 1's previous_hash must be the genesis block hash.
    assert_eq!(
        block_1.previous_hash, GENESIS_BLOCK_HASH,
        "block 1 previous_hash must be the genesis block hash"
    );
}

// =============================================================================
// Test 8: from_url convenience constructor
// =============================================================================

#[tokio::test]
#[ignore] // Requires live server
async fn test_chaintracks_from_url_convenience() {
    // Verify the from_url() convenience constructor works end-to-end.
    let client = ChaintracksServiceClient::from_url(CHAINTRACKS_URL);

    let height = client
        .get_present_height()
        .await
        .expect("from_url client should be able to get height");

    assert!(height > 900_000);
}
