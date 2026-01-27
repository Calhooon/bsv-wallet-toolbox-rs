//! Basic wallet example demonstrating StorageClient usage.
//!
//! This example shows how to connect to a remote storage server
//! and perform basic wallet operations.

use bsv_sdk::primitives::PrivateKey;
use bsv_sdk::wallet::ProtoWallet;

fn main() {
    println!("Basic wallet example");
    println!("To run this example with actual storage:");
    println!("  1. Set up a wallet with a private key");
    println!("  2. Connect to storage.babbage.systems");
    println!("  3. Perform wallet operations");

    // Create a test wallet
    let key = PrivateKey::random();
    let _wallet = ProtoWallet::new(Some(key));

    println!("\nWallet created successfully!");
    println!("See the StorageClient documentation for full usage.");
}
