//! Chaintracks configuration example.
//!
//! This example demonstrates how to configure the Chaintracks block header
//! tracking system, including mainnet and testnet options and custom thresholds.
//!
//! Run with: `cargo run --example chaintracks_demo`

use bsv_wallet_toolbox::chaintracks::ChaintracksOptions;

fn main() {
    println!("BSV Wallet Toolbox - Chaintracks Demo");
    println!("======================================\n");

    // Show mainnet default options
    let mainnet_options = ChaintracksOptions::default_mainnet();
    println!("Mainnet Chaintracks options:");
    println!("  Chain: {:?}", mainnet_options.chain);
    println!(
        "  Live height threshold: {}",
        mainnet_options.live_height_threshold
    );
    println!(
        "  Reorg height threshold: {}",
        mainnet_options.reorg_height_threshold
    );
    println!(
        "  Batch insert limit: {}",
        mainnet_options.batch_insert_limit
    );
    println!(
        "  Bulk migration chunk size: {}",
        mainnet_options.bulk_migration_chunk_size
    );
    println!("  Require ingestors: {}", mainnet_options.require_ingestors);
    println!("  Readonly: {}", mainnet_options.readonly);

    // Show testnet options
    let testnet_options = ChaintracksOptions::default_testnet();
    println!("\nTestnet Chaintracks options:");
    println!("  Chain: {:?}", testnet_options.chain);
    println!(
        "  Live height threshold: {}",
        testnet_options.live_height_threshold
    );

    // Show custom options with readonly mode
    let readonly_options = ChaintracksOptions {
        readonly: true,
        live_height_threshold: 500,
        ..ChaintracksOptions::default_mainnet()
    };
    println!("\nCustom readonly Chaintracks options:");
    println!("  Chain: {:?}", readonly_options.chain);
    println!(
        "  Live height threshold: {}",
        readonly_options.live_height_threshold
    );
    println!("  Readonly: {}", readonly_options.readonly);

    println!("\nDemo complete!");
}
