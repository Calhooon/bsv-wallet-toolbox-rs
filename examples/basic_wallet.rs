//! Basic wallet example demonstrating core functionality.
//!
//! This example shows how to create and configure the main components
//! of the BSV Wallet Toolbox: Services, ServicesOptions, and Chain.
//!
//! Run with: `cargo run --example basic_wallet`

use bsv_wallet_toolbox_rs::{Chain, Services, ServicesOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("BSV Wallet Toolbox - Basic Example");
    println!("===================================\n");

    // Arcade is the default broadcaster mode: mainnet's defaults name the
    // crate's public ARCADE_V2_MAINNET; testnet's name none and refuse at
    // construction (#317). Name another with `with_arcade`; ARCADE_URL
    // here, a loopback placeholder otherwise (constructing `Services` makes
    // no request).
    assert!(Services::mainnet().is_ok());
    assert!(Services::testnet().is_err());
    let arcade_url =
        std::env::var("ARCADE_URL").unwrap_or_else(|_| "http://127.0.0.1:9".to_string());

    // Mainnet services: Arcade, then Bitails and WhatsOnChain for posts
    let services = Services::with_options(
        Chain::Main,
        ServicesOptions::mainnet().with_arcade(arcade_url.clone(), None),
    )?;
    println!("Services configured for: {:?}", services.chain);

    // Testnet services
    let testnet_services = Services::with_options(
        Chain::Test,
        ServicesOptions::testnet().with_arcade(arcade_url.clone(), None),
    )?;
    println!(
        "Testnet services configured for: {:?}",
        testnet_services.chain
    );

    // Create services with custom options using the builder pattern
    let custom_options = ServicesOptions::default()
        .with_arcade(arcade_url, None)
        .with_bhs_url("https://bhs.babbage.systems")
        .with_bhs_api_key("my-api-key");
    let _custom_services = Services::with_options(Chain::Main, custom_options)?;
    println!("Custom services configured with BHS provider");

    // Show version info
    println!("\nWallet toolbox version: 0.1.0");

    // Demonstrate the ServicesOptions builder
    println!("\nServicesOptions builder example:");
    let opts = ServicesOptions::mainnet()
        .with_woc_api_key("woc-key")
        .with_bitails_api_key("bitails-key")
        .with_bhs("https://bhs.example.com", Some("bearer-token".to_string()));
    println!("  WoC API key set: {}", opts.whatsonchain_api_key.is_some());
    println!("  Bitails API key set: {}", opts.bitails_api_key.is_some());
    println!("  BHS URL: {:?}", opts.bhs_url);

    println!("\nExample complete!");
    Ok(())
}
