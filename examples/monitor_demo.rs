//! Monitor daemon configuration example.
//!
//! This example demonstrates how to configure the Monitor background task
//! scheduler, including task intervals, enabling/disabling tasks, and
//! inspecting the default configuration.
//!
//! Run with: `cargo run --example monitor_demo`

use bsv_wallet_toolbox::monitor::{MonitorOptions, TaskConfig};
use std::time::Duration;

fn main() {
    println!("BSV Wallet Toolbox - Monitor Configuration Demo");
    println!("================================================\n");

    // Show default configuration
    let options = MonitorOptions::default();
    println!("Default monitor options:");
    println!(
        "  Fail abandoned timeout: {:?}",
        options.fail_abandoned_timeout
    );

    // Print all default task configurations
    println!("\nDefault task intervals:");
    println!(
        "  clock:                {:>6}s  (start immediately: {})",
        options.tasks.clock.interval.as_secs(),
        options.tasks.clock.start_immediately
    );
    println!(
        "  check_for_proofs:     {:>6}s  (start immediately: {})",
        options.tasks.check_for_proofs.interval.as_secs(),
        options.tasks.check_for_proofs.start_immediately
    );
    println!(
        "  new_header:           {:>6}s  (start immediately: {})",
        options.tasks.new_header.interval.as_secs(),
        options.tasks.new_header.start_immediately
    );
    println!(
        "  reorg:                {:>6}s  (start immediately: {})",
        options.tasks.reorg.interval.as_secs(),
        options.tasks.reorg.start_immediately
    );
    println!(
        "  send_waiting:         {:>6}s  (start immediately: {})",
        options.tasks.send_waiting.interval.as_secs(),
        options.tasks.send_waiting.start_immediately
    );
    println!(
        "  fail_abandoned:       {:>6}s  (start immediately: {})",
        options.tasks.fail_abandoned.interval.as_secs(),
        options.tasks.fail_abandoned.start_immediately
    );
    println!(
        "  unfail:               {:>6}s  (start immediately: {})",
        options.tasks.unfail.interval.as_secs(),
        options.tasks.unfail.start_immediately
    );
    println!(
        "  monitor_call_history: {:>6}s  (start immediately: {})",
        options.tasks.monitor_call_history.interval.as_secs(),
        options.tasks.monitor_call_history.start_immediately
    );
    println!(
        "  review_status:        {:>6}s  (start immediately: {})",
        options.tasks.review_status.interval.as_secs(),
        options.tasks.review_status.start_immediately
    );
    println!(
        "  purge:                {:>6}s  (start immediately: {})",
        options.tasks.purge.interval.as_secs(),
        options.tasks.purge.start_immediately
    );
    println!(
        "  check_no_sends:       {:>6}s  (start immediately: {})",
        options.tasks.check_no_sends.interval.as_secs(),
        options.tasks.check_no_sends.start_immediately
    );

    // Show custom task config
    println!("\nCustom task config (30s interval):");
    let custom = TaskConfig::new(Duration::from_secs(30));
    println!("  Enabled: {}", custom.enabled);
    println!("  Interval: {:?}", custom.interval);
    println!("  Start immediately: {}", custom.start_immediately);

    // Show disabled task
    println!("\nDisabled task config:");
    let disabled = TaskConfig::disabled();
    println!("  Enabled: {}", disabled.enabled);

    println!("\nDemo complete!");
}
