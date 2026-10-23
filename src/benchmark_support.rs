//! Unstable seams used by the offline Criterion harness.
//!
//! These helpers are hidden from the supported wallet API. The allocation
//! seam delegates to the existing allocator; the explorer seam only replaces
//! provider endpoints so the real ladders can run against local fixtures.

use crate::services::providers::{BananaBlocks, Bitails, RateGate, WhatsOnChain};
use crate::{Chain, Result, Services, ServicesOptions};
use std::sync::Arc;
use std::time::Duration;

/// Allocate one change coin on an already acquired connection.
pub async fn allocate_change_input(
    conn: &mut sqlx::SqliteConnection,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    target_satoshis: u64,
) -> Result<Option<i64>> {
    crate::storage::sqlx::benchmark_allocate_change_input(
        conn,
        user_id,
        basket_id,
        transaction_id,
        target_satoshis,
    )
    .await
}

/// Build the real service ladders with every explorer at a fixture endpoint.
pub fn local_explorer_services(
    options: ServicesOptions,
    whatsonchain: &str,
    bananablocks: &str,
    bitails: &str,
) -> Result<Services> {
    Services::with_explorers(
        Chain::Main,
        options,
        Arc::new(WhatsOnChain::with_base_url(Chain::Main, whatsonchain)),
        Arc::new(Bitails::with_base_url(Chain::Main, bitails)),
        Some(Arc::new(BananaBlocks::with_gate(
            bananablocks,
            None,
            // Exercise the token bucket without exhausting a remote API's
            // request budget while repeatedly calling a local fixture.
            Arc::new(
                RateGate::new("BananaBlocks", Duration::from_secs(1)).with_per_minute(u32::MAX),
            ),
        )?)),
    )
}
