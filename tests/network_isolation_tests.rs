//! Run the #182 witnesses with the library compiled as an external dependency.
#![cfg(feature = "bench-support")]

use bsv_wallet_toolbox_rs::services::{
    services, Arc, Chain, Services, ServicesOptions, WalletServices,
};

#[path = "../src/services/network_post_tests.rs"]
mod network_post_tests;

#[cfg(feature = "remote")]
use bsv_wallet_toolbox_rs::{
    storage::{entities::TableSettings, StorageClient, WalletStorageWriter},
    Error,
};

#[cfg(feature = "remote")]
#[path = "common/transport_isolation.rs"]
mod transport_isolation;
#[cfg(feature = "remote")]
use transport_isolation::assert_isolated;

#[cfg(feature = "remote")]
#[path = "../src/storage/client/storage_client_network_isolation_tests.rs"]
mod storage_client_network_isolation_tests;
