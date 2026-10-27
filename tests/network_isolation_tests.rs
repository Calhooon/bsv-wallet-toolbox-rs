//! Run the #182 witnesses with the library compiled as an external dependency.
#![cfg(feature = "bench-support")]

use bsv_wallet_toolbox_rs::services::{
    services, Arc, Chain, Services, ServicesOptions, WalletServices,
};

#[path = "../src/services/network_post_tests.rs"]
mod network_post_tests;
