//! #502: the discovery door verifies every resolver's outputs with the
//! wallet's chain tracker, and with none asks no resolver and answers no
//! overlay certificate, as the reference's `queryOverlay` answers `[]`
//! (ts-stack@edf6e03 `identityUtils.ts:395-400`, `Wallet.ts:894-911`).

use std::sync::atomic::{AtomicUsize, Ordering};

use bsv_rs::overlay::OutputListItem;
use bsv_rs::wallet::{DiscoverByAttributesArgs, DiscoverByIdentityKeyArgs};
use indexmap::IndexMap;

use super::super::discovery_rows_tests::{wallet, wallet_on};
use crate::services::{
    BlockHeader, FiatCurrency, GetBeefResult, GetMerklePathResult, GetRawTxResult,
    GetStatusForTxidsResult, GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput,
    PostBeefResult, UtxoVerdict,
};
use crate::wallet::lookup::fixture::{key, published, record_json, revealed, token};
use crate::wallet::lookup::OverlayLookupResolver;
use crate::WalletServices;

/// A host's own resolver: it answers `outputs` and counts the asks.
struct Host {
    outputs: Vec<OutputListItem>,
    asked: AtomicUsize,
}

impl Host {
    fn new(outputs: Vec<OutputListItem>) -> Self {
        Self {
            outputs,
            asked: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl OverlayLookupResolver for Host {
    async fn lookup_by_identity_key(
        &self,
        _identity_key: &str,
        _certifiers: &[String],
    ) -> crate::Result<Vec<OutputListItem>> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Ok(self.outputs.clone())
    }

    async fn lookup_by_attributes(
        &self,
        _attributes: &IndexMap<String, String>,
        certifiers: &[String],
    ) -> crate::Result<Vec<OutputListItem>> {
        self.lookup_by_identity_key("", certifiers).await
    }
}

/// Each door's request for the wallet's own key and for `name` `Alice`.
fn requests<V: WalletServices + Send + Sync + 'static>(
    wallet: &super::Wallet<crate::StorageSqlx, V>,
) -> (DiscoverByIdentityKeyArgs, DiscoverByAttributesArgs) {
    let identity = DiscoverByIdentityKeyArgs {
        identity_key: wallet.identity_key().into(),
        limit: None,
        offset: None,
        seek_permission: None,
    };
    let attributes = DiscoverByAttributesArgs {
        attributes: IndexMap::from([("name".to_string(), "Alice".to_string())]),
        limit: None,
        offset: None,
        seek_permission: None,
    };
    (identity, attributes)
}

/// The serials each door answers for `requests`, against `resolver`.
async fn answers<V: WalletServices + Send + Sync + 'static>(
    wallet: &super::Wallet<crate::StorageSqlx, V>,
    resolver: &Host,
) -> (Vec<String>, Vec<String>) {
    let (identity, attributes) = requests(wallet);
    let by_key = wallet.discover_identity_with_resolver(identity, resolver);
    let by_key = by_key.await.unwrap().certificates;
    let by_name = wallet.discover_attributes_with_resolver(attributes, resolver);
    let by_name = by_name.await.unwrap().certificates;
    let serials = |c: Vec<bsv_rs::wallet::IdentityCertificate>| {
        c.into_iter().map(|c| c.certificate.serial_number).collect()
    };
    (serials(by_key), serials(by_name))
}

/// Services built with no `chaintracks_url` reach no header service: both
/// doors answer no overlay certificate and ask no resolver, where a wallet
/// whose services hold a tracker answers the same output; before #502 both
/// answered it.
#[tokio::test]
async fn with_no_chain_tracker_the_doors_answer_no_overlay_certificate() {
    let record = revealed(&key(9), &key(8), [2; 32], &[("name", "Alice")]).await;
    let host = Host::new(vec![published(&record)]);
    let tracked = answers(&wallet(None).await, &host).await;
    let serial = vec![record.serial_number.clone()];
    assert_eq!(tracked, (serial.clone(), serial));
    assert_eq!(host.asked.load(Ordering::SeqCst), 2);
    let services = crate::services::Services::mainnet().unwrap();
    let untracked = answers(&wallet_on(services, None).await, &host).await;
    assert_eq!(untracked, (vec![], vec![]));
    assert_eq!(host.asked.load(Ordering::SeqCst), 2);
}

/// A host's resolver meets the evidence as the HTTP one does: its output
/// with no BEEF and its output whose transaction carries no proof (the
/// shape a resolver could hand over before #502) are dropped, its proven
/// output answered.
#[tokio::test]
async fn a_host_resolvers_outputs_meet_the_evidence() {
    let good = revealed(&key(9), &key(8), [3; 32], &[("name", "Alice")]).await;
    let bare = revealed(&key(9), &key(8), [2; 32], &[("name", "Alice")]).await;
    let mut unproven = bsv_rs::transaction::Transaction::new();
    unproven
        .outputs
        .push(bsv_rs::transaction::TransactionOutput {
            satoshis: Some(1),
            locking_script: token(&key(8), record_json(&bare)),
            change: false,
        });
    let unproven = OutputListItem {
        beef: unproven.to_beef(true).unwrap(),
        output_index: 0,
        context: None,
    };
    let empty = OutputListItem {
        beef: vec![],
        ..unproven.clone()
    };
    let host = Host::new(vec![unproven, empty, published(&good)]);
    let serial = vec![good.serial_number.clone()];
    assert_eq!(
        answers(&wallet(None).await, &host).await,
        (serial.clone(), serial)
    );
}

/// Services whose header service faults: `get_chain_tracker` answers a
/// transport error, every other read the mock's (#521).
struct Faulting(crate::services::mock::MockWalletServices);

const FAULT: &str = "chaintracks: connection refused";

#[async_trait::async_trait]
impl WalletServices for Faulting {
    async fn get_chain_tracker(&self) -> crate::Result<&dyn bsv_rs::transaction::ChainTracker> {
        Err(crate::Error::NetworkError(FAULT.into()))
    }

    async fn get_height(&self) -> crate::Result<u32> {
        self.0.get_height().await
    }

    async fn get_header_for_height(&self, height: u32) -> crate::Result<Vec<u8>> {
        self.0.get_header_for_height(height).await
    }

    async fn hash_to_header(&self, hash: &str) -> crate::Result<BlockHeader> {
        self.0.hash_to_header(hash).await
    }

    async fn get_raw_tx(&self, txid: &str, next: bool) -> crate::Result<GetRawTxResult> {
        self.0.get_raw_tx(txid, next).await
    }

    async fn get_merkle_path(&self, txid: &str, next: bool) -> crate::Result<GetMerklePathResult> {
        self.0.get_merkle_path(txid, next).await
    }

    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> crate::Result<Vec<PostBeefResult>> {
        self.0.post_beef(beef, txids).await
    }

    async fn get_utxo_status(
        &self,
        script: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        at: Option<&str>,
        next: bool,
    ) -> crate::Result<GetUtxoStatusResult> {
        self.0.get_utxo_status(script, format, at, next).await
    }

    async fn get_status_for_txids(
        &self,
        txids: &[String],
        next: bool,
    ) -> crate::Result<GetStatusForTxidsResult> {
        self.0.get_status_for_txids(txids, next).await
    }

    async fn get_bsv_exchange_rate(&self) -> crate::Result<f64> {
        self.0.get_bsv_exchange_rate().await
    }

    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> crate::Result<f64> {
        self.0.get_fiat_exchange_rate(currency, base).await
    }

    fn hash_output_script(&self, script: &[u8]) -> String {
        self.0.hash_output_script(script)
    }

    async fn is_utxo(&self, txid: &str, vout: u32, script: &[u8]) -> UtxoVerdict {
        self.0.is_utxo(txid, vout, script).await
    }

    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> crate::Result<bool> {
        self.0.n_lock_time_is_final(n_lock_time).await
    }

    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> crate::Result<bool> {
        self.0.n_lock_time_is_final_for_tx(input).await
    }

    async fn get_beef(&self, txid: &str, known: &[String]) -> crate::Result<GetBeefResult> {
        self.0.get_beef(txid, known).await
    }
}

/// A header service that faults with a text other than not-configured is
/// each door's answer, the services' class and text unchanged, and no
/// resolver is asked; before #521 both doors answered an empty overlay part
/// (the reference's `getChainTracker()` is uncaught, ts-stack@edf6e03
/// `Wallet.ts:901`).
#[tokio::test]
async fn a_faulting_header_service_is_the_discover_calls_answer() {
    let record = revealed(&key(9), &key(8), [2; 32], &[("name", "Alice")]).await;
    let host = Host::new(vec![published(&record)]);
    let services = Faulting(crate::services::mock::MockWalletServices::new());
    let wallet = wallet_on(services, None).await;
    let (identity, attributes) = requests(&wallet);
    let by_key = wallet
        .discover_identity_with_resolver(identity, &host)
        .await;
    let by_name = wallet.discover_attributes_with_resolver(attributes, &host);
    let by_name = by_name.await;
    for answer in [by_key, by_name] {
        match answer {
            Err(crate::Error::NetworkError(text)) => assert_eq!(text, FAULT),
            other => panic!("expected the header service's fault, got {other:?}"),
        }
    }
    assert_eq!(host.asked.load(Ordering::SeqCst), 0);
}

/// The two `WalletInterface` discover methods, as a host calls them, each
/// answer mapped to the shape a host matches.
async fn at_the_boundary<V: WalletServices + Send + Sync + 'static>(
    wallet: &super::Wallet<crate::StorageSqlx, V>,
) -> [bsv_rs::Result<bsv_rs::wallet::DiscoverCertificatesResult>; 2] {
    use bsv_rs::wallet::WalletInterface;
    let (identity, attributes) = requests(wallet);
    let by_key = wallet.discover_by_identity_key(identity, None).await;
    let by_name = wallet.discover_by_attributes(attributes, None).await;
    [by_key, by_name]
}

/// Through the `WalletInterface` methods (`validate_originator` and the
/// `?` of `wallet.rs`'s discover doors), the header service's fault reaches
/// the host as `src/error.rs`'s mapping gives it: the Transport fault
/// family, the toolbox's Display text byte for byte (#524). A coverage
/// scene, green on the base.
#[tokio::test]
async fn a_faulting_header_service_reaches_the_host_as_a_transport_fault() {
    let services = Faulting(crate::services::mock::MockWalletServices::new());
    for answer in at_the_boundary(&wallet_on(services, None).await).await {
        match answer {
            Err(bsv_rs::Error::WalletFault(fault)) => assert_eq!(
                (fault.family, fault.message.as_str()),
                (
                    bsv_rs::WalletFaultFamily::Transport,
                    "Network error: chaintracks: connection refused"
                )
            ),
            other => panic!("expected a Transport fault, got {other:?}"),
        }
    }
}

/// Through the same boundary, services with no header service (the
/// not-configured answer) still answer an empty overlay part, not a fault.
#[tokio::test]
async fn with_no_header_service_the_boundary_answers_no_certificate() {
    let services = crate::services::Services::mainnet().unwrap();
    for answer in at_the_boundary(&wallet_on(services, None).await).await {
        assert!(answer.unwrap().certificates.is_empty());
    }
}
