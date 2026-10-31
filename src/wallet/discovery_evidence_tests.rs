//! #502: the discovery door verifies every resolver's outputs with the
//! wallet's chain tracker, and with none asks no resolver and answers no
//! overlay certificate, as the reference's `queryOverlay` answers `[]`
//! (ts-stack@edf6e03 `identityUtils.ts:395-400`, `Wallet.ts:894-911`).

use std::sync::atomic::{AtomicUsize, Ordering};

use bsv_rs::overlay::OutputListItem;
use bsv_rs::wallet::{DiscoverByAttributesArgs, DiscoverByIdentityKeyArgs};
use indexmap::IndexMap;

use super::super::discovery_rows_tests::{wallet, wallet_on};
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
        _certifiers: &[String],
    ) -> crate::Result<Vec<OutputListItem>> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Ok(self.outputs.clone())
    }
}

/// The serials each door answers for the wallet's own key and for `name`
/// `Alice`, against `resolver`.
async fn answers<V: WalletServices + Send + Sync + 'static>(
    wallet: &super::Wallet<crate::StorageSqlx, V>,
    resolver: &Host,
) -> (Vec<String>, Vec<String>) {
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
