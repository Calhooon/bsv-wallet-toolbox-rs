//! TS `identityUtils.ts:247-255,283-293,313-384`: bind, group and weight discovery.

use std::collections::{HashMap, HashSet};

use bsv_rs::block::ChainParams;
use bsv_rs::overlay::OutputListItem;
use bsv_rs::transaction::ChainTracker;
use bsv_rs::wallet::{
    DiscoverByAttributesArgs, DiscoverByIdentityKeyArgs, DiscoverCertificatesResult,
    IdentityCertificate, IdentityCertifier,
};
use indexmap::IndexMap;

use super::Wallet;
use crate::managers::{TrustSettings, WalletSettings};
use crate::services::{Chain, WalletServices};
use crate::storage::{entities::TableCertificate, FindCertificatesArgs, WalletStorageProvider};
use crate::wallet::lookup::{
    overlay_certificates, HttpLookupResolver, OverlayCertificate, OverlayLookupResolver,
};
use crate::Result;

#[path = "discovery_search.rs"]
mod discovery_search;

impl<S, V> Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync + 'static,
    V: WalletServices + Send + Sync + 'static,
{
    pub(super) fn discovery_resolver(&self) -> HttpLookupResolver {
        match self.chain {
            Chain::Main => HttpLookupResolver::mainnet(),
            Chain::Test | Chain::Regtest => HttpLookupResolver::testnet(),
        }
    }

    /// The chain tracker the overlay's evidence is verified with: the
    /// services' header service, as the reference's
    /// `requireOverlayChainTracker` reads `services.getChainTracker()`
    /// (ts-stack@edf6e03 `Wallet.ts:894-901`). With none (`Services` built
    /// with no `chaintracks_url`), or a fault reaching it, the door asks no
    /// resolver and answers no overlay certificates, as `queryOverlay`
    /// answers `[]` (`identityUtils.ts:399`).
    async fn overlay_tracker(&self) -> Option<&dyn ChainTracker> {
        match self.services.get_chain_tracker().await {
            Ok(tracker) => Some(tracker),
            Err(error) => {
                tracing::debug!("No chain tracker for overlay evidence: {}", error);
                None
            }
        }
    }

    /// The resolver's outputs whose evidence verifies, decoded, verified
    /// and decrypted (`overlay_certificates`); a lookup failure or an answer
    /// past the evidence limits answers none.
    async fn verified_overlay(
        &self,
        outputs: Result<Vec<OutputListItem>>,
        tracker: &dyn ChainTracker,
    ) -> Vec<OverlayCertificate> {
        let chain = match self.chain {
            Chain::Main => ChainParams::mainnet(),
            Chain::Test => ChainParams::testnet(),
            Chain::Regtest => ChainParams::regtest(),
        };
        let verified = match outputs {
            Ok(outputs) => overlay_certificates(outputs, tracker, &chain).await,
            Err(error) => Err(error),
        };
        verified.unwrap_or_else(|error| {
            tracing::debug!("Overlay lookup failed (no overlay certificates): {}", error);
            vec![]
        })
    }

    async fn discovery_settings(&self) -> Result<WalletSettings> {
        match &self.options.settings_manager {
            Some(manager) => manager.get().await,
            None => Ok(WalletSettings::default_settings_for(self.chain).clone()),
        }
    }

    pub(super) async fn discover_identity_with_resolver(
        &self,
        args: DiscoverByIdentityKeyArgs,
        resolver: &dyn OverlayLookupResolver,
    ) -> Result<DiscoverCertificatesResult> {
        let settings = self.discovery_settings().await?;
        let certifiers = trusted_keys(&settings.trust_settings);
        // Local discovery is this port's extension. Do not page before merging.
        let find_args = FindCertificatesArgs {
            include_fields: Some(true),
            ..Default::default()
        };
        let mut certificates: Vec<_> = self
            .storage
            .find_certificates(&self.auth(), find_args)
            .await?
            .into_iter()
            .map(local_certificate)
            .collect();
        if let Some(tracker) = self.overlay_tracker().await {
            let outputs = resolver
                .lookup_by_identity_key(&args.identity_key, &certifiers)
                .await;
            certificates.extend(self.verified_overlay(outputs, tracker).await);
        }
        let certificates = filter_by_identity_key(certificates, &args.identity_key);
        finish(&settings, certificates, (args.offset, args.limit))
    }

    pub(super) async fn discover_attributes_with_resolver(
        &self,
        args: DiscoverByAttributesArgs,
        resolver: &dyn OverlayLookupResolver,
    ) -> Result<DiscoverCertificatesResult> {
        let settings = self.discovery_settings().await?;
        let certifiers = trusted_keys(&settings.trust_settings);
        let certificates = match self.overlay_tracker().await {
            Some(tracker) => {
                let outputs = resolver
                    .lookup_by_attributes(&args.attributes, &certifiers)
                    .await;
                self.verified_overlay(outputs, tracker).await
            }
            None => vec![],
        };
        // TS `Wallet.ts:1078`: bind the answer to the query before the transform.
        let certificates = filter_certificates_by_attributes(certificates, &args.attributes)?;
        finish(&settings, certificates, (args.offset, args.limit))
    }
}

/// The reference limits work before grouping, independently of result paging.
const MAX_IDENTITY_RESULTS: usize = 256;

pub(super) fn certifier_info(settings: &TrustSettings, key: &str) -> Option<IdentityCertifier> {
    let certifier = settings
        .trusted_certifiers
        .iter()
        .find(|c| c.identity_key == key)?;
    let mut end = 0;
    for (offset, ch) in certifier.description.char_indices() {
        if offset + ch.len_utf8() > 50 {
            break;
        }
        end = offset + ch.len_utf8();
    }
    // ECMAScript trimEnd, including BOM and excluding NEXT LINE (U+0085).
    let description = certifier.description[..end].trim_end_matches(|ch| {
        matches!(ch, '\u{0009}'..='\u{000d}' | ' ' | '\u{00a0}' | '\u{1680}'
            | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
            | '\u{205f}' | '\u{3000}' | '\u{feff}')
    });
    Some(IdentityCertifier {
        name: certifier.name.clone(),
        icon_url: certifier
            .icon_url
            .clone()
            .unwrap_or_else(|| "https://bsvblockchain.org/favicon.ico".into()),
        description: if description.len() < 5 {
            "Trusted certifier".into()
        } else {
            description.into()
        },
        trust: certifier.trust as u8,
    })
}

pub(super) fn filter_by_identity_key(
    certificates: Vec<OverlayCertificate>,
    identity_key: &str,
) -> Vec<OverlayCertificate> {
    certificates
        .into_iter()
        .filter(|c| c.subject.eq_ignore_ascii_case(identity_key))
        .collect()
}

/// TS `identityUtils.ts:283-293` `filterCertificatesByAttributes`. The typed
/// map is an object of strings, so the reference's non-object and non-string
/// `any` refusals are the typed boundary's. With `any` present its search
/// alone decides; otherwise every usable pair must match, and none is none.
pub(super) fn filter_certificates_by_attributes(
    certificates: Vec<OverlayCertificate>,
    attributes: &IndexMap<String, String>,
) -> Result<Vec<OverlayCertificate>> {
    let any = attributes.get("any").map(String::as_str);
    let expected = match any {
        None => discovery_search::usable_identity_attributes(attributes),
        Some(_) => vec![],
    };
    if any.is_none() && expected.is_empty() {
        return Ok(vec![]);
    }
    let mut kept = Vec::new();
    for certificate in certificates {
        if discovery_search::certificate_matches_attributes(
            &certificate.decrypted_fields,
            any,
            &expected,
        )? {
            kept.push(certificate);
        }
    }
    Ok(kept)
}

#[derive(Default)]
struct IdentityGroup {
    trust: u32,
    certifiers: HashSet<String>,
    certificates: HashSet<(String, String, String)>,
    members: Vec<IdentityCertificate>,
}

pub(super) fn transform(
    settings: &TrustSettings,
    certificates: Vec<OverlayCertificate>,
) -> Result<DiscoverCertificatesResult> {
    settings.validate()?;
    let mut groups: IndexMap<String, IdentityGroup> = IndexMap::new();
    let mut cache = HashMap::new();
    for cert in certificates.into_iter().take(MAX_IDENTITY_RESULTS) {
        if cert.subject.is_empty() || cert.certifier.is_empty() {
            continue;
        }
        let info = cache
            .entry(cert.certifier.clone())
            .or_insert_with(|| certifier_info(settings, &cert.certifier));
        let Some(info) = info else { continue };
        let group = groups.entry(cert.subject.clone()).or_default();
        let id = (
            cert.type_id.clone(),
            cert.serial_number.clone(),
            cert.certifier.clone(),
        );
        if !group.certificates.insert(id) {
            continue;
        }
        if group.certifiers.insert(cert.certifier.clone()) {
            group.trust += u32::from(info.trust);
        }
        group
            .members
            .push(cert.to_identity_certificate(info.clone()));
    }
    let mut certificates: Vec<_> = groups
        .into_values()
        .filter(|group| group.trust >= settings.trust_level)
        .flat_map(|group| group.members)
        .collect();
    // Stable sorting keeps the reference's group/member order for equal trust.
    certificates.sort_by_key(|cert| std::cmp::Reverse(cert.certifier_info.trust));
    Ok(DiscoverCertificatesResult {
        total_certificates: certificates.len() as u32,
        certificates,
    })
}

/// TS `Wallet.ts:228-233`: page only after the complete trust transform.
pub(super) fn discovery_page(
    mut result: DiscoverCertificatesResult,
    offset: Option<u32>,
    limit: Option<u32>,
) -> DiscoverCertificatesResult {
    result.certificates = result
        .certificates
        .into_iter()
        .skip(offset.unwrap_or(0) as usize)
        .take(limit.unwrap_or(10) as usize)
        .collect();
    result
}

fn local_certificate(cert: TableCertificate) -> OverlayCertificate {
    OverlayCertificate {
        type_id: cert.cert_type,
        serial_number: cert.serial_number,
        subject: cert.subject,
        certifier: cert.certifier,
        revocation_outpoint: Some(cert.revocation_outpoint),
        signature: Some(cert.signature),
        fields: IndexMap::new(),
        keyring: IndexMap::new(),
        decrypted_fields: IndexMap::new(),
    }
}

fn trusted_keys(settings: &TrustSettings) -> Vec<String> {
    let mut keys: Vec<_> = settings
        .trusted_certifiers
        .iter()
        .map(|c| c.identity_key.clone())
        .collect();
    keys.sort();
    keys
}

fn finish(
    settings: &WalletSettings,
    certificates: Vec<OverlayCertificate>,
    (offset, limit): (Option<u32>, Option<u32>),
) -> Result<DiscoverCertificatesResult> {
    Ok(discovery_page(
        transform(&settings.trust_settings, certificates)?,
        offset,
        limit,
    ))
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "discovery_decrypt_tests.rs"]
mod decrypt_tests;

#[cfg(all(test, feature = "sqlite"))]
#[path = "discovery_evidence_tests.rs"]
mod evidence_tests;
