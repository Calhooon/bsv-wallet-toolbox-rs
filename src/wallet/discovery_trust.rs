//! TS `identityUtils.ts:247-255,313-384`: bind, group and weight discovery.

use std::collections::{HashMap, HashSet};

use bsv_rs::wallet::{DiscoverCertificatesResult, IdentityCertificate, IdentityCertifier};
use indexmap::IndexMap;

use crate::managers::TrustSettings;
use crate::wallet::lookup::OverlayCertificate;
use crate::Result;

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
    certificates.sort_by(|a, b| b.certifier_info.trust.cmp(&a.certifier_info.trust));
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

use super::Wallet;
use crate::managers::WalletSettings;
use crate::services::WalletServices;
use crate::storage::{entities::TableCertificate, FindCertificatesArgs, WalletStorageProvider};
use crate::wallet::lookup::OverlayLookupResolver;
use bsv_rs::wallet::{DiscoverByAttributesArgs, DiscoverByIdentityKeyArgs};

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

impl<S, V> Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync + 'static,
    V: WalletServices + Send + Sync + 'static,
{
    async fn discovery_settings(&self) -> Result<WalletSettings> {
        Ok(WalletSettings::default_settings_for(self.chain).clone())
    }

    pub(super) async fn discover_identity_with_resolver(
        &self,
        args: DiscoverByIdentityKeyArgs,
        resolver: &dyn OverlayLookupResolver,
    ) -> Result<DiscoverCertificatesResult> {
        let settings = self.discovery_settings().await?;
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
        match resolver.lookup_by_identity_key(&args.identity_key).await {
            Ok(overlay) => certificates.extend(overlay),
            Err(error) => tracing::debug!("Overlay lookup failed (using local only): {}", error),
        }
        let certificates = filter_by_identity_key(certificates, &args.identity_key);
        Ok(discovery_page(
            transform(&settings.trust_settings, certificates)?,
            args.offset,
            args.limit,
        ))
    }

    pub(super) async fn discover_attributes_with_resolver(
        &self,
        args: DiscoverByAttributesArgs,
        resolver: &dyn OverlayLookupResolver,
    ) -> Result<DiscoverCertificatesResult> {
        let settings = self.discovery_settings().await?;
        let certificates = match resolver.lookup_by_attributes(&args.attributes).await {
            Ok(certificates) => certificates,
            Err(error) => {
                tracing::debug!("Overlay attribute lookup failed: {}", error);
                vec![]
            }
        };
        Ok(discovery_page(
            transform(&settings.trust_settings, certificates)?,
            args.offset,
            args.limit,
        ))
    }
}
