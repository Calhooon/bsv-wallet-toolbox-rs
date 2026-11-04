//! #334: the production issuance client's refusals at connect, as the
//! reference builds its issuance fetch with `createPublicHTTPSFetch` from the
//! endpoint's origin (ts-stack@edf6e03 `Wallet.ts:773`,
//! `PublicHTTPSFetch.ts:372-377`): a credential-free HTTPS URL, a literal
//! address only when public (:193-204), every resolved address public
//! (:262-271), that answer pinned into the connection (:273-289), and no
//! redirect followed (:260). Under test and `bench-support` the #182 policy
//! client stands in its place, so the witnesses call these parts directly.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::services::providers::http_client::{public_pinned_client, PublicClient, Url};

/// `PublicHTTPSFetch.ts:199`.
pub(super) const CREDENTIAL_FREE: &str = "Restricted requests require a credential-free HTTPS URL";
/// `PublicHTTPSFetch.ts:203`.
pub(super) const LITERAL_NON_PUBLIC: &str = "Restricted HTTPS request targets a non-public address";
/// `PublicHTTPSFetch.ts:270`.
pub(super) const RESOLVED_NON_PUBLIC: &str =
    "Restricted HTTPS request resolved to a non-public address";
/// `PublicHTTPSFetch.ts:171`.
pub(super) const DNS_TIMED_OUT: &str = "Restricted HTTPS DNS resolution timed out";

/// `PublicHTTPSFetch.ts:47-48`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DNS_ADDRESSES: usize = 64;

/// The resolver seam (`PublicHTTPSFetch.ts:4`): a hostname to its addresses.
pub(super) type Resolver =
    fn(String) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<IpAddr>>> + Send>>;

/// The system resolver, as `defaultAddressResolver` (:154-159) asks
/// `dns.lookup` for every address.
#[cfg(not(any(test, feature = "bench-support")))]
pub(super) fn system_resolver(
    host: String,
) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<IpAddr>>> + Send>> {
    Box::pin(async move {
        let addresses = tokio::net::lookup_host((host.as_str(), 0)).await?;
        Ok(addresses.map(|address| address.ip()).collect())
    })
}

/// `isPublicNetworkAddress` (`PublicHTTPSFetch.ts:112-152`) over `std::net`:
/// the textual `::ffff:a.b.c.d` form takes the IPv4 rule (:114-115); any
/// other IPv6 form takes the IPv6 rule, its embedded IPv4 checked before it.
pub(super) fn is_public_network_address(address: &str) -> bool {
    let lower = address.to_ascii_lowercase();
    let normalized = lower.strip_prefix('[').unwrap_or(&lower);
    let normalized = normalized.strip_suffix(']').unwrap_or(normalized);
    let mapped = normalized.strip_prefix("::ffff:").and_then(ipv4_parts);
    if let Some(v4) = mapped.or_else(|| ipv4_parts(normalized)) {
        return is_public_v4(v4);
    }
    match normalized.parse::<Ipv6Addr>() {
        Ok(v6) => is_public_v6(v6),
        Err(_) => false,
    }
}

/// `ipv4Parts` (:60-67): four decimal parts of one to three digits, each at
/// most 255.
fn ipv4_parts(address: &str) -> Option<Ipv4Addr> {
    let mut octets = [0u8; 4];
    let mut parts = address.split('.');
    for octet in &mut octets {
        let part = parts.next()?;
        if part.is_empty() || part.len() > 3 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        *octet = part.parse().ok()?;
    }
    parts.next().is_none().then_some(Ipv4Addr::from(octets))
}

/// The IPv4 ranges of :117-129: 0/8, 10/8, 127/8, 100.64/10, 169.254/16,
/// 172.16/12, 192.0/16, 192.168/16, 198.18/15, 198.51/16, 203.0/16, 224/3.
fn is_public_v4(address: Ipv4Addr) -> bool {
    let [a, b, ..] = address.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && (b == 0 || b == 168))
        || (a == 198 && (b == 18 || b == 19 || b == 51))
        || (a == 203 && b == 0)
        || a >= 224)
}

/// `ipv4FromIpv6` (:101-110): the IPv4 inside `::/96` (compatible),
/// `::ffff:0:0/96` (mapped), `64:ff9b::/96` (NAT64) and `2002::/16` (6to4).
fn embedded_v4(parts: [u16; 8]) -> Option<Ipv4Addr> {
    let compatible_or_mapped =
        parts[..5].iter().all(|&p| p == 0) && (parts[5] == 0 || parts[5] == 0xffff);
    let nat64 = parts[0] == 0x0064 && parts[1] == 0xff9b && parts[2..6].iter().all(|&p| p == 0);
    let six_to_four = parts[0] == 0x2002;
    if !compatible_or_mapped && !nat64 && !six_to_four {
        return None;
    }
    let start = if six_to_four { 1 } else { 6 };
    let [a, b] = parts[start].to_be_bytes();
    let [c, d] = parts[start + 1].to_be_bytes();
    Some(Ipv4Addr::new(a, b, c, d))
}

/// The IPv6 rule of :134-151: an embedded non-public IPv4, `::`, `::1`,
/// outside `2000::/3`, `2001::/23`, `2001:db8::/32`, `3ff0::/12`, `fc00::/7`,
/// `fe80::/10`, `fec0::/10` and `ff00::/8` are non-public.
fn is_public_v6(address: Ipv6Addr) -> bool {
    let v6 = address.segments();
    if embedded_v4(v6).is_some_and(|v4| !is_public_v4(v4)) {
        return false;
    }
    let all_zero = v6.iter().all(|&p| p == 0);
    let loopback = v6[..7].iter().all(|&p| p == 0) && v6[7] == 1;
    let global_unicast = (v6[0] & 0xe000) == 0x2000;
    let iana_special_2001 = v6[0] == 0x2001 && v6[1] < 0x0200;
    let documentation = (v6[0] == 0x2001 && v6[1] == 0x0db8) || (v6[0] & 0xfff0) == 0x3ff0;
    !(all_zero
        || loopback
        || !global_unicast
        || iana_special_2001
        || documentation
        || (v6[0] & 0xfe00) == 0xfc00
        || (v6[0] & 0xffc0) == 0xfe80
        || (v6[0] & 0xffc0) == 0xfec0
        || (v6[0] & 0xff00) == 0xff00)
}

/// `checkedTarget` (:193-204), `allowHTTP` off as `createPublicHTTPSFetch`
/// passes it: an HTTPS URL with no username, password or fragment, and a
/// literal address only when public. The origin check (:190) holds by
/// construction: the transport's base is the endpoint's origin.
pub(super) fn checked_target(target: &Url) -> Result<()> {
    if target.scheme() != "https"
        || !target.username().is_empty()
        || target.password().is_some_and(|p| !p.is_empty())
        || target.fragment().is_some_and(|f| !f.is_empty())
    {
        return Err(Error::NetworkError(CREDENTIAL_FREE.to_string()));
    }
    if literal_address(target).is_some()
        && !is_public_network_address(target.host_str().unwrap_or_default())
    {
        return Err(Error::NetworkError(LITERAL_NON_PUBLIC.to_string()));
    }
    Ok(())
}

/// The host's literal address (:201): the WHATWG parser leaves an IPv6
/// literal bracketed and an IPv4 one dotted, and a domain neither.
fn literal_address(target: &Url) -> Option<IpAddr> {
    let host = target.host_str()?;
    match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(v6) => v6.parse::<Ipv6Addr>().ok().map(IpAddr::V6),
        None => host.parse::<Ipv4Addr>().ok().map(IpAddr::V4),
    }
}

/// :262-271: the hostname resolved within the deadline; no address, more
/// than 64, or any non-public one refused.
pub(super) async fn approved_addresses(
    resolver: Resolver,
    host: &str,
    deadline: Duration,
) -> Result<Vec<IpAddr>> {
    let addresses = tokio::time::timeout(deadline, resolver(host.to_string()))
        .await
        .map_err(|_| Error::NetworkError(DNS_TIMED_OUT.to_string()))?
        .map_err(|e| Error::NetworkError(e.to_string()))?;
    if addresses.is_empty()
        || addresses.len() > MAX_DNS_ADDRESSES
        || addresses
            .iter()
            .any(|address| !is_public_network_address(&address.to_string()))
    {
        return Err(Error::NetworkError(RESOLVED_NON_PUBLIC.to_string()));
    }
    Ok(addresses)
}

/// The client the approved answer is pinned into (the `lookup` of :273-289),
/// no redirect followed (:260's `redirect: 'error'`) and no proxy, so the
/// connection goes to the approved addresses and nowhere else
/// (`http_client::public_pinned_client`).
pub(super) fn pinned_client(host: &str, addresses: &[IpAddr]) -> Result<PublicClient> {
    // Port 0: the client takes the URL's port.
    let pinned: Vec<SocketAddr> = addresses.iter().map(|&ip| SocketAddr::new(ip, 0)).collect();
    public_pinned_client(host, &pinned, REQUEST_TIMEOUT)
        .map_err(|e| Error::NetworkError(format!("Invalid certifier URL: {e}")))
}

/// The production issuance client for `target`: checked, resolved, approved
/// and pinned. A literal address is its own answer.
pub(super) async fn public_client(target: &Url, resolver: Resolver) -> Result<PublicClient> {
    checked_target(target)?;
    let host = target.host_str().unwrap_or_default();
    let addresses = match literal_address(target) {
        Some(ip) => vec![ip],
        None => approved_addresses(resolver, host, REQUEST_TIMEOUT).await?,
    };
    pinned_client(host, &addresses)
}
