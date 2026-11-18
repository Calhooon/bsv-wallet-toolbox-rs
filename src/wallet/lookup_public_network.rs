//! The public-network rule the reference's lookup client holds on every
//! tracker and host it calls when HTTP is not enabled
//! (`LookupResolver.ts` `HTTPSOverlayLookupFacilitator`, which takes
//! `PublicHTTPSFetch.ts` `createPublicNetworkFetch`): a URL is an `https`
//! one with no credentials or fragment; an address literal is a public
//! one; a name resolves to 1 to 64 addresses, every one public, and the
//! connection goes to one of them.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// The most addresses one name may resolve to (`MAX_DNS_ADDRESSES`).
pub(crate) const MAX_DNS_ADDRESSES: usize = 64;

/// Whether an IPv4 address is public, by the reference's list.
fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
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

/// Whether an address is public, as the reference's
/// `isPublicNetworkAddress` reads it: the IPv4 list above, for an IPv4
/// address or one mapped into IPv6; for any other IPv6 address, the
/// IPv4 address a mapped, compatible, NAT64 or 6to4 address carries is
/// public, and the address is global unicast and none of the unspecified,
/// loopback, IANA special `2001::/23`, documentation, unique local,
/// link-local, site-local or multicast ranges.
pub(crate) fn is_public_network_address(ip: IpAddr) -> bool {
    let v6 = match ip {
        IpAddr::V4(v4) => return public_v4(v4),
        IpAddr::V6(v6) => v6,
    };
    // `::ffff:a.b.c.d` is read as its IPv4 address (`mappedV4`).
    if let Some(v4) = v6.to_ipv4_mapped() {
        return public_v4(v4);
    }
    let p = v6.segments();
    let embedded = |start: usize| {
        let [a, b] = p[start].to_be_bytes();
        let [c, d] = p[start + 1].to_be_bytes();
        Ipv4Addr::new(a, b, c, d)
    };
    let compatible_or_mapped = p[..5].iter().all(|&x| x == 0) && (p[5] == 0 || p[5] == 0xffff);
    let nat64 = p[0] == 0x0064 && p[1] == 0xff9b && p[2..6].iter().all(|&x| x == 0);
    let six_to_four = p[0] == 0x2002;
    if compatible_or_mapped || nat64 {
        if !public_v4(embedded(6)) {
            return false;
        }
    } else if six_to_four && !public_v4(embedded(1)) {
        return false;
    }
    let all_zero = p.iter().all(|&x| x == 0);
    let loopback = p[..7].iter().all(|&x| x == 0) && p[7] == 1;
    let global_unicast = (p[0] & 0xe000) == 0x2000;
    let iana_special = p[0] == 0x2001 && p[1] < 0x0200;
    let documentation = (p[0] == 0x2001 && p[1] == 0x0db8) || (p[0] & 0xfff0) == 0x3ff0;
    !(all_zero
        || loopback
        || !global_unicast
        || iana_special
        || documentation
        || (p[0] & 0xfe00) == 0xfc00
        || (p[0] & 0xffc0) == 0xfe80
        || (p[0] & 0xffc0) == 0xfec0
        || (p[0] & 0xff00) == 0xff00)
}

/// Whether the public client may send to `url` (`checkedTarget`): `https`,
/// no user name, password or fragment, and a host that is either a name
/// (its addresses are [`PublicResolver`]'s to check) or a public address.
pub(crate) fn public_target(url: &reqwest::Url) -> bool {
    let shape = url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none();
    let host = match url.host_str() {
        Some(text) => match text
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
        {
            Ok(ip) => is_public_network_address(ip),
            Err(_) => true,
        },
        None => false,
    };
    shape && host
}

/// The public client's name resolution: the system's answer for a name,
/// taken only when it holds 1 to 64 addresses, every one public; the
/// connection is made to those addresses alone.
pub(crate) struct PublicResolver;

impl Resolve for PublicResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let found: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if found.is_empty()
                || found.len() > MAX_DNS_ADDRESSES
                || found.iter().any(|a| !is_public_network_address(a.ip()))
            {
                return Err(format!("{host} resolved to a non-public address").into());
            }
            Ok(Box::new(found.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
#[path = "lookup_public_network_tests.rs"]
mod tests;
