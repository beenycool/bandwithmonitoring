//! LRU + TTL reverse-DNS cache. Configurable per-IP TTLs.
//!
//! Backed by `moka::future::Cache`. Per-entry TTLs are chosen at insert time
//! by a small `Expiry` impl: positive entries get `max_ttl`, negative
//! (`None`) entries get `negative_ttl`. `time_to_live(max_ttl)` is set on
//! the builder as a hard upper bound for positive entries.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::{Duration, Instant};

use moka::future::Cache;
use moka::Expiry;

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_capacity: u64,
    pub min_ttl: Duration,
    pub max_ttl: Duration,
    pub negative_ttl: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: 5_000,
            min_ttl: Duration::from_secs(60 * 60),
            max_ttl: Duration::from_secs(60 * 60 * 24),
            negative_ttl: Duration::from_secs(60 * 5),
        }
    }
}

#[derive(Clone)]
pub struct DnsCache {
    inner: Cache<IpAddr, Option<String>>,
    #[allow(dead_code)]
    cfg: CacheConfig,
}

impl DnsCache {
    pub fn new(cfg: CacheConfig) -> Self {
        let expiry = DnsExpiry {
            positive_ttl: cfg.max_ttl,
            negative_ttl: cfg.negative_ttl,
        };
        let inner = Cache::builder()
            .max_capacity(cfg.max_capacity)
            .time_to_live(cfg.max_ttl)
            .expire_after(expiry)
            .build();
        Self { inner, cfg }
    }

    /// Get a cached result (positive or negative). Returns `Some(Some(name))`
    /// for a known positive, `Some(None)` for a known negative, `None` if
    /// not cached.
    pub async fn get(&self, ip: IpAddr) -> Option<Option<String>> {
        self.inner.get(&ip).await
    }

    pub async fn cached_name(&self, ip: IpAddr) -> Option<String> {
        self.inner.get(&ip).await.flatten()
    }

    pub async fn put_positive(&self, ip: IpAddr, name: String) {
        self.inner.insert(ip, Some(name)).await;
    }

    pub async fn put_negative(&self, ip: IpAddr) {
        self.inner.insert(ip, None).await;
    }

    pub async fn invalidate(&self, ip: IpAddr) {
        self.inner.invalidate(&ip).await;
    }

    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

impl Default for DnsCache {
    fn default() -> Self {
        Self::new(CacheConfig::default())
    }
}

struct DnsExpiry {
    positive_ttl: Duration,
    negative_ttl: Duration,
}

impl Expiry<IpAddr, Option<String>> for DnsExpiry {
    fn expire_after_create(
        &self,
        _key: &IpAddr,
        value: &Option<String>,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(if value.is_some() {
            self.positive_ttl
        } else {
            self.negative_ttl
        })
    }
}

/// Whether an IP is worth resolving at all. Skips private/loopback/link-local.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !v4.is_private()
                && !v4.is_loopback()
                && !v4.is_link_local()
                && !v4.is_broadcast()
                && !v4.is_documentation()
                && !v4.is_unspecified()
                && !is_v4_multicast(v4)
        }
        IpAddr::V6(v6) => {
            !v6.is_loopback()
                && !v6.is_unspecified()
                && !is_v6_unique_local(v6)
                && !is_v6_unicast_link_local(v6)
        }
    }
}

/// `fc00::/7` — RFC 4193 unique local addresses. Hand-rolled because
/// `Ipv6Addr::is_unique_local` is stable only from Rust 1.84.
fn is_v6_unique_local(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xfe00) == 0xfc00
}

/// `fe80::/10` — RFC 4291 link-local unicast. Hand-rolled for the same MSRV
/// reason as `is_v6_unique_local`.
fn is_v6_unicast_link_local(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

/// `224.0.0.0/4` — RFC 5771 IPv4 multicast. `Ipv4Addr::is_multicast` is
/// stable since Rust 1.84; hand-rolled to keep MSRV 1.75.
fn is_v4_multicast(v4: Ipv4Addr) -> bool {
    v4.octets()[0] >= 0xe0 && v4.octets()[0] <= 0xef
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[tokio::test]
    async fn put_and_get_positive() {
        let cache = DnsCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        assert_eq!(cache.cached_name(ip).await, None);
        cache.put_positive(ip, "one.one.one.one".to_string()).await;
        assert_eq!(
            cache.cached_name(ip).await.as_deref(),
            Some("one.one.one.one")
        );
    }

    #[tokio::test]
    async fn put_negative_does_not_return_name() {
        let cache = DnsCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9));
        cache.put_negative(ip).await;
        assert_eq!(cache.cached_name(ip).await, None);
        assert!(cache.get(ip).await.is_some(), "negative still cached");
    }

    #[tokio::test]
    async fn invalidate_removes_entry() {
        let cache = DnsCache::default();
        let ip = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        cache.put_positive(ip, "x".to_string()).await;
        cache.invalidate(ip).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(cache.cached_name(ip).await, None);
    }

    #[test]
    fn skips_private_ipv4() {
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 0, 1))));
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1))));
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))));
        assert!(!is_public_ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
    }

    #[test]
    fn allows_public_ipv4() {
        assert!(is_public_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        assert!(is_public_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(is_public_ip(IpAddr::V4(Ipv4Addr::new(142, 250, 80, 46))));
    }

    #[test]
    fn skips_private_ipv6() {
        assert!(!is_public_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!is_public_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
    }

    #[test]
    fn allows_public_ipv6() {
        assert!(is_public_ip("2606:4700:4700::1111".parse().unwrap()));
    }
}
