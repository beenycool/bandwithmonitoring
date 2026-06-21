//! LRU + TTL DNS cache. Real impl in phase 5.

use std::net::IpAddr;

#[derive(Default)]
pub struct DnsCache {
    // Phase 5: wrap a moka::future::Cache<IpAddr, (String, Instant)>
    _placeholder: (),
}

impl DnsCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn resolve(&self, _ip: IpAddr) -> Option<String> {
        None
    }
}
