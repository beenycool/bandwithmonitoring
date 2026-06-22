//! Reverse-DNS resolution. Public-facing types and the worker task.

pub mod cache;

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use hickory_resolver::config::{NameServerConfig, ResolverConfig, ResolverOpts};
use hickory_resolver::lookup::Lookup;
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::proto::rr::RData;
use hickory_resolver::TokioResolver;
use tokio::sync::mpsc;

pub use cache::{is_public_ip, CacheConfig, DnsCache};

#[derive(Debug, Clone)]
pub struct ResolverConfig2 {
    pub servers: Vec<std::net::SocketAddr>,
    pub timeout: Duration,
    pub cache: CacheConfig,
}

impl Default for ResolverConfig2 {
    fn default() -> Self {
        Self {
            servers: vec!["1.1.1.1:53".parse().unwrap(), "1.0.0.1:53".parse().unwrap()],
            timeout: Duration::from_secs(3),
            cache: CacheConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResolveRequest {
    pub ip: IpAddr,
}

#[derive(Debug, Clone)]
pub struct ResolveResult {
    pub ip: IpAddr,
    pub name: Option<String>,
}

#[derive(Clone)]
pub struct Resolver {
    cache: DnsCache,
    tx: mpsc::Sender<ResolveRequest>,
    inner: Arc<TokioResolver>,
}

impl Resolver {
    pub fn spawn(
        cfg: ResolverConfig2,
        runtime: &tokio::runtime::Handle,
    ) -> anyhow::Result<(Self, mpsc::Receiver<ResolveResult>)> {
        let mut rc = ResolverConfig::from_parts(None, Vec::new(), Vec::new());
        for addr in &cfg.servers {
            rc.add_name_server(NameServerConfig::udp_and_tcp(addr.ip()));
        }

        let resolver = TokioResolver::builder_with_config(rc, TokioRuntimeProvider::default())
            .with_options(ResolverOpts::default())
            .build()?;
        let inner = Arc::new(resolver);

        let cache = DnsCache::new(cfg.cache);
        let (tx, rx) = mpsc::channel::<ResolveRequest>(1024);
        let (result_tx, result_rx) = mpsc::channel::<ResolveResult>(1024);

        let inner_w = inner.clone();
        let cache_w = cache.clone();
        runtime.spawn(worker(rx, result_tx, inner_w, cache_w, cfg.timeout));

        Ok((Self { cache, tx, inner }, result_rx))
    }

    pub fn request(&self, ip: IpAddr) {
        if !is_public_ip(ip) {
            return;
        }
        let _ = self.tx.try_send(ResolveRequest { ip });
    }

    pub fn cached_name(&self, ip: IpAddr) -> Option<String> {
        let handle = tokio::runtime::Handle::try_current().ok()?;
        handle.block_on(self.cache.cached_name(ip))
    }

    pub fn lookup_blocking(&self, ip: IpAddr) -> Option<String> {
        let name = reverse_dns_name(&ip).ok()?;
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        runtime.block_on(async {
            let q = hickory_resolver::proto::rr::Name::from_str_relaxed(name.as_str()).ok()?;
            let r = self.inner.reverse_lookup(q).await.ok()?;
            extract_ptr_name(&r)
        })
    }

    pub fn cache(&self) -> &DnsCache {
        &self.cache
    }
}

fn extract_ptr_name(lookup: &Lookup) -> Option<String> {
    for record in lookup.answers() {
        if let RData::PTR(ptr) = &record.data {
            return Some(ptr.0.to_string());
        }
    }
    None
}

fn reverse_dns_name(ip: &IpAddr) -> anyhow::Result<String> {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            Ok(format!(
                "{}.{}.{}.{}.in-addr.arpa",
                octets[3], octets[2], octets[1], octets[0]
            ))
        }
        IpAddr::V6(v6) => {
            let segments = v6.segments();
            let mut nibbles = Vec::with_capacity(32);
            for seg in segments {
                nibbles.push(format!("{:04x}", seg));
            }
            let joined = nibbles.join("");
            let chars: Vec<char> = joined.chars().collect();
            let reversed: String = chars.into_iter().rev().collect();
            let dotted: String = reversed
                .chars()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(".");
            Ok(format!("{}.ip6.arpa", dotted))
        }
    }
}

async fn worker(
    mut rx: mpsc::Receiver<ResolveRequest>,
    result_tx: mpsc::Sender<ResolveResult>,
    resolver: Arc<TokioResolver>,
    cache: DnsCache,
    timeout: Duration,
) {
    while let Some(req) = rx.recv().await {
        let ip = req.ip;
        if let Some(Some(name)) = cache.get(ip).await {
            let _ = result_tx
                .send(ResolveResult {
                    ip,
                    name: Some(name),
                })
                .await;
            continue;
        }
        let name_opt = match reverse_dns_name(&ip) {
            Ok(name) => {
                let query = hickory_resolver::proto::rr::Name::from_str_relaxed(name.as_str()).ok();
                match query {
                    Some(q) => {
                        let lookup =
                            tokio::time::timeout(timeout, resolver.reverse_lookup(q)).await;
                        match lookup {
                            Ok(Ok(rrs)) => extract_ptr_name(&rrs),
                            _ => None,
                        }
                    }
                    None => None,
                }
            }
            Err(_) => None,
        };

        match &name_opt {
            Some(name) => cache.put_positive(ip, name.clone()).await,
            None => cache.put_negative(ip).await,
        }
        let _ = result_tx.send(ResolveResult { ip, name: name_opt }).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    /// No network: just test the PTR name builder.
    #[test]
    fn reverse_name_v4() {
        let n = reverse_dns_name(&IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))).unwrap();
        assert_eq!(n, "1.1.1.1.in-addr.arpa");
    }

    #[test]
    fn reverse_name_v6() {
        let n = reverse_dns_name(
            &"2606:4700:4700::1111"
                .parse::<std::net::Ipv6Addr>()
                .unwrap()
                .into(),
        )
        .unwrap();
        assert!(n.ends_with(".ip6.arpa"));
        assert_eq!(
            n,
            "1.1.1.1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.7.4.0.0.7.4.6.0.6.2.ip6.arpa"
        );
    }
}
