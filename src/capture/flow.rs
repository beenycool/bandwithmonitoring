//! In-memory flow aggregator. Sums bytes per (pid, remote_ip, port, proto)
//! and flushes deltas to a `WriterHandle` at a fixed cadence.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use crate::capture::{now_ms, ConnEvent, Direction, Proto};
use crate::store::{Row, WriterHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlowKey {
    pub pid: u32,
    pub remote_ip: IpAddr,
    pub remote_port: u16,
    pub proto: Proto,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FlowStats {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub last_seen_ms: i64,
}

pub struct FlowAggregator {
    flows: HashMap<FlowKey, FlowStats>,
    /// Process name cache, looked up on flush.
    proc_names: HashMap<u32, String>,
    /// Optional resolver hint (filled in phase 5). Keyed by remote_ip.
    domains: HashMap<IpAddr, String>,
    /// Last flush timestamp; used to compute the bucket `ts` for writes.
    last_flush: Instant,
    /// Configurable flush interval. Default 2s.
    flush_interval: Duration,
    /// Bypass for tests: if set, accumulate forever and expose snapshot.
    test_mode: bool,
}

impl FlowAggregator {
    pub fn new() -> Self {
        Self::with_interval(Duration::from_secs(2))
    }

    pub fn with_interval(flush_interval: Duration) -> Self {
        Self {
            flows: HashMap::new(),
            proc_names: HashMap::new(),
            domains: HashMap::new(),
            last_flush: Instant::now(),
            flush_interval,
            test_mode: false,
        }
    }

    /// Test-only: disable auto-flush; expose snapshot via `snapshot()`.
    pub fn for_test() -> Self {
        let mut s = Self::new();
        s.test_mode = true;
        s
    }

    /// Register a (pid -> name) mapping. Usually fed by `ProcessCache::refresh`.
    pub fn update_proc_name(&mut self, pid: u32, name: String) {
        self.proc_names.insert(pid, name);
    }

    /// Bulk-update proc names from a snapshot.
    pub fn update_proc_names(&mut self, map: HashMap<u32, String>) {
        self.proc_names.extend(map);
    }

    /// Register a reverse-DNS hint for an IP. Phase 5 wires this in.
    pub fn update_domain(&mut self, ip: IpAddr, domain: String) {
        self.domains.insert(ip, domain);
    }

    /// Observe one ETW event. Aggregates bytes by direction.
    pub fn observe(&mut self, ev: ConnEvent) {
        let key = FlowKey {
            pid: ev.pid,
            remote_ip: ev.remote_ip,
            remote_port: ev.remote_port,
            proto: ev.proto,
        };
        let entry = self.flows.entry(key).or_default();
        match ev.direction {
            Direction::In => entry.bytes_in += ev.bytes,
            Direction::Out => entry.bytes_out += ev.bytes,
        }
        entry.last_seen_ms = ev.ts_ms;
    }

    /// Whether enough time has passed since the last flush to warrant one.
    pub fn should_flush(&self) -> bool {
        self.last_flush.elapsed() >= self.flush_interval
    }

    /// Drain all accumulated flows into a `Vec<Row>` and reset state.
    /// Caller decides what to do with the rows (write them, drop them, etc.).
    pub fn drain_into_rows(&mut self) -> Vec<Row> {
        let now = now_ms();
        // Bucket ts: truncate to the flush-interval boundary so multiple events
        // for the same flow within one window collapse to one row.
        let bucket_ms = self.flush_interval.as_millis() as i64;
        let bucket = (now / bucket_ms) * bucket_ms;

        let mut out = Vec::with_capacity(self.flows.len());
        for (key, stats) in self.flows.drain() {
            out.push(Row {
                ts: bucket,
                pid: key.pid,
                proc_name: self
                    .proc_names
                    .get(&key.pid)
                    .cloned()
                    .unwrap_or_else(|| format!("pid:{}", key.pid)),
                remote_ip: key.remote_ip,
                domain: self.domains.get(&key.remote_ip).cloned(),
                dport: key.remote_port,
                proto: key.proto,
                bytes_in: stats.bytes_in,
                bytes_out: stats.bytes_out,
            });
        }
        self.last_flush = Instant::now();
        out
    }

    /// Convenience: if `should_flush()`, drain and submit to the writer.
    /// Returns the number of rows submitted.
    pub fn maybe_flush(&mut self, writer: &WriterHandle) -> usize {
        if !self.should_flush() {
            return 0;
        }
        let rows = self.drain_into_rows();
        let n = rows.len();
        for r in rows {
            writer.submit(r);
        }
        writer.flush();
        n
    }

    /// Force a flush regardless of timer. Used on shutdown.
    pub fn flush_now(&mut self, writer: &WriterHandle) -> usize {
        let rows = self.drain_into_rows();
        let n = rows.len();
        for r in rows {
            writer.submit(r);
        }
        writer.flush();
        n
    }

    /// Number of distinct flows currently tracked. For tests + status.
    pub fn len(&self) -> usize {
        self.flows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.flows.is_empty()
    }

    /// Test-only: snapshot the in-memory state without draining.
    pub fn snapshot(&self) -> Vec<(FlowKey, FlowStats)> {
        self.flows.iter().map(|(k, v)| (*k, *v)).collect()
    }
}

impl Default for FlowAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(pid: u32, ip: &str, port: u16, dir: Direction, bytes: u64) -> ConnEvent {
        ConnEvent {
            ts_ms: now_ms(),
            pid,
            proto: Proto::Tcp,
            remote_ip: ip.parse().unwrap(),
            remote_port: port,
            bytes,
            direction: dir,
        }
    }

    #[test]
    fn aggregates_bytes_by_direction() {
        let mut a = FlowAggregator::for_test();
        a.observe(ev(1, "1.1.1.1", 443, Direction::In, 1500));
        a.observe(ev(1, "1.1.1.1", 443, Direction::In, 500));
        a.observe(ev(1, "1.1.1.1", 443, Direction::Out, 200));
        let snap = a.snapshot();
        assert_eq!(snap.len(), 1);
        let (_, stats) = &snap[0];
        assert_eq!(stats.bytes_in, 2000);
        assert_eq!(stats.bytes_out, 200);
    }

    #[test]
    fn distinct_flows_are_tracked_separately() {
        let mut a = FlowAggregator::for_test();
        a.observe(ev(1, "1.1.1.1", 443, Direction::In, 100));
        a.observe(ev(1, "1.1.1.1", 80, Direction::In, 200));
        a.observe(ev(2, "1.1.1.1", 443, Direction::In, 300));
        a.observe(ev(1, "2.2.2.2", 443, Direction::In, 400));
        // Fifth event has the same key as the first (only direction differs);
        // it must collapse into flow 1 and bump its bytes_out. Direction is
        // intentionally NOT part of `FlowKey` — we sum per-direction in
        // `FlowStats` instead.
        a.observe(ev(1, "1.1.1.1", 443, Direction::Out, 50));
        assert_eq!(a.len(), 4);

        let snap = a.snapshot();
        let flow1 = snap
            .iter()
            .find(|(k, _)| {
                k.pid == 1 && k.remote_ip.to_string() == "1.1.1.1" && k.remote_port == 443
            })
            .expect("flow 1 should exist");
        assert_eq!(flow1.1.bytes_in, 100);
        assert_eq!(flow1.1.bytes_out, 50);
    }

    #[test]
    fn drain_clears_state() {
        let mut a = FlowAggregator::for_test();
        a.observe(ev(1, "1.1.1.1", 443, Direction::In, 100));
        let rows = a.drain_into_rows();
        assert_eq!(rows.len(), 1);
        assert!(a.is_empty());
    }

    #[test]
    fn rows_carry_proc_name_or_fallback() {
        let mut a = FlowAggregator::for_test();
        a.update_proc_name(42, "chrome.exe".to_string());
        a.observe(ev(42, "1.1.1.1", 443, Direction::In, 100));
        a.observe(ev(99, "1.1.1.1", 443, Direction::In, 200));
        let rows = a.drain_into_rows();
        let mut by_pid: std::collections::HashMap<u32, String> =
            rows.iter().map(|r| (r.pid, r.proc_name.clone())).collect();
        assert_eq!(by_pid.remove(&42).unwrap(), "chrome.exe");
        assert_eq!(by_pid.remove(&99).unwrap(), "pid:99");
    }

    #[test]
    fn rows_carry_resolved_domain_when_known() {
        let mut a = FlowAggregator::for_test();
        let ip: IpAddr = "1.1.1.1".parse().unwrap();
        a.update_domain(ip, "one.one.one.one".to_string());
        a.observe(ev(1, "1.1.1.1", 443, Direction::In, 100));
        let rows = a.drain_into_rows();
        assert_eq!(rows[0].domain.as_deref(), Some("one.one.one.one"));
    }
}
