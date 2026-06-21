//! In-memory flow aggregator. Real implementation lands in phase 3.

use crate::capture::ConnEvent;

/// Key identifying a single network flow (one process, one remote endpoint, one proto).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlowKey {
    pub pid: u32,
    pub remote_ip: std::net::IpAddr,
    pub remote_port: u16,
    pub proto: crate::capture::Proto,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FlowStats {
    pub bytes_in: u64,
    pub bytes_out: u64,
}

#[derive(Default)]
pub struct FlowAggregator {
    // Phase 3 will add: HashMap<FlowKey, FlowStats>, last-flush timestamps, etc.
    _placeholder: std::marker::PhantomData<ConnEvent>,
}

impl FlowAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one event. Phase 3 replaces the body with real aggregation.
    pub fn observe(&mut self, _ev: ConnEvent) {
        // no-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::{Direction, Proto};
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn aggregator_compiles_and_accepts_events() {
        let mut agg = FlowAggregator::new();
        agg.observe(ConnEvent {
            ts_ms: 1_700_000_000_000,
            pid: 1234,
            proto: Proto::Tcp,
            remote_ip: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            remote_port: 443,
            bytes: 1500,
            direction: Direction::In,
        });
    }

    #[test]
    fn flow_keys_are_distinct() {
        let a = FlowKey {
            pid: 1,
            remote_ip: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            remote_port: 443,
            proto: Proto::Tcp,
        };
        let b = FlowKey {
            pid: 1,
            remote_ip: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            remote_port: 80, // different port
            proto: Proto::Tcp,
        };
        assert_ne!(a, b);
    }
}
