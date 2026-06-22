//! ETW consumer for `Microsoft-Windows-Kernel-Network`.
//!
//! Subscribes to the manifest-based network provider via ferrisetw and forwards
//! each TCP/UDP send/recv event into a [`crossbeam_channel::Sender<ConnEvent>`]
//! for downstream aggregation. All parsing errors are logged at `debug` level
//! and the capture keeps running — a single malformed event must never kill
//! the trace.
//!
//! Provider GUID: `{7DD42A49-5329-4832-8DFD-43D979153A88}` (per `netevent.h`).
//! Event ID layout (IPv4 unless suffixed with `V6`):
//!   - 12 `TcpIpSend`, 13 `TcpIpRecv`
//!   - 26 `UdpIpSend`, 27 `UdpIpRecv`
//!   - 28 `UdpIpSendV6`, 29 `UdpIpRecvV6`
//!   - 30 `TcpIpSendV6`, 31 `TcpIpRecvV6`
//!
//! Each event carries: `PID` (u32), `size` (u32), `dport` (u16), `sport`
//! (u16), `daddr` (4 or 16 bytes), `saddr` (4 or 16 bytes).

#![cfg(windows)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::Sender;
use ferrisetw::parser::Parser;
use ferrisetw::provider::Provider;
use ferrisetw::schema_locator::SchemaLocator;
use ferrisetw::trace::UserTrace;
use ferrisetw::EventRecord;
use parking_lot::Mutex;

use crate::capture::process::ProcessCache;
use crate::capture::{ConnEvent, Direction, Proto};

/// GUID for `Microsoft-Windows-Kernel-Network` (manifest-based kernel provider).
pub const KERNEL_NETWORK_GUID: &str = "7DD42A49-5329-4832-8DFD-43D979153A88";

// --- Event IDs (from netevent.h) ---------------------------------------------

const EVENT_TCP_SEND: u16 = 12;
const EVENT_TCP_RECV: u16 = 13;
const EVENT_UDP_SEND: u16 = 26;
const EVENT_UDP_RECV: u16 = 27;
const EVENT_UDP_SEND_V6: u16 = 28;
const EVENT_UDP_RECV_V6: u16 = 29;
const EVENT_TCP_SEND_V6: u16 = 30;
const EVENT_TCP_RECV_V6: u16 = 31;

/// How often the background process cache is refreshed.
const PROC_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Public capture handle. Owns the process cache and runs the ETW session.
pub struct EtwCapture {
    proc_cache: ProcessCache,
}

impl EtwCapture {
    pub fn new() -> anyhow::Result<Self> {
        let mut proc_cache = ProcessCache::new();
        if let Err(e) = proc_cache.refresh() {
            tracing::warn!(error = %e, "initial process snapshot failed");
        }
        Ok(Self { proc_cache })
    }

    /// Block until the ETW session is stopped (e.g. user kills the process).
    ///
    /// Spawns the process-cache refresh loop and the ETW consumer thread,
    /// then parks the calling thread. The trace's `Drop` impl (which fires
    /// on process exit) cleanly stops the kernel session. All parsed events
    /// are forwarded to `tx`; the aggregator / writer pipeline is driven by
    /// the caller.
    pub fn run(self, tx: Sender<ConnEvent>) -> anyhow::Result<()> {
        let proc_cache = Arc::new(Mutex::new(self.proc_cache));
        spawn_proc_refresh(proc_cache.clone());

        let provider = Provider::by_guid(KERNEL_NETWORK_GUID)
            .add_callback(
                move |record: &EventRecord, schema_locator: &SchemaLocator| {
                    handle_event(record, schema_locator, &tx);
                },
            )
            .build();

        let _trace = UserTrace::new()
            .named("bandwith-etw".to_string())
            .enable(provider)
            .start_and_process()
            .map_err(|e| anyhow::anyhow!("ETW start failed: {:?}", e))?;

        tracing::info!("ETW trace started; capturing network events");

        // Block the calling thread until the process is terminated. On
        // termination, the `_trace` Drop stops the kernel session.
        loop {
            std::thread::sleep(Duration::from_secs(60));
        }
    }
}

/// Periodic process-name refresh. The ETW callback can't hold a `&mut` to
/// the cache, so we share it via `Arc<Mutex<…>>` and refresh on a side
/// thread. The cache is currently used for debug logging only; wiring
/// the snapshot into the aggregator is a follow-up.
fn spawn_proc_refresh(cache: Arc<Mutex<ProcessCache>>) {
    std::thread::Builder::new()
        .name("bandwith-proc-refresh".into())
        .spawn(move || loop {
            std::thread::sleep(PROC_REFRESH_INTERVAL);
            let mut guard = cache.lock();
            if let Err(e) = guard.refresh() {
                tracing::warn!(error = %e, "process refresh failed");
            }
        })
        .expect("failed to spawn proc-refresh thread");
}

/// One event handler. All errors are swallowed (logged at debug) so a
/// single malformed event never kills the trace.
fn handle_event(record: &EventRecord, schema_locator: &SchemaLocator, tx: &Sender<ConnEvent>) {
    let event_id = record.event_id();
    let (proto, direction) = match event_id {
        EVENT_TCP_SEND | EVENT_TCP_SEND_V6 => (Proto::Tcp, Direction::Out),
        EVENT_TCP_RECV | EVENT_TCP_RECV_V6 => (Proto::Tcp, Direction::In),
        EVENT_UDP_SEND | EVENT_UDP_SEND_V6 => (Proto::Udp, Direction::Out),
        EVENT_UDP_RECV | EVENT_UDP_RECV_V6 => (Proto::Udp, Direction::In),
        _ => return,
    };

    let schema = match schema_locator.event_schema(record) {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(error = ?e, event_id, "schema lookup failed");
            return;
        }
    };
    let parser = Parser::create(record, &schema);

    // Fall back to the event header's process id if the manifest property
    // is missing (some events don't carry PID).
    let pid: u32 = parser
        .try_parse("PID")
        .unwrap_or_else(|_| record.process_id());
    let bytes: u32 = parser.try_parse("size").unwrap_or(0);

    let dport: u16 = match parser.try_parse("dport") {
        Ok(v) => v,
        Err(_) => return,
    };

    // daddr is a length-prefixed byte array: 4 bytes for IPv4, 16 for IPv6
    // (including IPv4-mapped IPv6). Length varies by event version.
    let daddr_bytes = match parser.try_parse::<Vec<u8>>("daddr") {
        Ok(b) => b,
        Err(_) => {
            tracing::debug!(event_id, "daddr parse failed");
            return;
        }
    };
    let remote_ip = match bytes_to_ip(&daddr_bytes) {
        Some(ip) => ip,
        None => {
            tracing::debug!(
                event_id,
                daddr_len = daddr_bytes.len(),
                "daddr length unsupported"
            );
            return;
        }
    };

    // Prefer the ETW event timestamp (kernel time, more accurate), but fall
    // back to wall-clock if the parser didn't yield a usable value.
    let ts_ms = etw_ts_to_ms(record.raw_timestamp()).unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    });

    let ev = ConnEvent {
        ts_ms,
        pid,
        proto,
        remote_ip,
        remote_port: dport,
        bytes: bytes as u64,
        direction,
    };

    if tx.send(ev).is_err() {
        tracing::warn!("receiver dropped, stopping event processing");
    }
}

/// Convert an ETW `TimeStamp` (100-ns FILETIME ticks since 1601) to unix ms.
fn etw_ts_to_ms(ts: i64) -> Option<i64> {
    // FILETIME epoch is 1601-01-01; unix epoch is 1970-01-01.
    // Delta = 11644473600 seconds = 116_444_736_00_000 ms.
    const FILETIME_UNIX_EPOCH_DELTA_MS: i64 = 11_644_473_600_000;
    let ms = (ts / 10_000) - FILETIME_UNIX_EPOCH_DELTA_MS;
    if ms >= 0 {
        Some(ms)
    } else {
        None
    }
}

/// Decode a raw `daddr` / `saddr` byte slice into an `IpAddr`.
/// 4 bytes -> IPv4. 16 bytes -> IPv6 (which may be IPv4-mapped).
fn bytes_to_ip(b: &[u8]) -> Option<std::net::IpAddr> {
    match b.len() {
        4 => Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
            b[0], b[1], b[2], b[3],
        ))),
        16 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(b);
            // Unwrap IPv4-mapped IPv6 (`::ffff:a.b.c.d`) so the rest of the
            // pipeline can treat it as plain IPv4.
            if let Some(v4) = ipv4_mapped(&octets) {
                return Some(std::net::IpAddr::V4(v4));
            }
            Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)))
        }
        _ => None,
    }
}

fn ipv4_mapped(o: &[u8; 16]) -> Option<std::net::Ipv4Addr> {
    if o[..10].iter().all(|b| *b == 0) && o[10] == 0xff && o[11] == 0xff {
        Some(std::net::Ipv4Addr::new(o[12], o[13], o[14], o[15]))
    } else {
        None
    }
}
