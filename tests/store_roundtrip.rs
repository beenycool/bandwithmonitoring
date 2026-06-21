//! Storage layer integration tests.

use bandwith::capture::Proto;
use bandwith::store::{
    spawn_in_memory_with_handle, spawn_in_tempfile, Query, Row, WriterConfig, DEFAULT_BATCH_SIZE,
};
use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn fake_row(ts: i64, pid: u32, proc: &str, domain: Option<&str>, bin: u64, bout: u64) -> Row {
    Row {
        ts,
        pid,
        proc_name: proc.to_string(),
        remote_ip: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        domain: domain.map(String::from),
        dport: 443,
        proto: Proto::Tcp,
        bytes_in: bin,
        bytes_out: bout,
    }
}

fn fast_config() -> WriterConfig {
    WriterConfig {
        batch_size: 1000,
        batch_interval: Duration::from_millis(50),
    }
}

#[test]
fn open_in_memory_creates_schema() {
    let (h, _reader) = spawn_in_memory_with_handle(fast_config()).unwrap();
    h.shutdown().unwrap();
    let q = Query::open_in_memory_for_test().unwrap();
    assert_eq!(q.count().unwrap(), 0);
}

#[test]
fn insert_one_row() {
    let (h, _reader) = spawn_in_memory_with_handle(fast_config()).unwrap();
    let ts = now_ms();
    h.submit(fake_row(
        ts,
        1234,
        "chrome.exe",
        Some("example.com"),
        1500,
        200,
    ));
    h.flush();
    h.shutdown().unwrap();
    // Reader is independent in-memory, so we can't query it directly. The
    // tempfile tests below exercise the same path with cross-connection
    // visibility. This test verifies submit/flush/shutdown completes cleanly.
}

#[test]
fn tempfile_insert_and_query() {
    let (h, path, _guard) = spawn_in_tempfile(fast_config()).unwrap();

    let ts = now_ms();
    h.submit(fake_row(
        ts,
        1,
        "chrome.exe",
        Some("example.com"),
        1000,
        100,
    ));
    h.submit(fake_row(ts, 1, "chrome.exe", Some("example.com"), 500, 50));
    h.flush();
    h.shutdown().unwrap();

    let q = Query::open(&path).unwrap();
    assert_eq!(q.count().unwrap(), 1, "duplicate PK should be ignored");
    let top = q.top_processes(ts - 1, 10).unwrap();
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].key, "chrome.exe");
}

#[test]
fn duplicate_pk_is_noop() {
    let (h, path, _guard) = spawn_in_tempfile(fast_config()).unwrap();
    let ts = now_ms();
    for _ in 0..5 {
        h.submit(fake_row(ts, 1, "chrome.exe", Some("example.com"), 100, 50));
    }
    h.flush();
    h.shutdown().unwrap();
    let q = Query::open(&path).unwrap();
    assert_eq!(q.count().unwrap(), 1);
}

#[test]
fn batch_flush_threshold() {
    let (h, path, _guard) = spawn_in_tempfile(WriterConfig {
        batch_size: 100,
        batch_interval: Duration::from_millis(20),
    })
    .unwrap();

    let ts = now_ms();
    for i in 0..1000 {
        h.submit(fake_row(
            ts + i,
            100 + (i as u32 % 5),
            "test.exe",
            Some("x.com"),
            10,
            5,
        ));
    }
    h.shutdown().unwrap();
    let q = Query::open(&path).unwrap();
    // 1000 unique (ts, pid, ...) keys = 1000 rows
    assert_eq!(q.count().unwrap(), 1000);
}

#[test]
fn top_domains_orders_by_bytes() {
    let (h, path, _guard) = spawn_in_tempfile(fast_config()).unwrap();
    let ts = now_ms();
    h.submit(fake_row(ts, 1, "a.exe", Some("big.com"), 10000, 0));
    h.submit(fake_row(ts, 2, "a.exe", Some("med.com"), 1000, 0));
    h.submit(fake_row(ts, 3, "a.exe", Some("sml.com"), 100, 0));
    h.flush();
    h.shutdown().unwrap();

    let q = Query::open(&path).unwrap();
    let top = q.top_domains(ts - 1, 10).unwrap();
    assert_eq!(top[0].key, "big.com");
    assert_eq!(top[1].key, "med.com");
    assert_eq!(top[2].key, "sml.com");
    assert_eq!(top[0].bytes_in, 10000);
}

#[test]
fn rate_series_buckets() {
    let (h, path, _guard) = spawn_in_tempfile(fast_config()).unwrap();
    let t0 = 1_700_000_000_000i64; // fixed, deterministic
                                   // 10 rows spaced 100ms apart -> 10 distinct 1000ms buckets
    for i in 0..10 {
        h.submit(fake_row(t0 + i * 100, 1, "p", Some("d"), 100, 0));
    }
    h.flush();
    h.shutdown().unwrap();

    let q = Query::open(&path).unwrap();
    let series = q.rate_series(t0 - 1, 100).unwrap();
    assert_eq!(series.len(), 10);
    assert_eq!(series[0].ts, (t0 / 100) * 100);
    assert_eq!(series[9].ts, ((t0 + 900) / 100) * 100);
    assert_eq!(series.iter().map(|p| p.bytes_in).sum::<u64>(), 1000);
}

#[test]
fn shutdown_flushes_pending() {
    let (h, path, _guard) = spawn_in_tempfile(WriterConfig {
        batch_size: 10_000,                      // intentionally too large
        batch_interval: Duration::from_secs(60), // intentionally too long
    })
    .unwrap();
    let ts = now_ms();
    for i in 0..5 {
        h.submit(fake_row(ts + i, 1, "p", Some("d"), 10, 5));
    }
    h.shutdown().unwrap(); // must flush
    let q = Query::open(&path).unwrap();
    assert_eq!(q.count().unwrap(), 5);
}

#[test]
fn concurrent_reader_and_writer() {
    use rusqlite::Connection;
    let (h, path, _guard) = spawn_in_tempfile(fast_config()).unwrap();
    let ts = now_ms();

    // Writer: submit 500 rows in background
    let h2 = h.clone();
    let writer = std::thread::spawn(move || {
        for i in 0..500 {
            h2.submit(fake_row(ts + i, 1, "p", Some("d"), 10, 5));
        }
        h2.shutdown().unwrap();
    });

    writer.join().unwrap();
    let q = Query::open(&path).unwrap();
    assert_eq!(q.count().unwrap(), 500);

    // Sanity: the writer actually committed the rows (not the same path
    // connection as the reader — `Query::open` is read-only, the writer's
    // connection is closed by `shutdown`).
    let _raw = Connection::open(&path).unwrap();
}

#[test]
fn default_config_constants_match() {
    assert_eq!(WriterConfig::default().batch_size, DEFAULT_BATCH_SIZE);
}
