//! Batched async writer. Owns its own OS thread + rusqlite connection.

use std::net::IpAddr;
use std::path::Path;
#[cfg(feature = "test-support")]
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};
use rusqlite::Connection;

use crate::capture::Proto;
use crate::store::schema::run_migrations;

/// Default batch thresholds. Tunable later via config.
pub const DEFAULT_BATCH_SIZE: usize = 5_000;
pub const DEFAULT_BATCH_INTERVAL: Duration = Duration::from_secs(2);
/// Channel capacity. Sized for ~10s of heavy traffic before backpressure kicks in.
pub const CHANNEL_CAPACITY: usize = 50_000;

/// One row to be inserted into `flows`.
#[derive(Debug, Clone)]
pub struct Row {
    pub ts: i64,
    pub pid: u32,
    pub proc_name: String,
    pub remote_ip: IpAddr,
    pub domain: Option<String>,
    pub dport: u16,
    pub proto: Proto,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

enum WriterMsg {
    Row(Row),
    Flush,
    Shutdown,
}

/// Handle to the background writer. Cheap to clone (it's a Sender).
#[derive(Clone)]
pub struct WriterHandle {
    tx: Sender<WriterMsg>,
    /// Signaled by the writer thread when it has exited. The `Mutex` makes
    /// the `mpsc::Receiver` Sync so it can live in an `Arc`.
    done_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl WriterHandle {
    /// Submit a row. Blocks if the channel is full (backpressure).
    pub fn submit(&self, row: Row) {
        // If the receiver was dropped (writer panicked), send returns Err — we
        // log via tracing and drop the row rather than panic the caller.
        if self.tx.send(WriterMsg::Row(row)).is_err() {
            tracing::error!("writer channel closed; row dropped");
        }
    }

    /// Request a flush. The writer will commit any pending batch ASAP.
    pub fn flush(&self) {
        let _ = self.tx.send(WriterMsg::Flush);
    }

    /// Shut down the writer, flushing any pending batch, and block until the
    /// writer thread has actually exited. Returns an error if the writer was
    /// already shut down.
    pub fn shutdown(self) -> anyhow::Result<()> {
        self.tx
            .send(WriterMsg::Shutdown)
            .map_err(|_| anyhow::anyhow!("writer already shut down"))?;
        // Wait for the writer thread to actually finish. The Mutex makes
        // `done_rx` Sync; we take the lock only to call `recv` once.
        let rx = self.done_rx.lock().expect("done_rx poisoned");
        let _ = rx.recv();
        Ok(())
    }
}

/// Configuration for the writer thread.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub batch_size: usize,
    pub batch_interval: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            batch_interval: DEFAULT_BATCH_INTERVAL,
        }
    }
}

/// Spawn the writer thread against a file-backed DB.
pub fn spawn(db_path: &Path, cfg: WriterConfig) -> anyhow::Result<WriterHandle> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = open_connection(db_path)?;
    Ok(spawn_with_connection(conn, cfg))
}

/// Spawn against an in-memory DB. The second element is a *separate* in-memory
/// connection that has the schema applied but is **independent** of the
/// writer's database — reads through it will not see the writer's rows.
/// For tests that need cross-connection visibility, use `spawn_in_tempfile`.
pub fn spawn_in_memory_with_handle(
    cfg: WriterConfig,
) -> anyhow::Result<(WriterHandle, Connection)> {
    let writer_conn = Connection::open_in_memory()?;
    run_migrations(&writer_conn)?;
    let reader_conn = Connection::open_in_memory()?;
    run_migrations(&reader_conn)?;
    let handle = spawn_with_connection(writer_conn, cfg);
    Ok((handle, reader_conn))
}

/// Spawn against a tempdir-backed DB. Used by integration tests.
/// Returns the writer handle, the path to the DB file, and an opaque
/// "guard" value (the path again) that holds the tempdir alive. Tests
/// should open a `Query` via `Query::open(&path)` after writing.
#[cfg(feature = "test-support")]
pub fn spawn_in_tempfile(
    cfg: WriterConfig,
) -> anyhow::Result<(WriterHandle, PathBuf, TempDirGuard)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test.db");
    let writer = spawn(&path, cfg)?;
    // Leak the dir; the guard's Drop would delete it but the writer thread
    // may still be using the file. Tests are short-lived; OS cleans up /tmp.
    let guard = TempDirGuard(std::mem::ManuallyDrop::new(dir));
    Ok((writer, path, guard))
}

/// Holds a `tempfile::TempDir` alive for the duration of a test without
/// auto-cleanup. Drop is a no-op (we leak via `ManuallyDrop`).
#[cfg(feature = "test-support")]
pub struct TempDirGuard(#[allow(dead_code)] std::mem::ManuallyDrop<tempfile::TempDir>);

fn open_connection(db_path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(db_path)?;
    run_migrations(&conn)?;
    Ok(conn)
}

fn spawn_with_connection(conn: Connection, cfg: WriterConfig) -> WriterHandle {
    let (tx, rx) = bounded(CHANNEL_CAPACITY);
    let (done_tx, done_rx) = mpsc::channel();
    let done_rx = Arc::new(Mutex::new(done_rx));
    thread::Builder::new()
        .name("bandwith-writer".into())
        .spawn(move || {
            writer_loop(conn, rx, cfg);
            // Signal any waiting `shutdown()` calls that we're done.
            let _ = done_tx.send(());
        })
        .expect("failed to spawn writer thread");
    WriterHandle { tx, done_rx }
}

fn writer_loop(conn: Connection, rx: Receiver<WriterMsg>, cfg: WriterConfig) {
    let mut stmt = match prepare_insert(&conn) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "writer: failed to prepare insert; exiting");
            return;
        }
    };

    let mut batch: Vec<Row> = Vec::with_capacity(cfg.batch_size);
    let mut deadline = Instant::now() + cfg.batch_interval;

    loop {
        let timeout = deadline.saturating_duration_since(Instant::now());
        let recv: Result<WriterMsg, crossbeam_channel::RecvTimeoutError> = if batch.is_empty() {
            // Block indefinitely when idle (until Shutdown). Map the
            // "disconnected" error into `RecvTimeoutError::Disconnected` for
            // uniform handling below (we never get a timeout here).
            rx.recv()
                .map_err(|_| crossbeam_channel::RecvTimeoutError::Disconnected)
        } else {
            // Time-bound receive so we can flush by interval.
            rx.recv_timeout(timeout)
        };

        match recv {
            Ok(WriterMsg::Row(row)) => {
                batch.push(row);
                if batch.len() >= cfg.batch_size {
                    if let Err(e) = flush_batch(&conn, &mut stmt, &mut batch) {
                        tracing::error!(error = %e, "writer: flush failed");
                    }
                    deadline = Instant::now() + cfg.batch_interval;
                }
            }
            Ok(WriterMsg::Flush) => {
                if !batch.is_empty() {
                    if let Err(e) = flush_batch(&conn, &mut stmt, &mut batch) {
                        tracing::error!(error = %e, "writer: flush on Flush request failed");
                    }
                }
                deadline = Instant::now() + cfg.batch_interval;
            }
            Ok(WriterMsg::Shutdown) => {
                if !batch.is_empty() {
                    if let Err(e) = flush_batch(&conn, &mut stmt, &mut batch) {
                        tracing::error!(error = %e, "writer: final flush failed");
                    }
                }
                tracing::info!("writer shut down cleanly");
                return;
            }
            Err(recv_err) => {
                // Timeout (only possible when batch is non-empty) or channel
                // closed (all Senders dropped). Either way, flush what we have.
                if !batch.is_empty() {
                    if let Err(e) = flush_batch(&conn, &mut stmt, &mut batch) {
                        tracing::error!(error = %e, "writer: timed flush failed");
                    }
                }
                if recv_err == crossbeam_channel::RecvTimeoutError::Disconnected {
                    tracing::info!("writer: channel disconnected, exiting");
                    return;
                }
                deadline = Instant::now() + cfg.batch_interval;
            }
        }
    }
}

fn prepare_insert(conn: &Connection) -> rusqlite::Result<rusqlite::CachedStatement<'_>> {
    conn.prepare_cached(
        "INSERT OR IGNORE INTO flows
            (ts, pid, proc_name, remote_ip, domain, dport, proto, bytes_in, bytes_out)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
}

fn flush_batch(
    conn: &Connection,
    stmt: &mut rusqlite::CachedStatement<'_>,
    batch: &mut Vec<Row>,
) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    for row in batch.drain(..) {
        stmt.execute(rusqlite::params![
            row.ts,
            row.pid as i64,
            row.proc_name,
            row.remote_ip.to_string(),
            row.domain,
            row.dport as i64,
            row.proto.as_str(),
            row.bytes_in as i64,
            row.bytes_out as i64,
        ])?;
    }
    tx.commit()?;
    Ok(())
}
