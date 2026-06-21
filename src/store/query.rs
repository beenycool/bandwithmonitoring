//! Read-side aggregations. Uses its own rusqlite connection (WAL allows
//! concurrent reads while the writer commits).

use std::path::Path;

use rusqlite::{params, Connection, OpenFlags};

use crate::store::schema::run_migrations;

#[derive(Debug, Clone)]
pub struct TopRow {
    pub key: String,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct RatePoint {
    pub ts: i64, // bucket start, unix ms
    pub bytes_in: u64,
    pub bytes_out: u64,
}

pub struct Query {
    conn: Connection,
}

impl Query {
    /// Open a read-only handle against an existing DB file.
    pub fn open(db_path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        // Verify the schema (cheap, no writes) so we don't run queries
        // against a half-initialized DB.
        run_migrations(&conn)?;
        Ok(Self { conn })
    }

    /// Open a separate in-memory handle sharing the schema. The connection is
    /// not actually shared with the writer; for cross-connection tests use
    /// `Query::open` against a tempfile.
    pub fn open_in_memory_for_test() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        run_migrations(&conn)?;
        Ok(Self { conn })
    }

    /// Wrap an already-open connection. For tests that need to share a
    /// connection with a writer (only possible with file-backed DBs).
    #[doc(hidden)]
    pub fn from_connection(conn: Connection) -> Self {
        Self { conn }
    }

    /// Insert a row directly. Test-only escape hatch.
    #[doc(hidden)]
    pub fn _insert_for_test(&self, row: &super::Row) -> rusqlite::Result<()> {
        let mut stmt = self.conn.prepare_cached(
            "INSERT OR IGNORE INTO flows
                (ts, pid, proc_name, remote_ip, domain, dport, proto, bytes_in, bytes_out)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;
        stmt.execute(params![
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
        Ok(())
    }

    /// Top domains by total bytes (in + out) since `since_ms`.
    pub fn top_domains(&self, since_ms: i64, limit: usize) -> anyhow::Result<Vec<TopRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT COALESCE(domain, remote_ip) AS key,
                    SUM(bytes_in)  AS bin,
                    SUM(bytes_out) AS bout
             FROM flows
             WHERE ts >= ?1
             GROUP BY key
             ORDER BY (bin + bout) DESC
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![since_ms, limit as i64], |r| {
                Ok(TopRow {
                    key: r.get(0)?,
                    bytes_in: r.get::<_, i64>(1)? as u64,
                    bytes_out: r.get::<_, i64>(2)? as u64,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Top processes by total bytes since `since_ms`.
    pub fn top_processes(&self, since_ms: i64, limit: usize) -> anyhow::Result<Vec<TopRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT proc_name,
                    SUM(bytes_in)  AS bin,
                    SUM(bytes_out) AS bout
             FROM flows
             WHERE ts >= ?1
             GROUP BY proc_name
             ORDER BY (bin + bout) DESC
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![since_ms, limit as i64], |r| {
                Ok(TopRow {
                    key: r.get(0)?,
                    bytes_in: r.get::<_, i64>(1)? as u64,
                    bytes_out: r.get::<_, i64>(2)? as u64,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Time-bucketed total bytes. `bucket_ms` is the bucket size.
    pub fn rate_series(&self, since_ms: i64, bucket_ms: i64) -> anyhow::Result<Vec<RatePoint>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT (ts / ?2) * ?2 AS bucket,
                    SUM(bytes_in),
                    SUM(bytes_out)
             FROM flows
             WHERE ts >= ?1
             GROUP BY bucket
             ORDER BY bucket ASC",
        )?;
        let rows = stmt
            .query_map(params![since_ms, bucket_ms], |r| {
                Ok(RatePoint {
                    ts: r.get(0)?,
                    bytes_in: r.get::<_, i64>(1)? as u64,
                    bytes_out: r.get::<_, i64>(2)? as u64,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Total bytes since `since_ms`. Returns (in, out).
    pub fn total_since(&self, since_ms: i64) -> anyhow::Result<(u64, u64)> {
        let (bin, bout): (i64, i64) = self.conn.query_row(
            "SELECT COALESCE(SUM(bytes_in), 0), COALESCE(SUM(bytes_out), 0)
             FROM flows WHERE ts >= ?1",
            params![since_ms],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok((bin as u64, bout as u64))
    }

    /// Row count. For tests and health checks.
    pub fn count(&self) -> anyhow::Result<u64> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM flows", [], |r| r.get(0))?;
        Ok(n as u64)
    }
}
