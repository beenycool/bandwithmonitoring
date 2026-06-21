//! Schema definitions and migration runner.
//!
//! Bump `SCHEMA_VERSION` and extend `MIGRATIONS` whenever the schema changes.

use rusqlite::Connection;

/// Current schema version. Stored in `schema_meta` row.
pub const SCHEMA_VERSION: i32 = 1;

/// PRAGMAs applied on every open. Idempotent.
const PRAGMAS: &[&str] = &[
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA foreign_keys = ON",
    "PRAGMA busy_timeout = 5000",
];

/// Schema statements. Append new versions; do not edit old ones.
const MIGRATIONS: &[&str] = &[
    // v1
    "CREATE TABLE IF NOT EXISTS schema_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
     )",
    "CREATE TABLE IF NOT EXISTS flows (
        ts         INTEGER NOT NULL,
        pid        INTEGER NOT NULL,
        proc_name  TEXT    NOT NULL,
        remote_ip  TEXT    NOT NULL,
        domain     TEXT,
        dport      INTEGER NOT NULL,
        proto      TEXT    NOT NULL,
        bytes_in   INTEGER NOT NULL,
        bytes_out  INTEGER NOT NULL,
        PRIMARY KEY (ts, pid, remote_ip, dport, proto)
     ) WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS flows_domain ON flows(domain)",
    "CREATE INDEX IF NOT EXISTS flows_proc   ON flows(proc_name)",
    "CREATE INDEX IF NOT EXISTS flows_ts     ON flows(ts)",
    "CREATE TABLE IF NOT EXISTS flows_daily (
        day        INTEGER NOT NULL,
        domain     TEXT,
        proc_name  TEXT,
        bytes_in   INTEGER NOT NULL,
        bytes_out  INTEGER NOT NULL,
        PRIMARY KEY (day, domain, proc_name)
     ) WITHOUT ROWID",
];

/// Apply PRAGMAs + migrations, set/verify schema version.
/// Returns the version that is now active in the database.
pub fn run_migrations(conn: &Connection) -> anyhow::Result<i32> {
    for pragma in PRAGMAS {
        // PRAGMA journal_mode returns a row; ignore result. Use execute.
        conn.execute_batch(pragma)?;
    }

    // Apply all migrations (each is idempotent thanks to IF NOT EXISTS).
    for stmt in MIGRATIONS {
        conn.execute_batch(stmt)?;
    }

    // Read or initialize version.
    let existing: Option<i32> = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| s.parse().ok());

    match existing {
        None => {
            conn.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('version', ?1)",
                [SCHEMA_VERSION.to_string()],
            )?;
            Ok(SCHEMA_VERSION)
        }
        Some(v) if v == SCHEMA_VERSION => Ok(v),
        Some(v) => anyhow::bail!(
            "schema version mismatch: db has {}, code expects {}",
            v,
            SCHEMA_VERSION
        ),
    }
}
