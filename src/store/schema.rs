//! Schema definitions and migrations. Real impl in phase 2.

pub const SCHEMA_VERSION: i32 = 1;

/// SQL statements applied on first open.
pub const MIGRATIONS: &[&str] = &[
    // Phase 2 will replace this with the real flows + flows_daily tables.
    "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
];
