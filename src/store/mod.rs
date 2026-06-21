//! SQLite-backed event store. Owns schema, writer (batched), and read query API.

pub mod query;
pub mod schema;
pub mod writer;

pub use query::{Query, RatePoint, TopRow};
pub use writer::{
    spawn, spawn_in_memory_with_handle, Row, WriterConfig, WriterHandle, DEFAULT_BATCH_INTERVAL,
    DEFAULT_BATCH_SIZE,
};
#[cfg(feature = "test-support")]
pub use writer::{spawn_in_tempfile, TempDirGuard};
