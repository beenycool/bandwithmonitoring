//! Batched async writer. Real impl in phase 2.

use std::path::Path;

pub struct Writer {
    _path: std::path::PathBuf,
}

impl Writer {
    pub async fn open(_path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            _path: _path.to_path_buf(),
        })
    }
}
