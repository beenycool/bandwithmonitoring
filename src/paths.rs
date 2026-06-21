//! Filesystem locations for data, config, logs.
//!
//! - Windows: `%APPDATA%\bandwith\` and `%LOCALAPPDATA%\bandwith\`
//! - Other:   `$XDG_CONFIG_HOME/bandwith/` and `$XDG_DATA_HOME/bandwith/`

use std::path::PathBuf;

pub fn data_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("bandwith")
}

pub fn cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("bandwith")
}

pub fn config_path() -> PathBuf {
    data_dir().join("config.toml")
}

pub fn db_path() -> PathBuf {
    data_dir().join("bandwith.db")
}

pub fn ensure_data_dir() -> std::io::Result<()> {
    std::fs::create_dir_all(data_dir())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_dir_ends_with_bandwith() {
        assert!(data_dir().ends_with("bandwith"));
    }

    #[test]
    fn db_path_is_under_data_dir() {
        assert!(db_path().starts_with(data_dir()));
    }
}
