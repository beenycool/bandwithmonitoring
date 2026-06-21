//! Config loading. Real impl in phase 6.

use std::path::Path;

#[derive(Debug, Clone)]
pub struct Config {
    pub raw: toml::Value,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            raw: toml::Value::Table(toml::map::Map::new()),
        }
    }
}

impl Config {
    pub fn load(_path: &Path) -> anyhow::Result<Self> {
        // Phase 6: parse real config; for now return defaults.
        Ok(Self {
            raw: toml::Value::Table(Default::default()),
        })
    }
}
