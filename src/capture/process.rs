//! PID -> process name cache. Real implementation in phase 3.

#![cfg(windows)]

use std::collections::HashMap;

pub struct ProcessCache {
    map: HashMap<u32, String>,
}

impl ProcessCache {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn refresh(&mut self) -> anyhow::Result<()> {
        // Phase 3: NtQuerySystemInformation(SystemProcessInformation)
        Ok(())
    }

    pub fn name(&self, pid: u32) -> Option<&str> {
        self.map.get(&pid).map(String::as_str)
    }
}

impl Default for ProcessCache {
    fn default() -> Self {
        Self::new()
    }
}
