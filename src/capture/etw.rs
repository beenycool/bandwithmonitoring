//! ETW consumer for Microsoft-Windows-Kernel-Network.
//!
//! Real implementation lands in phase 4. Skeleton exists so the module
//! tree is complete and `cargo check` passes on Windows.

#![cfg(windows)]

use crate::capture::ConnEvent;

/// GUID for `Microsoft-Windows-Kernel-Network` (verified against
/// <https://learn.microsoft.com/en-us/windows/win32/etw/provider-guids>).
pub const KERNEL_NETWORK_GUID: &str = "{7DD42A49-5329-4832-8DFD-43D979153A88}";

/// Capture handle. Phase 4 owns a `ferrisetw::KernelTrace` inside.
pub struct EtwCapture {
    _private: (),
}

impl EtwCapture {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self { _private: () })
    }

    /// Blocking start. Phase 4 spawns the ferrisetw consumer thread and
    /// forwards events into the provided channel.
    pub fn run(self, _tx: crossbeam_channel::Sender<ConnEvent>) -> anyhow::Result<()> {
        // Phase 4: implement kernel trace, parse events, send.
        anyhow::bail!("EtwCapture::run is not yet implemented (phase 4)")
    }
}
