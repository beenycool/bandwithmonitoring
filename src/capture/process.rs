//! Windows PID -> process name cache.
//!
//! Implementation uses `psapi::EnumProcesses` to enumerate PIDs, then
//! `OpenProcess` + `QueryFullProcessImageNameW` to resolve each PID to its
//! image path. We then take the file-name component of the path (e.g.
//! `chrome.exe`) for storage.
//!
//! Refresh is on demand and cheap: ~1ms for thousands of processes.
//!
//! Note: the spec suggested `NtQuerySystemInformation` first, but
//! `windows = "0.58"` does not expose `SystemProcessInformation` or
//! `SYSTEM_PROCESS_INFORMATION`. The spec explicitly permits swapping to
//! the `EnumProcesses` path in that case.

#![cfg(windows)]

use std::collections::HashMap;
use std::ffi::OsString;
use std::os::windows::ffi::OsStringExt;
use std::path::PathBuf;

use windows::Win32::Foundation::{CloseHandle, HANDLE};
use windows::Win32::System::ProcessStatus::{EnumProcesses, K32GetModuleFileNameExW};
use windows::Win32::System::Threading::{
    OpenProcess, QueryFullProcessImageNameW, PROCESS_NAME_FORMAT, PROCESS_QUERY_LIMITED_INFORMATION,
};

/// One row in the process snapshot.
#[derive(Debug, Clone)]
pub struct ProcInfo {
    pub pid: u32,
    pub name: String,
}

/// Maintains a (pid -> name) map. Refreshed by calling `refresh()`.
#[derive(Default)]
pub struct ProcessCache {
    by_pid: HashMap<u32, String>,
}

impl ProcessCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Re-scan the system process table. Safe to call frequently.
    pub fn refresh(&mut self) -> anyhow::Result<()> {
        let infos = snapshot()?;
        self.by_pid = infos.into_iter().map(|p| (p.pid, p.name)).collect();
        Ok(())
    }

    /// Look up a process name by PID.
    pub fn name(&self, pid: u32) -> Option<&str> {
        self.by_pid.get(&pid).map(String::as_str)
    }

    /// Snapshot of the entire map. Used by `FlowAggregator::update_proc_names`.
    pub fn snapshot_map(&self) -> HashMap<u32, String> {
        self.by_pid.clone()
    }

    /// Number of known PIDs.
    pub fn len(&self) -> usize {
        self.by_pid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_pid.is_empty()
    }
}

/// One-shot system snapshot. Returns `Vec<ProcInfo>`. Public for callers
/// that want a one-off read without owning a `ProcessCache`.
pub fn snapshot() -> anyhow::Result<Vec<ProcInfo>> {
    unsafe { snapshot_unsafe() }
}

/// Upper bound on the number of PIDs we'll request in a single `EnumProcesses`
/// call. On Windows a typical system has a few hundred processes, so 64 KiB
/// (= 16 384 PIDs) is plenty. We loop if the returned `bytes_needed` indicates
/// the buffer was full.
const PID_BUFFER_CAPACITY: usize = 16_384;
/// Initial buffer size in bytes. `EnumProcesses` returns the number of bytes
/// actually written; if it equals this size, we retry with a larger buffer.
const INITIAL_PID_BUFFER_BYTES: usize = PID_BUFFER_CAPACITY * std::mem::size_of::<u32>();

unsafe fn snapshot_unsafe() -> anyhow::Result<Vec<ProcInfo>> {
    // Collect PIDs. We may need to retry if the buffer fills up.
    let pids;
    let mut buf_bytes = INITIAL_PID_BUFFER_BYTES;
    loop {
        let mut bytes_needed: u32 = 0;
        let mut buf: Vec<u32> = vec![0u32; buf_bytes / std::mem::size_of::<u32>()];
        let res = EnumProcesses(
            buf.as_mut_ptr(),
            (buf.len() * std::mem::size_of::<u32>()) as u32,
            &mut bytes_needed,
        );
        if let Err(e) = res {
            return Err(anyhow::anyhow!("EnumProcesses failed: {}", e));
        }
        let written_pids = bytes_needed as usize / std::mem::size_of::<u32>();
        if written_pids < buf.len() {
            buf.truncate(written_pids);
            pids = buf;
            break;
        }
        // Buffer was filled: double and retry.
        buf_bytes = buf_bytes.saturating_mul(2);
        if buf_bytes > 4 * 1024 * 1024 {
            return Err(anyhow::anyhow!(
                "EnumProcesses: more than 1M PIDs, refusing to retry"
            ));
        }
    }

    // Resolve each PID to a process name.
    let mut out: Vec<ProcInfo> = Vec::with_capacity(pids.len());
    for &pid in &pids {
        if pid == 0 {
            // System Idle Process: skip — not a real user process.
            continue;
        }
        match query_process_name(pid) {
            Some(name) => out.push(ProcInfo { pid, name }),
            None => {
                // Process may have exited between EnumProcesses and OpenProcess,
                // or we don't have access rights. Skip silently — common in CI.
            }
        }
    }
    Ok(out)
}

/// Resolve a single PID to a short process name (`chrome.exe`, not the full
/// image path). Returns `None` on access denial or process exit.
unsafe fn query_process_name(pid: u32) -> Option<String> {
    let handle: HANDLE = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid).ok()?;
    if handle.0.is_null() {
        return None;
    }
    let result = query_process_name_with_handle(handle);
    let _ = CloseHandle(handle);
    result
}

unsafe fn query_process_name_with_handle(handle: HANDLE) -> Option<String> {
    // Prefer QueryFullProcessImageNameW — it gives the canonical path.
    let mut buf: Vec<u16> = vec![0u16; 1024];
    let mut size: u32 = buf.len() as u32;
    let ok = QueryFullProcessImageNameW(
        handle,
        PROCESS_NAME_FORMAT(0),
        windows::core::PWSTR(buf.as_mut_ptr()),
        &mut size,
    )
    .is_ok();
    if ok && size > 0 {
        buf.truncate(size as usize);
        let path = PathBuf::from(OsString::from_wide(&buf));
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            return Some(name.to_string());
        }
    }
    // Fall back to K32GetModuleFileNameExW (older API, still in psapi).
    let mut buf: Vec<u16> = vec![0u16; 1024];
    let copied = K32GetModuleFileNameExW(handle, HANDLE::default(), &mut buf) as usize;
    if copied == 0 {
        return None;
    }
    buf.truncate(copied);
    let path = PathBuf::from(OsString::from_wide(&buf));
    path.file_name().and_then(|s| s.to_str()).map(String::from)
}
