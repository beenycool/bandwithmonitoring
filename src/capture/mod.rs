//! Network capture subsystem.
//!
//! On Windows, [`etw`] consumes ETW kernel-network events and produces
//! `ConnEvent`s. On other platforms the module compiles but does nothing —
//! this keeps `cargo check` green on Linux CI.

#[cfg(windows)]
pub mod etw;
pub mod flow;
#[cfg(windows)]
pub mod process;

#[cfg(windows)]
pub use etw::EtwCapture;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Proto {
    Tcp,
    Udp,
}

impl Proto {
    pub fn as_str(self) -> &'static str {
        match self {
            Proto::Tcp => "tcp",
            Proto::Udp => "udp",
        }
    }
}

/// Direction of bytes in a single event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Local -> remote (upload).
    Out,
    /// Remote -> local (download).
    In,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Out => "out",
            Direction::In => "in",
        }
    }
}

/// A single byte-delta event from the network stack.
#[derive(Debug, Clone)]
pub struct ConnEvent {
    pub ts_ms: i64,
    pub pid: u32,
    pub proto: Proto,
    pub remote_ip: std::net::IpAddr,
    pub remote_port: u16,
    pub bytes: u64,
    pub direction: Direction,
}
