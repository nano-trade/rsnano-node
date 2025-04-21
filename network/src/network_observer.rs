use anyhow::Error;

use crate::{ChannelDirection, NetworkError};
use std::net::SocketAddrV6;

pub trait NetworkObserver: Send + Sync {
    fn connect_error(&self, _peer: SocketAddrV6, _e: Error) {}
    fn attempt_timeout(&self, _peer: SocketAddrV6) {}
    fn attempt_cancelled(&self, _peer: SocketAddrV6) {}
    fn merge_peer(&self) {}
    fn accept_failure(&self) {}
}

pub struct NullNetworkObserver {}

impl NullNetworkObserver {
    pub fn new() -> Self {
        Self {}
    }
}

impl NetworkObserver for NullNetworkObserver {}
