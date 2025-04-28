#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate strum_macros;

mod handshake_process;
mod handshake_stats;
mod inbound_message_queue;
mod latest_keepalives;
mod syn_cookies;

pub use handshake_process::*;
pub use handshake_stats::*;
pub use inbound_message_queue::*;
pub use latest_keepalives::*;
use rsnano_messages::Message;
use rsnano_network::ChannelId;
use std::sync::Arc;
pub use syn_cookies::*;

pub type MessageCallback = Arc<dyn Fn(ChannelId, &Message) + Send + Sync>;
