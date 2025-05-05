use std::sync::Arc;
use tokio::select;

use rsnano_core::{Networks, NodeId, ProtocolInfo};
use rsnano_messages::MessageDeserializer;
use rsnano_network::Channel;
use rsnano_network_protocol::InboundMessageQueue;

pub(crate) async fn run_loopback_channel_adapter(
    loopback: Arc<Channel>,
    node_id: NodeId,
    network: Networks,
    inbound: Arc<InboundMessageQueue>,
) {
    loopback.set_node_id(node_id);
    let protocol = ProtocolInfo::default_for(network);
    let mut deserializer = MessageDeserializer::new(protocol);
    loop {
        let res = select! {
            _ = loopback.cancelled() =>{
                return;
            },
          res = loopback.pop() => res
        };

        if let Some(entry) = res {
            deserializer.push(&entry.buffer);
            if let Some(Ok(m)) = deserializer.try_deserialize() {
                inbound.put(m.message, loopback.clone());
            }
        }
    }
}
