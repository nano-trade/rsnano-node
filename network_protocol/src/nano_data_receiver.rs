use std::sync::{atomic::Ordering, Arc, Mutex, RwLock, Weak};

use tracing::{debug, warn};

use rsnano_core::{NodeId, ProtocolInfo};
use rsnano_messages::*;
use rsnano_network::{
    Channel, ChannelDirection, ChannelMode, DataReceiver, Network, ReceiveResult, TrafficType,
};
use rsnano_stats::{DetailType, Direction, StatType, Stats};

use crate::{HandshakeProcess, HandshakeStats, HandshakeStatus, LatestKeepalives};

pub struct NanoDataReceiver {
    channel: Arc<Channel>,
    handshake_process: HandshakeProcess,
    serializer: MessageSerializer,
    message_deserializer: MessageDeserializer,
    try_enqueue: Arc<dyn Fn(Message, Arc<Channel>) -> bool + Send + Sync>,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    stats: Arc<Stats>,
    network: Weak<RwLock<Network>>,
    first_message: bool,
    node_id: NodeId,
    handshake_stats: Arc<HandshakeStats>,
    retry_enqueue: Option<Message>,
}

impl NanoDataReceiver {
    pub fn new(
        channel: Arc<Channel>,
        handshake_process: HandshakeProcess,
        message_deserializer: MessageDeserializer,
        try_enqueue: Arc<dyn Fn(Message, Arc<Channel>) -> bool + Send + Sync>,
        latest_keepalives: Arc<Mutex<LatestKeepalives>>,
        stats: Arc<Stats>,
        network: Weak<RwLock<Network>>,
        handshake_stats: Arc<HandshakeStats>,
        protocol: ProtocolInfo,
    ) -> Self {
        Self {
            channel,
            handshake_process,
            serializer: MessageSerializer::new_with_buffer_size(protocol, 512),
            message_deserializer,
            try_enqueue,
            latest_keepalives,
            stats,
            network,
            first_message: true,
            node_id: NodeId::ZERO,
            handshake_stats,
            retry_enqueue: None,
        }
    }

    pub fn ensure_handshake(&mut self) {
        if self.channel.direction() == ChannelDirection::Outbound {
            self.initiate_handshake();
        }
    }

    fn initiate_handshake(&mut self) {
        let peer = self.channel.peer_addr();
        let result = self.handshake_process.initiate_handshake(peer);

        match result {
            Ok(handshake) => {
                let data = self
                    .serializer
                    .serialize(&Message::NodeIdHandshake(handshake));

                debug!("Initiating handshake query ({})", peer);
                let enqueued = self.channel.send(data, TrafficType::Generic);
                if enqueued {
                    self.handshake_stats
                        .handshakes_sent
                        .fetch_add(1, Ordering::Relaxed);
                    self.handshake_stats
                        .initiate
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.handshake_stats
                        .network_error
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(%peer, "Could not send handshake");
                    self.channel.close();
                }
            }
            Err(e) => {
                warn!("Could not initiate handshake: {:?}", e);
                self.channel.close();
            }
        }
    }

    fn queue_realtime(&mut self, message: Message) -> ReceiveResult {
        let enqueued = self.try_enqueue(message.clone());
        if enqueued {
            ReceiveResult::Continue
        } else {
            debug_assert!(self.retry_enqueue.is_none());
            self.retry_enqueue = Some(message);
            ReceiveResult::Pause
        }
    }

    fn try_enqueue(&self, message: Message) -> bool {
        (self.try_enqueue)(message, self.channel.clone())
    }

    fn set_last_keepalive(&self, keepalive: Keepalive) {
        self.latest_keepalives
            .lock()
            .unwrap()
            .insert(self.channel.channel_id(), keepalive);
    }

    fn process_realtime(&mut self, message: Message) -> ReceiveResult {
        let process = match &message {
            Message::Keepalive(keepalive) => {
                self.set_last_keepalive(keepalive.clone());
                true
            }
            Message::Publish(_)
            | Message::AscPullAck(_)
            | Message::AscPullReq(_)
            | Message::ConfirmAck(_)
            | Message::ConfirmReq(_)
            | Message::FrontierReq(_)
            | Message::TelemetryAck(_) => true,
            _ => false,
        };

        if process {
            self.queue_realtime(message)
        } else {
            // TODO: Ban the peer, instead of continuing?
            ReceiveResult::Continue
        }
    }

    fn to_realtime_connection(&self, node_id: &NodeId) -> bool {
        if self.channel.mode() != ChannelMode::Undefined {
            return false;
        }

        let Some(network) = self.network.upgrade() else {
            return false;
        };

        let result = network
            .read()
            .unwrap()
            .upgrade_to_realtime_connection(self.channel.channel_id(), *node_id);

        if let Some((channel, observers)) = result {
            for observer in observers {
                observer(channel.clone());
            }

            self.stats
                .inc(StatType::TcpChannels, DetailType::ChannelAccepted);

            debug!(
                "Switched to realtime mode (addr: {}, node_id: {})",
                self.channel.peer_addr(),
                node_id
            );
            true
        } else {
            debug!(
                channel_id = ?self.channel.channel_id(),
                peer = %self.channel.peer_addr(),
                %node_id,
                "Could not upgrade channel to realtime connection, because another channel for the same node ID was found",
            );
            false
        }
    }

    fn process_message(&mut self, message: Message) -> ReceiveResult {
        self.stats.inc_dir(
            StatType::TcpServer,
            DetailType::from(message.message_type()),
            Direction::In,
        );

        /*
         * Server initially starts in undefined state, where it waits for either a handshake or booststrap request message
         * If the server receives a handshake (and it is successfully validated) it will switch to a realtime mode.
         * In realtime mode messages are deserialized and queued to `tcp_message_manager` for further processing.
         * In realtime mode any bootstrap requests are ignored.
         *
         * If the server receives a bootstrap request before receiving a handshake, it will switch to a bootstrap mode.
         * In bootstrap mode once a valid bootstrap request message is received, the server will start a corresponding bootstrap server and pass control to that server.
         * Once that server finishes its task, control is passed back to this server to read and process any subsequent messages.
         * In bootstrap mode any realtime messages are ignored
         */
        if self.channel.mode() == ChannelMode::Undefined {
            let (mut status, response) = match &message {
                Message::NodeIdHandshake(payload) => {
                    self.handshake_stats
                        .handshakes_received
                        .fetch_add(1, Ordering::Relaxed);

                    match self
                        .handshake_process
                        .process_handshake(payload, self.channel.peer_addr())
                    {
                        Ok((their_node_id, response)) => {
                            self.handshake_stats
                                .response_ok
                                .fetch_add(1, Ordering::Relaxed);

                            match their_node_id {
                                Some(node_id) => (HandshakeStatus::Realtime(node_id), response),
                                None => (HandshakeStatus::Handshake, response),
                            }
                        }
                        Err(e) => {
                            self.handshake_stats.errors[e as usize].fetch_add(1, Ordering::Relaxed);
                            self.handshake_stats
                                .handshake_error
                                .fetch_add(1, Ordering::Relaxed);
                            debug!(
                                peer = %self.channel.peer_addr(),
                                error = ?e,
                                "Invalid handshake response received"
                            );
                            (HandshakeStatus::Abort, None)
                        }
                    }
                }

                _ => (HandshakeStatus::Abort, None),
            };

            if let Some(response) = response {
                debug!("Responding to handshake ({})", self.channel.peer_addr());
                let buffer = self
                    .serializer
                    .serialize(&Message::NodeIdHandshake(response));

                let enqueued = self.channel.send(buffer, TrafficType::Generic);
                if enqueued {
                    self.handshake_stats
                        .handshakes_sent
                        .fetch_add(1, Ordering::Relaxed);
                    self.handshake_stats
                        .response_sent
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.handshake_stats
                        .network_error
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(peer = %self.channel.peer_addr(), "Error sending handshake response");
                    status = HandshakeStatus::Abort;
                }
            }

            match status {
                HandshakeStatus::Abort | HandshakeStatus::AbortOwnNodeId => {
                    self.stats.inc_dir(
                        StatType::TcpServer,
                        DetailType::HandshakeAbort,
                        Direction::In,
                    );
                    debug!(
                        "Aborting handshake: {:?} ({})",
                        message.message_type(),
                        self.channel.peer_addr()
                    );
                    if matches!(status, HandshakeStatus::AbortOwnNodeId) {
                        if let Some(peering_addr) = self.channel.peering_addr() {
                            if let Some(network) = self.network.upgrade() {
                                network.write().unwrap().perma_ban(peering_addr);
                            }
                        }
                    }
                    return ReceiveResult::Abort;
                }
                HandshakeStatus::Handshake => {
                    return ReceiveResult::Continue; // Continue handshake
                }
                HandshakeStatus::Realtime(node_id) => {
                    self.node_id = node_id;
                    // Wait until send queue is empty for the handshake to complete
                    return ReceiveResult::Pause;
                }
            }
        } else if self.channel.mode() == ChannelMode::Realtime {
            return self.process_realtime(message);
        }

        debug_assert!(false);
        ReceiveResult::Abort
    }
}

impl DataReceiver for NanoDataReceiver {
    fn receive(&mut self, data: &[u8]) -> ReceiveResult {
        self.message_deserializer.push(data);
        while let Some(result) = self.message_deserializer.try_deserialize() {
            let result = match result {
                Ok(msg) => {
                    if self.first_message {
                        // TODO: if version using changes => peer misbehaved!
                        self.channel
                            .set_protocol_version(msg.protocol.version_using);
                        self.first_message = false;
                    }
                    self.process_message(msg.message)
                }
                Err(ParseMessageError::DuplicatePublishMessage) => {
                    // Avoid too much noise about `duplicate_publish_message` errors
                    self.stats.inc_dir(
                        StatType::Filter,
                        DetailType::DuplicatePublishMessage,
                        Direction::In,
                    );
                    ReceiveResult::Continue
                }
                Err(ParseMessageError::DuplicateConfirmAckMessage) => {
                    self.stats.inc_dir(
                        StatType::Filter,
                        DetailType::DuplicateConfirmAckMessage,
                        Direction::In,
                    );
                    ReceiveResult::Continue
                }
                Err(e) => {
                    // IO error or critical error when deserializing message
                    self.stats
                        .inc_dir(StatType::Error, DetailType::from(&e), Direction::In);
                    debug!(
                        "Error reading message: {:?} ({})",
                        e,
                        self.channel.peer_addr()
                    );
                    ReceiveResult::Abort
                }
            };

            if !matches!(result, ReceiveResult::Continue) {
                return result;
            }
        }

        ReceiveResult::Continue
    }

    fn try_unpause(&self) -> ReceiveResult {
        let mode = self.channel.mode();
        match mode {
            ChannelMode::Undefined => {
                // Paused during handshake

                // Wait until all outbound messages are processed.
                // This is needed for the handshake because the channel can't be upgraded to
                // a realtime channel unless the handshake response is actually sent out
                if self.channel.queue_len() > 0 {
                    return ReceiveResult::Pause;
                }

                if !self.to_realtime_connection(&self.node_id) {
                    self.stats.inc_dir(
                        StatType::TcpServer,
                        DetailType::HandshakeError,
                        Direction::In,
                    );
                    debug!(
                        "Error switching to realtime mode ({})",
                        self.channel.peer_addr()
                    );
                    return ReceiveResult::Abort;
                }

                ReceiveResult::Continue
            }
            ChannelMode::Realtime => {
                let message = self.retry_enqueue.clone();
                match message {
                    Some(message) => {
                        if self.try_enqueue(message) {
                            ReceiveResult::Continue
                        } else {
                            ReceiveResult::Pause
                        }
                    }
                    None => ReceiveResult::Continue,
                }
            }
        }
    }
}

impl Drop for NanoDataReceiver {
    fn drop(&mut self) {
        self.channel.close();
    }
}
