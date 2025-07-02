use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, RwLock},
};

use rsnano_messages::{Message, MessageSerializer};
use rsnano_network::{Channel, ChannelDirection, Network, TrafficType};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use rsnano_stats::Stats;

use super::{try_send_serialized_message, MessageSender};
use crate::representatives::OnlineReps;
use rsnano_core::utils::{TEST_ENDPOINT_1, TEST_ENDPOINT_2};
use rsnano_nullable_clock::Timestamp;

/// Floods messages to PRs and non PRs
pub struct MessageFlooder {
    // TODO make private again
    pub online_reps: Arc<Mutex<OnlineReps>>,
    network: Arc<RwLock<Network>>,
    stats: Arc<Stats>,
    message_serializer: MessageSerializer,
    sender: MessageSender,
    flood_listener: OutputListenerMt<FloodEvent>,
}

impl MessageFlooder {
    pub fn new(
        online_reps: Arc<Mutex<OnlineReps>>,
        network: Arc<RwLock<Network>>,
        stats: Arc<Stats>,
        sender: MessageSender,
    ) -> Self {
        Self {
            online_reps,
            network,
            stats,
            message_serializer: sender.get_serializer(),
            sender,
            flood_listener: OutputListenerMt::new(),
        }
    }

    pub(crate) fn new_null() -> Self {
        let mut network = Network::new_test_instance();
        // add a channel so that capacity checks succeed
        let (channel, _) = network
            .add(
                TEST_ENDPOINT_1,
                TEST_ENDPOINT_2,
                ChannelDirection::Outbound,
                Timestamp::new_test_instance(),
            )
            .unwrap();
        channel.set_mode(rsnano_network::ChannelMode::Realtime);

        Self::new(
            Arc::new(Mutex::new(OnlineReps::default())),
            Arc::new(RwLock::new(network)),
            Arc::new(Stats::default()),
            MessageSender::new_null(),
        )
    }

    pub(crate) fn flood_prs_and_some_non_prs(
        &mut self,
        message: &Message,
        traffic_type: TrafficType,
        scale: f32,
    ) -> FloodCount {
        let mut flood_count = FloodCount::default();
        let peered_prs = self.online_reps.lock().unwrap().peered_principal_reps();
        for rep in peered_prs {
            if self.sender.try_send(&rep.channel, &message, traffic_type) {
                flood_count.principal_reps += 1;
            }
        }

        let mut channels;
        let fanout;
        {
            let network = self.network.read().unwrap();
            fanout = network.fanout(scale);
            channels = network.shuffled_channels(traffic_type)
        }

        self.remove_principal_reps(&mut channels, fanout);
        for peer in channels {
            if self.sender.try_send(&peer, &message, traffic_type) {
                flood_count.non_principal_reps += 1;
            }
        }

        flood_count
    }

    fn remove_principal_reps(&self, channels: &mut Vec<Arc<Channel>>, count: usize) {
        {
            let reps = self.online_reps.lock().unwrap();
            channels.retain(|c| !reps.is_principal_rep(c.channel_id()));
        }
        channels.truncate(count);
    }

    pub fn flood(&mut self, message: &Message, traffic_type: TrafficType, scale: f32) -> usize {
        if self.flood_listener.is_tracked() {
            self.flood_listener.emit(FloodEvent {
                message: message.clone(),
                traffic_type,
                scale,
            });
        }

        let buffer = self.message_serializer.serialize(message);
        let network = self.network.read().unwrap();
        let channels = Self::random_fanout(&network, traffic_type, scale);
        let mut sent = 0;

        for channel in channels {
            if try_send_serialized_message(&channel, &self.stats, buffer, message, traffic_type) {
                sent += 1;
            }
        }
        sent
    }

    pub fn track_floods(&self) -> Arc<OutputTrackerMt<FloodEvent>> {
        self.flood_listener.track()
    }

    fn random_fanout(
        network: &Network,
        traffic_type: TrafficType,
        scale: f32,
    ) -> Vec<Arc<Channel>> {
        let mut channels = network.shuffled_channels(traffic_type);
        channels.truncate(network.fanout(scale));
        channels
    }

    pub fn check_capacity(&self, traffic_type: TrafficType, scale: f32) -> bool {
        self.network
            .read()
            .unwrap()
            .check_capacity(traffic_type, scale)
    }
}

impl Clone for MessageFlooder {
    fn clone(&self) -> Self {
        Self {
            online_reps: self.online_reps.clone(),
            network: self.network.clone(),
            stats: self.stats.clone(),
            message_serializer: self.message_serializer.clone(),
            sender: self.sender.clone(),
            flood_listener: OutputListenerMt::new(),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, PartialEq, Debug)]
pub struct FloodEvent {
    pub message: Message,
    pub traffic_type: TrafficType,
    pub scale: f32,
}

impl Deref for MessageFlooder {
    type Target = MessageSender;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl DerefMut for MessageFlooder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

#[derive(Default)]
pub struct FloodCount {
    pub principal_reps: usize,
    pub non_principal_reps: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_track_floods() {
        let mut flooder = MessageFlooder::new_null();
        let tracker = flooder.track_floods();
        let message = Message::BulkPush;
        let traffic_type = TrafficType::Vote;
        let scale = 0.5;
        flooder.flood(&message, traffic_type, scale);

        let floods = tracker.output();
        assert_eq!(
            floods,
            vec![FloodEvent {
                message,
                traffic_type,
                scale
            }]
        );
    }
}
