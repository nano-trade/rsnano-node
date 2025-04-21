use crate::TrafficType;
use rsnano_stats::{Direction, StatsCollection, StatsSource};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use strum::{EnumCount, IntoEnumIterator};

#[derive(Default)]
pub(crate) struct ChannelStats {
    pub timed_out: AtomicUsize,
    pub read_succeeded: AtomicUsize,
    pub read_failed: AtomicUsize,
    pub write_failed: AtomicUsize,
    pub write_succeeded: AtomicUsize,
    pub sent_by_type: [AtomicUsize; TrafficType::COUNT],
}

impl StatsSource for ChannelStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("tcp", "tcp_io_timeout_drop", self.timed_out.load(Relaxed));
        result.insert_dir(
            "traffic_tcp",
            "all",
            Direction::In,
            self.read_succeeded.load(Relaxed),
        );
        result.insert_dir(
            "traffic_tcp",
            "all",
            Direction::Out,
            self.write_succeeded.load(Relaxed),
        );
        result.insert_dir(
            "tcp",
            "tcp_read_error",
            Direction::In,
            self.read_failed.load(Relaxed),
        );
        result.insert_dir(
            "tcp",
            "tcp_write_error",
            Direction::Out,
            self.write_failed.load(Relaxed),
        );

        for i in TrafficType::iter() {
            result.insert_dir(
                "traffic_tcp_type",
                i.into(),
                Direction::Out,
                self.sent_by_type[i as usize].load(Relaxed),
            );
        }
    }
}
