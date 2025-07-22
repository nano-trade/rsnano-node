use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::Receiver,
        Mutex,
    },
    time::{Duration, Instant},
};

use tracing::info;

use rsnano_core::BlockHash;
use rsnano_websocket_messages::{BlockConfirmed, MessageEnvelope, Topic};

use crate::{
    domain::{BlockFactory, DelayedBlocks},
    high_prio_check::HighPrioTracker,
};

pub(crate) fn track_confirmations(
    rx_ws_msg: Receiver<(MessageEnvelope, Instant)>,
    delayed_blocks: &Mutex<DelayedBlocks>,
    block_factory: &Mutex<BlockFactory>,
    ws_queue_len: &AtomicUsize,
    sum_conf_time_total: &mut Duration,
    current_bps: &AtomicUsize,
    should_track: bool,
    high_prio_tracker: &Mutex<HighPrioTracker>,
) {
    let mut total = 0;
    let mut confirmed = 0;
    let mut start = Instant::now();
    let mut sum_conf_time = Duration::ZERO;
    while let Ok((msg, timestamp)) = rx_ws_msg.recv() {
        let len = ws_queue_len.fetch_sub(1, Ordering::Relaxed);
        if msg.topic == Some(Topic::Confirmation) {
            let data: BlockConfirmed = serde_json::from_value(msg.message.unwrap()).unwrap();
            let block_hash = BlockHash::decode_hex(data.hash).unwrap();

            if should_track {
                let conf_time = delayed_blocks
                    .lock()
                    .unwrap()
                    .confirmed(&block_hash, timestamp);

                if let Some(conf_time) = conf_time {
                    confirmed += 1;
                    total += 1;
                    sum_conf_time += conf_time;
                    *sum_conf_time_total += conf_time;
                }
                block_factory.lock().unwrap().confirm(block_hash);
                if confirmed > 0 && confirmed % 5000 == 0 {
                    let cps = (confirmed as f64 / start.elapsed().as_secs_f64()) as i32;
                    let avg_conf_time = sum_conf_time.as_millis() / confirmed;
                    let bps = current_bps.load(Ordering::Relaxed);
                    info!(
                    "Confirmed {confirmed} blocks ({total} total) | {bps} bps | {cps} cps | avg conf time: {avg_conf_time} ms | ws queue: {len}"
                );
                    confirmed = 0;
                    start = Instant::now();
                    sum_conf_time = Duration::ZERO;
                }
            }

            high_prio_tracker.lock().unwrap().confirmed(block_hash);
        }
    }
}
