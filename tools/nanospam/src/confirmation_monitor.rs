use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::Sender,
    },
    time::Instant,
};

use anyhow::anyhow;
use tokio::select;
use tokio_util::sync::CancellationToken;

use rsnano_websocket_client::{
    NanoWebSocketClient, NanoWebSocketClientFactory, SubscribeArgs, TopicSub,
};
use rsnano_websocket_messages::MessageEnvelope;

use crate::setup::websocket_port;

pub(crate) struct ConfirmationMonitor {
    ws_client: NanoWebSocketClient,
}

impl ConfirmationMonitor {
    pub async fn connect() -> anyhow::Result<Self> {
        let mut ws_client = NanoWebSocketClientFactory::default()
            .connect(&format!("ws://[::1]:{}", websocket_port(0)))
            .await?;

        ws_client
            .subscribe(SubscribeArgs {
                topic: TopicSub::Confirmation(Default::default()),
                ack: true,
                id: None,
            })
            .await?;

        // wait for ack
        ws_client
            .next()
            .await
            .ok_or_else(|| anyhow!("no ws response received"))??;

        Ok(Self { ws_client })
    }

    pub fn disabled() -> Self {
        Self {
            ws_client: NanoWebSocketClient::new_null(),
        }
    }

    pub async fn run(
        &mut self,
        cancel_token: CancellationToken,
        ws_queue_len: &AtomicUsize,
        tx_ws_msg: Sender<(MessageEnvelope, Instant)>,
    ) {
        loop {
            let res = select! {
                res = self.ws_client.next() =>  res,
                _ = cancel_token.cancelled() =>{ break;}
            };

            let msg = res.unwrap().unwrap();
            tx_ws_msg.send((msg, Instant::now())).unwrap();
            ws_queue_len.fetch_add(1, Ordering::Relaxed);
        }
    }
}
