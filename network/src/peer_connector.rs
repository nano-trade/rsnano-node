use crate::{ChannelDirection, NetworkError, TcpNetworkAdapter};
use rsnano_nullable_tcp::{TcpSocket, TcpStream};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use std::{net::SocketAddrV6, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Establishes a network connection to a given peer
pub struct PeerConnector {
    connect_timeout: Duration,
    network_adapter: Arc<TcpNetworkAdapter>,
    tokio: tokio::runtime::Handle,
    cancel_token: CancellationToken,
    connect_listener: OutputListenerMt<SocketAddrV6>,
}

impl PeerConnector {
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    pub fn new(
        connect_timeout: Duration,
        network_adapter: Arc<TcpNetworkAdapter>,
        tokio: tokio::runtime::Handle,
    ) -> Self {
        Self {
            connect_timeout,
            network_adapter,
            tokio,
            cancel_token: CancellationToken::new(),
            connect_listener: OutputListenerMt::new(),
        }
    }

    pub fn new_null(tokio: tokio::runtime::Handle) -> Self {
        Self {
            connect_timeout: Self::DEFAULT_TIMEOUT,
            network_adapter: Arc::new(TcpNetworkAdapter::new_null(tokio.clone())),
            tokio: tokio.clone(),
            cancel_token: CancellationToken::new(),
            connect_listener: OutputListenerMt::new(),
        }
    }

    pub fn track_connections(&self) -> Arc<OutputTrackerMt<SocketAddrV6>> {
        self.connect_listener.track()
    }

    /// Establish a network connection to the given peer
    pub fn connect_to(&self, peer: SocketAddrV6) -> Result<(), NetworkError> {
        self.connect_listener.emit(peer);

        if self.cancel_token.is_cancelled() {
            return Err(NetworkError::Cancelled);
        }

        self.network_adapter.add_outbound_attempt(peer)?;

        let network_l = self.network_adapter.clone();
        let connect_timeout = self.connect_timeout;
        let cancel_token = self.cancel_token.clone();

        self.tokio.spawn(async move {
            tokio::select! {
                _ =  connect_impl(peer, &network_l) => { },
                _ = tokio::time::sleep(connect_timeout) => {
                    network_l.attempt_timeout(peer);
                },
                _ = cancel_token.cancelled() => {
                    debug!("Connection attempt cancelled: {}", peer);
                },
            };

            network_l.remove_attempt(&peer);
        });

        Ok(())
    }

    pub fn stop(&self) {
        self.cancel_token.cancel();
    }
}

async fn connect_impl(peer: SocketAddrV6, network_adapter: &TcpNetworkAdapter) {
    match connect_stream(peer).await {
        Ok(stream) => {
            let _ = network_adapter.add(stream, ChannelDirection::Outbound);
        }
        Err(e) => {
            network_adapter.connect_error(peer, e);
        }
    }
}

async fn connect_stream(peer: SocketAddrV6) -> tokio::io::Result<TcpStream> {
    TcpSocket::new_v6()?.connect(peer.into()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::utils::TEST_ENDPOINT_1;

    #[tokio::test]
    async fn track_connections() {
        let peer_connector = Arc::new(PeerConnector::new_null(tokio::runtime::Handle::current()));
        let connect_tracker = peer_connector.track_connections();

        peer_connector.connect_to(TEST_ENDPOINT_1).unwrap();

        assert_eq!(connect_tracker.output(), vec![TEST_ENDPOINT_1]);
    }
}
