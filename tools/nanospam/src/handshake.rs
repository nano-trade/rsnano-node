use std::{
    net::{SocketAddr, SocketAddrV6},
    sync::Arc,
};

use anyhow::bail;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use rsnano_core::{BlockHash, PrivateKey, ProtocolInfo};
use rsnano_messages::{Message, MessageDeserializer, MessageSerializer};
use rsnano_network_protocol::{HandshakeProcess, SynCookies};
use rsnano_nullable_tcp::TcpStream;

pub(crate) async fn perform_handshake(
    protocol: ProtocolInfo,
    genesis_hash: BlockHash,
    node_id_key: PrivateKey,
    tcp_stream: &mut TcpStream,
) -> anyhow::Result<()> {
    let peer_addr = match tcp_stream.peer_addr()? {
        SocketAddr::V4(v4) => SocketAddrV6::new(v4.ip().to_ipv6_mapped(), v4.port(), 0, 0),
        SocketAddr::V6(v6) => v6,
    };
    let mut serializer = MessageSerializer::new(protocol);
    let mut deserializer = MessageDeserializer::new(protocol);

    let syn_cookies = Arc::new(SynCookies::default());
    let mut handshake = HandshakeProcess::new(genesis_hash, node_id_key, syn_cookies);

    let handshake_payload = handshake.initiate_handshake(peer_addr)?;
    let buffer = serializer.serialize(&Message::NodeIdHandshake(handshake_payload));
    tcp_stream.write_all(buffer).await?;

    let mut recv_buffer = vec![0; 1024];
    let response;
    loop {
        let size = tcp_stream.read(&mut recv_buffer).await?;
        deserializer.push(&recv_buffer[..size]);
        if let Some(msg) = deserializer.try_deserialize() {
            response = msg.unwrap().message;
            break;
        }
    }

    let Message::NodeIdHandshake(handshake_response) = response else {
        bail!("no handshake response received");
    };

    match handshake
        .process_handshake(&handshake_response, peer_addr)
        .unwrap()
    {
        (Some(_node_id), Some(response)) => {
            let buffer = serializer.serialize(&Message::NodeIdHandshake(response));
            tcp_stream.write(buffer).await?;
        }
        _ => unreachable!(),
    }
    Ok(())
}
