use rsnano_core::{Networks, ProtocolInfo};
use rsnano_messages::{Message, MessageSerializer, NodeIdHandshake, NodeIdHandshakeQuery};
use std::io::ErrorKind;
use tokio::{io::AsyncWriteExt, net::TcpSocket};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let socket = TcpSocket::new_v6()?;
    let mut stream = socket.connect("[::1]:17075".parse()?).await?;
    println!("Connected!");

    let mut serializer =
        MessageSerializer::new(ProtocolInfo::default_for(Networks::NanoTestNetwork));

    let handshake = Message::NodeIdHandshake(NodeIdHandshake {
        query: Some(NodeIdHandshakeQuery { cookie: [1; 32] }),
        response: None,
        is_v2: true,
    });
    let bytes = serializer.serialize(&handshake);
    stream.write(bytes).await?;

    let mut recv_buffer = [0; 1024];
    loop {
        stream.readable().await.unwrap();
        let size = match stream.try_read(&mut recv_buffer) {
            Ok(0) => panic!("closed"),
            Ok(n) => n,
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    continue;
                }
                panic!("error: {:?}", e);
            }
        };
        println!("recv: {:02X?}", &recv_buffer[..size]);
    }
}
