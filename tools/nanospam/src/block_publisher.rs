use rsnano_core::{Block, ProtocolInfo};
use rsnano_messages::{Message, MessageSerializer, Publish};
use rsnano_nullable_tcp::TcpStream;
use tokio::io::AsyncWriteExt;

pub(crate) struct BlockPublisher {
    serializer: MessageSerializer,
    tcp_stream: TcpStream,
}

impl BlockPublisher {
    pub fn new(protocol: ProtocolInfo, tcp_stream: TcpStream) -> Self {
        Self {
            serializer: MessageSerializer::new(protocol),
            tcp_stream,
        }
    }
    pub async fn publish(&mut self, block: Block) -> Result<(), std::io::Error> {
        let publish = Message::Publish(Publish::new_from_originator(block));
        let buffer = self.serializer.serialize(&publish);
        self.tcp_stream.write(&buffer).await?;
        Ok(())
    }
}
