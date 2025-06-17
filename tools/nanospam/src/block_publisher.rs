use rsnano_core::{Block, ProtocolInfo};
use rsnano_messages::{Message, MessageSerializer, Publish};
use rsnano_nullable_tcp::TcpStream;
use tokio::io::{AsyncWriteExt, WriteHalf};

pub(crate) struct BlockPublisher {
    serializer: MessageSerializer,
    tcp_write: WriteHalf<TcpStream>,
}

impl BlockPublisher {
    pub fn new(protocol: ProtocolInfo, tcp_write: WriteHalf<TcpStream>) -> Self {
        Self {
            serializer: MessageSerializer::new(protocol),
            tcp_write,
        }
    }
    pub async fn publish(&mut self, block: Block) -> Result<(), std::io::Error> {
        let publish = Message::Publish(Publish::new_from_originator(block));
        let buffer = self.serializer.serialize(&publish);
        self.tcp_write.write(&buffer).await?;
        Ok(())
    }
}
