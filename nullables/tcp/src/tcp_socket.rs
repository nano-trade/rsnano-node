use crate::TcpStream;
use std::{io, net::SocketAddr};

pub struct TcpSocket {
    strategy: Strategy,
}

impl TcpSocket {
    pub fn new_v6() -> io::Result<Self> {
        Ok(Self {
            strategy: Strategy::Real(tokio::net::TcpSocket::new_v6()?),
        })
    }

    pub async fn connect(self, addr: SocketAddr) -> io::Result<TcpStream> {
        let stream = match self.strategy {
            Strategy::Real(socket) => socket.connect(addr).await?,
        };
        Ok(TcpStream::new(stream))
    }
}

enum Strategy {
    Real(tokio::net::TcpSocket),
}
