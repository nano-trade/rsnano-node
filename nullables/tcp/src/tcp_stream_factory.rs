use super::tcp_stream::TcpStream;

pub struct TcpStreamFactory {
    factory: FactoryType,
}

impl TcpStreamFactory {
    pub fn new() -> Self {
        Self {
            factory: FactoryType::Tokio,
        }
    }

    pub fn new_null() -> Self {
        Self {
            factory: FactoryType::Null,
        }
    }

    pub async fn connect<A: tokio::net::ToSocketAddrs>(
        &self,
        addr: A,
    ) -> tokio::io::Result<TcpStream> {
        match &self.factory {
            FactoryType::Tokio => {
                let tokio_stream = tokio::net::TcpStream::connect(addr).await?;
                Ok(TcpStream::new(tokio_stream))
            }
            FactoryType::Null => Err(tokio::io::Error::new(
                std::io::ErrorKind::Other,
                "nulled TcpStreamFactory has no configured connections",
            )),
        }
    }
}

impl Default for TcpStreamFactory {
    fn default() -> Self {
        Self::new()
    }
}

enum FactoryType {
    Tokio,
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[tokio::test]
    async fn can_be_nulled() {
        let factory = TcpStreamFactory::new_null();
        match factory.connect("127.0.0.1:42").await {
            Ok(_) => panic!("connect should fail"),
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::Other);
                assert_eq!(
                    e.to_string(),
                    "nulled TcpStreamFactory has no configured connections"
                );
            }
        }
    }
}
