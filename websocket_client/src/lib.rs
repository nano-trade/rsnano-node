use futures_util::{Sink, Stream};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::TcpStream;

pub type Error = tokio_tungstenite::tungstenite::Error;
pub type Message = tokio_tungstenite::tungstenite::Message;

pub struct NanoWebsocketClient {}

#[derive(Default)]
pub struct WebSocketClientFactory {}

impl WebSocketClientFactory {
    pub async fn connect(&self, endpoint: &str) -> Result<WebSocketStream, Error> {
        let stream = tokio_tungstenite::connect_async(endpoint).await?.0;
        Ok(WebSocketStream { stream })
    }
}

pub struct WebSocketStream {
    stream: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
}

impl Sink<Message> for WebSocketStream {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unsafe { self.map_unchecked_mut(|i| &mut i.stream) }.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        unsafe { self.map_unchecked_mut(|i| &mut i.stream) }.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unsafe { self.map_unchecked_mut(|i| &mut i.stream) }.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unsafe { self.map_unchecked_mut(|i| &mut i.stream) }.poll_close(cx)
    }
}

impl Stream for WebSocketStream {
    type Item = Result<Message, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unsafe { self.map_unchecked_mut(|i| &mut i.stream) }.poll_next(cx)
    }
}
