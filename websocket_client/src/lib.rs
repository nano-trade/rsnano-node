use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rsnano_websocket_messages::MessageEnvelope;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Utf8Bytes;

pub type WsError = tokio_tungstenite::tungstenite::Error;
pub type Message = tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
pub enum Error {
    WebSocketErr(WsError),
    InvalidMessage,
    InvalidJson(serde_json::Error),
}

impl From<WsError> for Error {
    fn from(value: WsError) -> Self {
        Self::WebSocketErr(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::InvalidJson(value)
    }
}

#[derive(Default)]
pub struct NanoWebSocketClientFactory {
    stream_factory: WebSocketStreamFactory,
}

impl NanoWebSocketClientFactory {
    pub async fn connect(&self, endpoint: &str) -> Result<NanoWebSocketClient, WsError> {
        self.stream_factory
            .connect(endpoint)
            .await
            .map(NanoWebSocketClient::new)
    }
}

pub struct NanoWebSocketClient {
    stream: WebSocketStream,
}

impl NanoWebSocketClient {
    pub fn new(stream: WebSocketStream) -> Self {
        Self { stream }
    }

    pub async fn send_raw(&mut self, message: impl Into<Utf8Bytes>) -> Result<(), WsError> {
        self.stream.send(Message::Text(message.into())).await
    }

    pub async fn next(&mut self) -> Option<Result<MessageEnvelope, Error>> {
        self.stream
            .next()
            .await?
            .map_err(Error::from)
            .and_then(parse_message)
            .into()
    }
}

fn parse_message(msg: Message) -> Result<MessageEnvelope, Error> {
    let text = msg.to_text()?;
    let envelope: MessageEnvelope = serde_json::from_str(text)?;
    Ok(envelope)
}

#[derive(Default)]
pub struct WebSocketStreamFactory {}

impl WebSocketStreamFactory {
    pub async fn connect(&self, endpoint: &str) -> Result<WebSocketStream, WsError> {
        let stream = tokio_tungstenite::connect_async(endpoint).await?.0;
        Ok(WebSocketStream { stream })
    }
}

pub struct WebSocketStream {
    stream: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
}

impl Sink<Message> for WebSocketStream {
    type Error = WsError;

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
    type Item = Result<Message, WsError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unsafe { self.map_unchecked_mut(|i| &mut i.stream) }.poll_next(cx)
    }
}
