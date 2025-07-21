#[macro_use]
extern crate strum_macros;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rsnano_core::Account;
use rsnano_websocket_messages::{ConfirmationJsonOptions, MessageEnvelope, Request, Topic};
use std::{
    collections::HashSet,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Utf8Bytes;

pub type WsError = tokio_tungstenite::tungstenite::Error;
pub type Message = tokio_tungstenite::tungstenite::Message;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Websocket error: {0}")]
    WebSocketErr(WsError),
    #[error("Invalid message")]
    InvalidMessage,
    #[error("Invalid JSON: {0}")]
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

    pub fn new_null() -> Self {
        Self {
            stream: WebSocketStream::new_null(),
        }
    }

    pub async fn subscribe(&mut self, args: SubscribeArgs<'_>) -> Result<(), Error> {
        let request = Request {
            action: Some("subscribe"),
            topic: Some(Topic::from(&args.topic).into()),
            ack: args.ack,
            id: args.id,
            options: get_subscription_options(args.topic),
        };
        self.send_request(&request).await
    }

    pub async fn unsubscribe(&mut self, args: UnsubscribeArgs<'_>) -> Result<(), Error> {
        let request = Request {
            action: Some("unsubscribe"),
            topic: Some(args.topic.into()),
            ack: args.ack,
            id: args.id,
            options: None,
        };
        self.send_request(&request).await
    }

    pub async fn send_request(&mut self, request: &Request<'_>) -> Result<(), Error> {
        let req_str = serde_json::to_string(request)?;
        self.send_text(req_str).await?;
        Ok(())
    }

    pub async fn send_text(&mut self, message: impl Into<Utf8Bytes>) -> Result<(), WsError> {
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

fn get_subscription_options(opts: TopicSub) -> Option<serde_json::Value> {
    match opts {
        TopicSub::Confirmation(args) => {
            let conf_type_str: &str = args.confirmation_types.into();
            let json_opts = ConfirmationJsonOptions {
                include_block: Some(args.include_block),
                include_election_info: Some(args.include_election_info),
                include_election_info_with_votes: Some(args.include_election_info_with_votes),
                include_linked_account: Some(args.include_linked_account),
                include_sideband_info: Some(args.include_sideband_info),
                confirmation_type: Some(conf_type_str.to_string()),
                all_local_accounts: if args.all_local_accounts {
                    Some(true)
                } else {
                    None
                },
                accounts: if args.accounts.is_empty() {
                    None
                } else {
                    Some(args.accounts.iter().map(|a| a.encode_account()).collect())
                },
            };
            serde_json::to_value(json_opts).ok()
        }
        _ => None,
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
        Ok(WebSocketStream {
            stream: WsStreamImpl::Tungstenite(stream),
        })
    }
}

pub struct WebSocketStream {
    stream: WsStreamImpl,
}

impl WebSocketStream {
    pub fn new_null() -> Self {
        Self {
            stream: WsStreamImpl::Stub,
        }
    }
}

enum WsStreamImpl {
    Tungstenite(tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>),
    Stub,
}

impl Sink<Message> for WebSocketStream {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.get_mut().stream {
            WsStreamImpl::Tungstenite(stream) => Pin::new(stream).poll_ready(cx),
            WsStreamImpl::Stub => Poll::Pending,
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        match &mut self.get_mut().stream {
            WsStreamImpl::Tungstenite(stream) => Pin::new(stream).start_send(item),
            WsStreamImpl::Stub => Ok(()),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.get_mut().stream {
            WsStreamImpl::Tungstenite(stream) => Pin::new(stream).poll_flush(cx),
            WsStreamImpl::Stub => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.get_mut().stream {
            WsStreamImpl::Tungstenite(stream) => Pin::new(stream).poll_close(cx),
            WsStreamImpl::Stub => Poll::Ready(Ok(())),
        }
    }
}

impl Stream for WebSocketStream {
    type Item = Result<Message, WsError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().stream {
            WsStreamImpl::Tungstenite(stream) => Pin::new(stream).poll_next(cx),
            WsStreamImpl::Stub => Poll::Pending,
        }
    }
}

pub struct SubscribeArgs<'a> {
    pub topic: TopicSub,
    pub ack: bool,
    pub id: Option<&'a str>,
}

impl<'a> Default for SubscribeArgs<'a> {
    fn default() -> Self {
        Self {
            topic: TopicSub::Confirmation(Default::default()),
            ack: false,
            id: None,
        }
    }
}

pub enum TopicSub {
    /// A confirmation message
    Confirmation(ConfirmationSubArgs),
    StartedElection,
    /// Stopped election message (dropped elections due to bounding or block lost the elections)
    StoppedElection,
    /// A vote message
    Vote,
    /// Work generation message
    Work,
    /// A bootstrap message
    Bootstrap,
    /// A telemetry message
    Telemetry,
    /// New block arrival message
    NewUnconfirmedBlock,
}

impl From<&TopicSub> for Topic {
    fn from(value: &TopicSub) -> Self {
        match value {
            TopicSub::Confirmation(_) => Topic::Confirmation,
            TopicSub::StartedElection => Topic::StartedElection,
            TopicSub::StoppedElection => Topic::StoppedElection,
            TopicSub::Vote => Topic::Vote,
            TopicSub::Work => Topic::Work,
            TopicSub::Bootstrap => Topic::Bootstrap,
            TopicSub::Telemetry => Topic::Telemetry,
            TopicSub::NewUnconfirmedBlock => Topic::NewUnconfirmedBlock,
        }
    }
}

pub struct ConfirmationSubArgs {
    pub include_election_info: bool,
    pub include_election_info_with_votes: bool,
    pub include_linked_account: bool,
    pub include_sideband_info: bool,
    pub include_block: bool,
    pub all_local_accounts: bool,
    pub confirmation_types: ConfirmationTypeFilter,
    pub accounts: HashSet<Account>,
}

impl Default for ConfirmationSubArgs {
    fn default() -> Self {
        Self {
            include_election_info: false,
            include_election_info_with_votes: false,
            include_linked_account: false,
            include_sideband_info: false,
            include_block: true,
            all_local_accounts: false,
            confirmation_types: Default::default(),
            accounts: Default::default(),
        }
    }
}

#[derive(Default, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ConfirmationTypeFilter {
    #[default]
    All,
    Active,
    ActiveQuorum,
    ActiveConfirmationHeight,
    Inactive,
}

pub struct UnsubscribeArgs<'a> {
    pub topic: Topic,
    pub ack: bool,
    pub id: Option<&'a str>,
}

impl<'a> Default for UnsubscribeArgs<'a> {
    fn default() -> Self {
        Self {
            topic: Topic::Confirmation,
            ack: false,
            id: None,
        }
    }
}
