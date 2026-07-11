use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::protocol::{egress_subprotocols, PROTOCOL_VERSION, SUBPROTOCOL_MAGIC_COOKIE};
use crate::relay::{BoxTransportError, DatagramTransport};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub const DEFAULT_EGRESS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum EgressError {
    #[error("invalid egress WebSocket request: {0}")]
    Request(#[source] tungstenite::Error),
    #[error("invalid egress WebSocket subprotocol header: {0}")]
    Header(#[from] tungstenite::http::header::InvalidHeaderValue),
    #[error("egress WebSocket failed: {0}")]
    WebSocket(#[from] tungstenite::Error),
    #[error("timed out after {0:?} connecting to the egress WebSocket")]
    ConnectTimeout(Duration),
    #[error("egress selected no WebSocket subprotocol")]
    MissingSubprotocol,
    #[error("egress selected unexpected WebSocket subprotocol {0:?}")]
    UnexpectedSubprotocol(String),
    #[error("egress sent a text message on the binary packet tunnel")]
    TextMessage,
}

#[derive(Debug)]
pub struct EgressTunnel {
    socket: Socket,
}

impl EgressTunnel {
    pub async fn connect(url: &str, csid: &str) -> Result<Self, EgressError> {
        Self::connect_with_timeout(url, csid, DEFAULT_EGRESS_CONNECT_TIMEOUT).await
    }

    async fn connect_with_timeout(
        url: &str,
        csid: &str,
        connect_timeout: Duration,
    ) -> Result<Self, EgressError> {
        let mut request = url.into_client_request().map_err(EgressError::Request)?;
        let protocols = egress_subprotocols(csid, PROTOCOL_VERSION).join(", ");
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_str(&protocols)?);

        let (socket, response) =
            tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(request))
                .await
                .map_err(|_| EgressError::ConnectTimeout(connect_timeout))??;
        let selected = response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .ok_or(EgressError::MissingSubprotocol)?
            .to_str()
            .map_err(|_| EgressError::UnexpectedSubprotocol("non-ASCII".into()))?;
        if selected != SUBPROTOCOL_MAGIC_COOKIE {
            return Err(EgressError::UnexpectedSubprotocol(selected.into()));
        }
        Ok(Self { socket })
    }

    pub async fn send(&mut self, packet: Bytes) -> Result<(), EgressError> {
        self.socket.send(Message::Binary(packet)).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Option<Bytes>, EgressError> {
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(None);
            };
            match message? {
                Message::Binary(packet) => return Ok(Some(packet)),
                Message::Ping(_) => {}
                Message::Pong(_) => {}
                Message::Close(_) => return Ok(None),
                Message::Text(_) => return Err(EgressError::TextMessage),
                Message::Frame(_) => {}
            }
        }
    }

    pub async fn close(mut self) -> Result<(), EgressError> {
        self.socket.close(None).await?;
        Ok(())
    }
}

#[async_trait]
impl DatagramTransport for EgressTunnel {
    async fn send_packet(&mut self, packet: Bytes) -> Result<(), BoxTransportError> {
        self.send(packet)
            .await
            .map_err(|error| Box::new(error) as BoxTransportError)
    }

    async fn recv_packet(&mut self) -> Result<Option<Bytes>, BoxTransportError> {
        self.recv()
            .await
            .map_err(|error| Box::new(error) as BoxTransportError)
    }
}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::oneshot;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    use super::*;

    #[allow(clippy::result_large_err)]
    async fn spawn_egress_stub() -> (String, oneshot::Receiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (protocols_tx, protocols_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut protocols_tx = Some(protocols_tx);
            let mut socket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &Request, mut response: Response| {
                    let protocols = request.headers()[SEC_WEBSOCKET_PROTOCOL]
                        .to_str()
                        .unwrap()
                        .to_owned();
                    if let Some(tx) = protocols_tx.take() {
                        let _ = tx.send(protocols);
                    }
                    response.headers_mut().insert(
                        SEC_WEBSOCKET_PROTOCOL,
                        HeaderValue::from_static(SUBPROTOCOL_MAGIC_COOKIE),
                    );
                    Ok(response)
                },
            )
            .await
            .unwrap();

            let message = socket.next().await.unwrap().unwrap();
            socket.send(message).await.unwrap();
            socket.close(None).await.unwrap();
        });
        (format!("ws://{addr}/ws"), protocols_rx)
    }

    #[tokio::test]
    async fn sends_csid_subprotocols_and_relays_binary_packets() {
        let (url, protocols) = spawn_egress_stub().await;
        let mut tunnel = EgressTunnel::connect(&url, "consumer-session-id")
            .await
            .unwrap();
        assert_eq!(
            protocols.await.unwrap(),
            "un80und3d, consumer-session-id, v2.3.0"
        );

        tunnel
            .send(Bytes::from_static(b"opaque QUIC packet"))
            .await
            .unwrap();
        assert_eq!(
            tunnel.recv().await.unwrap(),
            Some(Bytes::from_static(b"opaque QUIC packet"))
        );
        assert_eq!(tunnel.recv().await.unwrap(), None);
    }

    #[tokio::test]
    async fn times_out_a_stalled_egress_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let stalled_server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });

        let timeout = Duration::from_millis(20);
        let error = EgressTunnel::connect_with_timeout(&url, "consumer-session-id", timeout)
            .await
            .unwrap_err();
        assert!(matches!(error, EgressError::ConnectTimeout(value) if value == timeout));
        stalled_server.abort();
    }
}
