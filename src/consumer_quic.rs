use std::io;
use std::sync::Arc;
use std::time::Duration;

use quinn::{
    Connection, Endpoint, EndpointConfig, RecvStream, SendStream, ServerConfig, TransportConfig,
    VarInt,
};
use tokio::io::Join;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::virtual_udp::VirtualUdpSocket;

pub const CONSUMER_QUIC_ALPN: &[u8] = b"broflake";
pub const CONSUMER_MAX_INCOMING_STREAMS: u32 = 131_072;
pub const CONSUMER_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
pub const CONSUMER_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

pub fn consumer_transport_config() -> Arc<TransportConfig> {
    let mut config = TransportConfig::default();
    config
        .max_concurrent_bidi_streams(VarInt::from_u32(CONSUMER_MAX_INCOMING_STREAMS))
        .max_concurrent_uni_streams(VarInt::from_u32(CONSUMER_MAX_INCOMING_STREAMS))
        .max_idle_timeout(Some(CONSUMER_MAX_IDLE_TIMEOUT.try_into().unwrap()))
        .keep_alive_interval(Some(CONSUMER_KEEP_ALIVE_INTERVAL))
        .initial_mtu(1200)
        .min_mtu(1200)
        .mtu_discovery_config(None);
    Arc::new(config)
}

#[derive(Debug, thiserror::Error)]
pub enum ConsumerQuicError {
    #[error("failed to create the consumer QUIC endpoint: {0}")]
    Endpoint(#[from] io::Error),
    #[error("consumer QUIC endpoint is closed")]
    Closed,
    #[error("consumer QUIC accept was cancelled")]
    Cancelled,
    #[error("consumer QUIC handshake failed: {0}")]
    Handshake(#[from] quinn::ConnectionError),
    #[error("consumer QUIC connection broker stopped")]
    BrokerStopped,
    #[error("consumer QUIC stream failed: {0}")]
    Stream(#[source] quinn::ConnectionError),
}

pub type ConsumerQuicStream = Join<RecvStream, SendStream>;

#[derive(Debug)]
pub struct ConsumerQuicServer {
    endpoint: Endpoint,
    socket: Arc<VirtualUdpSocket>,
}

#[derive(Debug)]
pub struct ConsumerQuicBroker {
    server: Arc<ConsumerQuicServer>,
    connections: watch::Sender<Option<Connection>>,
    connection_updates: watch::Receiver<Option<Connection>>,
}

#[derive(Debug, Clone)]
pub struct ConsumerQuicDialer {
    connections: watch::Receiver<Option<Connection>>,
}

impl ConsumerQuicBroker {
    pub fn new(server: Arc<ConsumerQuicServer>) -> Self {
        let (connections, connection_updates) = watch::channel(None);
        Self {
            server,
            connections,
            connection_updates,
        }
    }

    pub fn dialer(&self) -> ConsumerQuicDialer {
        ConsumerQuicDialer {
            connections: self.connection_updates.clone(),
        }
    }

    pub async fn run(self, cancellation: CancellationToken) -> Result<(), ConsumerQuicError> {
        loop {
            let connection = match self.server.accept(&cancellation).await {
                Ok(connection) => connection,
                Err(ConsumerQuicError::Cancelled) => {
                    self.server.close();
                    self.connections.send_replace(None);
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            self.connections.send_replace(Some(connection.clone()));

            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.server.close();
                    self.connections.send_replace(None);
                    return Ok(());
                }
                _ = connection.closed() => {
                    self.connections.send_replace(None);
                }
            }
        }
    }
}

impl ConsumerQuicDialer {
    pub fn current_connection(&self) -> Option<Connection> {
        self.connections.borrow().clone()
    }

    pub async fn open_bi(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<ConsumerQuicStream, ConsumerQuicError> {
        let mut connections = self.connections.clone();
        loop {
            let current = connections.borrow().clone();
            if let Some(connection) = current {
                let opened = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(ConsumerQuicError::Cancelled),
                    opened = connection.open_bi() => opened,
                };
                match opened {
                    Ok((send, recv)) => return Ok(tokio::io::join(recv, send)),
                    Err(_) if connection.close_reason().is_some() => {}
                    Err(error) => return Err(ConsumerQuicError::Stream(error)),
                }
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(ConsumerQuicError::Cancelled),
                changed = connections.changed() => {
                    changed.map_err(|_| ConsumerQuicError::BrokerStopped)?;
                }
            }
        }
    }
}

impl ConsumerQuicServer {
    pub fn new(
        socket: Arc<VirtualUdpSocket>,
        mut server_config: ServerConfig,
    ) -> Result<Self, ConsumerQuicError> {
        server_config.transport = consumer_transport_config();
        let endpoint = Endpoint::new_with_abstract_socket(
            EndpointConfig::default(),
            Some(server_config),
            socket.clone(),
            Arc::new(quinn::TokioRuntime),
        )?;
        Ok(Self { endpoint, socket })
    }

    pub fn socket(&self) -> &Arc<VirtualUdpSocket> {
        &self.socket
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub async fn accept(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Connection, ConsumerQuicError> {
        let incoming = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ConsumerQuicError::Cancelled),
            incoming = self.endpoint.accept() => incoming.ok_or(ConsumerQuicError::Closed)?,
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ConsumerQuicError::Cancelled),
            connection = incoming => Ok(connection?),
        }
    }

    pub fn close(&self) {
        self.endpoint.close(0_u8.into(), b"consumer shutdown");
    }
}
