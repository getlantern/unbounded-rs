use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::{ConsumerQuicDialer, ConsumerQuicError, ConsumerQuicStream};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Socks5Target {
    Ip(SocketAddr),
    Domain { host: String, port: u16 },
}

impl From<SocketAddr> for Socks5Target {
    fn from(value: SocketAddr) -> Self {
        Self::Ip(value)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConsumerSocks5Error {
    #[error("consumer QUIC stream failed: {0}")]
    Quic(#[from] ConsumerQuicError),
    #[error("consumer SOCKS5 handshake was cancelled")]
    Cancelled,
    #[error("consumer SOCKS5 I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("consumer SOCKS5 domain is too long: {0} bytes")]
    DomainTooLong(usize),
    #[error("consumer SOCKS5 domain is empty")]
    EmptyDomain,
    #[error("consumer SOCKS5 server selected unsupported auth method {0:#04x}")]
    UnsupportedAuth(u8),
    #[error("consumer SOCKS5 server returned version {0:#04x}")]
    InvalidVersion(u8),
    #[error("consumer SOCKS5 server rejected CONNECT with status {0:#04x}")]
    ConnectRejected(u8),
    #[error("consumer SOCKS5 server returned reserved byte {0:#04x}")]
    InvalidReserved(u8),
    #[error("consumer SOCKS5 server returned invalid address type {0:#04x}")]
    InvalidAddressType(u8),
}

pub async fn socks5_connect<S>(
    mut stream: S,
    target: &Socks5Target,
) -> Result<S, ConsumerSocks5Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut greeting = [0_u8; 2];
    stream.read_exact(&mut greeting).await?;
    if greeting[0] != 0x05 {
        return Err(ConsumerSocks5Error::InvalidVersion(greeting[0]));
    }
    if greeting[1] != 0x00 {
        return Err(ConsumerSocks5Error::UnsupportedAuth(greeting[1]));
    }

    let mut request = vec![0x05, 0x01, 0x00];
    match target {
        Socks5Target::Ip(SocketAddr::V4(address)) => {
            request.push(0x01);
            request.extend_from_slice(&address.ip().octets());
            request.extend_from_slice(&address.port().to_be_bytes());
        }
        Socks5Target::Ip(SocketAddr::V6(address)) => {
            request.push(0x04);
            request.extend_from_slice(&address.ip().octets());
            request.extend_from_slice(&address.port().to_be_bytes());
        }
        Socks5Target::Domain { host, port } => {
            if host.is_empty() {
                return Err(ConsumerSocks5Error::EmptyDomain);
            }
            let length = u8::try_from(host.len())
                .map_err(|_| ConsumerSocks5Error::DomainTooLong(host.len()))?;
            request.extend_from_slice(&[0x03, length]);
            request.extend_from_slice(host.as_bytes());
            request.extend_from_slice(&port.to_be_bytes());
        }
    }
    stream.write_all(&request).await?;

    let mut reply = [0_u8; 4];
    stream.read_exact(&mut reply).await?;
    if reply[0] != 0x05 {
        return Err(ConsumerSocks5Error::InvalidVersion(reply[0]));
    }
    if reply[1] != 0x00 {
        return Err(ConsumerSocks5Error::ConnectRejected(reply[1]));
    }
    if reply[2] != 0x00 {
        return Err(ConsumerSocks5Error::InvalidReserved(reply[2]));
    }
    let address_length = match reply[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut length = [0_u8; 1];
            stream.read_exact(&mut length).await?;
            usize::from(length[0])
        }
        address_type => return Err(ConsumerSocks5Error::InvalidAddressType(address_type)),
    };
    let mut bound_address_and_port = vec![0_u8; address_length + 2];
    stream.read_exact(&mut bound_address_and_port).await?;
    Ok(stream)
}

impl ConsumerQuicDialer {
    pub async fn connect_socks5(
        &self,
        target: &Socks5Target,
        cancellation: &CancellationToken,
    ) -> Result<ConsumerQuicStream, ConsumerSocks5Error> {
        let stream = self.open_bi(cancellation).await?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ConsumerSocks5Error::Cancelled),
            connected = socks5_connect(stream, target) => connected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    async fn serve_success(stream: tokio::io::DuplexStream, expected_request: Vec<u8>) {
        let (mut read, mut write) = tokio::io::split(stream);
        let mut greeting = [0_u8; 3];
        read.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [0x05, 0x01, 0x00]);
        write.write_all(&[0x05, 0x00]).await.unwrap();

        let mut request = vec![0_u8; expected_request.len()];
        read.read_exact(&mut request).await.unwrap();
        assert_eq!(request, expected_request);
        write
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0x1f, 0x90])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn handshakes_ipv4_target() {
        let (client, server) = tokio::io::duplex(256);
        let target = Socks5Target::Ip(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            443,
        ));
        let server = tokio::spawn(serve_success(
            server,
            vec![0x05, 0x01, 0x00, 0x01, 192, 0, 2, 10, 0x01, 0xbb],
        ));
        socks5_connect(client, &target).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn handshakes_ipv6_target_and_consumes_domain_reply() {
        let (client, mut server) = tokio::io::duplex(256);
        let target = Socks5Target::Ip(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 53));
        let server_task = tokio::spawn(async move {
            let mut greeting = [0_u8; 3];
            server.read_exact(&mut greeting).await.unwrap();
            server.write_all(&[0x05, 0x00]).await.unwrap();
            let mut request = [0_u8; 22];
            server.read_exact(&mut request).await.unwrap();
            assert_eq!(&request[..4], &[0x05, 0x01, 0x00, 0x04]);
            assert_eq!(&request[4..20], &Ipv6Addr::LOCALHOST.octets());
            assert_eq!(&request[20..], &53_u16.to_be_bytes());
            server
                .write_all(&[0x05, 0x00, 0x00, 0x03, 0x03, b'e', b'x', b't', 0, 80])
                .await
                .unwrap();
        });
        socks5_connect(client, &target).await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn handshakes_domain_target() {
        let (client, server) = tokio::io::duplex(256);
        let target = Socks5Target::Domain {
            host: "example.com".into(),
            port: 8443,
        };
        let mut request = vec![0x05, 0x01, 0x00, 0x03, 11];
        request.extend_from_slice(b"example.com");
        request.extend_from_slice(&8443_u16.to_be_bytes());
        let server = tokio::spawn(serve_success(server, request));
        socks5_connect(client, &target).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_failed_connect_response() {
        let (client, mut server) = tokio::io::duplex(256);
        let server = tokio::spawn(async move {
            let mut greeting = [0_u8; 3];
            server.read_exact(&mut greeting).await.unwrap();
            server.write_all(&[0x05, 0x00]).await.unwrap();
            let mut request = [0_u8; 10];
            server.read_exact(&mut request).await.unwrap();
            server.write_all(&[0x05, 0x05, 0x00, 0x01]).await.unwrap();
        });
        let error = socks5_connect(client, &Socks5Target::from(addr()))
            .await
            .unwrap_err();
        assert!(matches!(error, ConsumerSocks5Error::ConnectRejected(0x05)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_empty_domain_before_writing_connect_request() {
        let (client, mut server) = tokio::io::duplex(256);
        let server = tokio::spawn(async move {
            let mut greeting = [0_u8; 3];
            server.read_exact(&mut greeting).await.unwrap();
            server.write_all(&[0x05, 0x00]).await.unwrap();
        });
        let error = socks5_connect(
            client,
            &Socks5Target::Domain {
                host: String::new(),
                port: 80,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ConsumerSocks5Error::EmptyDomain));
        server.await.unwrap();
    }

    fn addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 80)
    }
}
