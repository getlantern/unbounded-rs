use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use lantern_unbounded::{VirtualPath, VirtualUdpSocket};
use quinn::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use quinn::{
    AsyncUdpSocket, ClientConfig, Endpoint, EndpointConfig, ServerConfig, TransportConfig,
};

fn addr(last_octet: u8, port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(100, 64, 0, last_octet)), port)
}

fn transport_config() -> Arc<TransportConfig> {
    let mut config = TransportConfig::default();
    config.initial_mtu(1200);
    config.min_mtu(1200);
    config.mtu_discovery_config(None);
    Arc::new(config)
}

fn endpoint_configs() -> (ServerConfig, ClientConfig) {
    let identity = rcgen::generate_simple_self_signed(vec!["unbounded.test".into()]).unwrap();
    let key = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());
    let cert = CertificateDer::from(identity.cert);

    let mut server = ServerConfig::with_single_cert(vec![cert.clone()], key.into()).unwrap();
    server.transport = transport_config();

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert).unwrap();
    let client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).unwrap();
    let mut client = ClientConfig::new(Arc::new(quic_crypto));
    client.transport_config(transport_config());
    (server, client)
}

fn bridge(mut left: VirtualPath, mut right: VirtualPath) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                packet = left.next_outgoing() => {
                    let Some(packet) = packet else { return };
                    right.try_receive(packet).unwrap();
                }
                packet = right.next_outgoing() => {
                    let Some(packet) = packet else { return };
                    left.try_receive(packet).unwrap();
                }
            }
        }
    })
}

async fn exchange(
    client_send: &mut quinn::SendStream,
    client_recv: &mut quinn::RecvStream,
    server_send: &mut quinn::SendStream,
    server_recv: &mut quinn::RecvStream,
    message: &[u8],
) {
    client_send.write_all(message).await.unwrap();
    let mut received = vec![0; message.len()];
    server_recv.read_exact(&mut received).await.unwrap();
    assert_eq!(received, message);

    server_send.write_all(message).await.unwrap();
    let mut echoed = vec![0; message.len()];
    client_recv.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, message);
}

#[tokio::test]
async fn quinn_server_keeps_stream_across_client_path_migration() {
    let server_addr = addr(1, 7000);
    let client_a_addr = addr(2, 8000);
    let client_b_addr = addr(3, 8001);
    let (server_config, client_config) = endpoint_configs();

    let server_socket = VirtualUdpSocket::new(server_addr);
    let client_a_socket = VirtualUdpSocket::new(client_a_addr);
    let path_a = bridge(
        server_socket.add_path(client_a_addr, 256),
        client_a_socket.add_path(server_addr, 256),
    );

    let server = Endpoint::new_with_abstract_socket(
        EndpointConfig::default(),
        Some(server_config),
        server_socket.clone(),
        Arc::new(quinn::TokioRuntime),
    )
    .unwrap();
    let mut client = Endpoint::new_with_abstract_socket(
        EndpointConfig::default(),
        None,
        client_a_socket,
        Arc::new(quinn::TokioRuntime),
    )
    .unwrap();
    client.set_default_client_config(client_config);

    let server_accept = async {
        server
            .accept()
            .await
            .expect("incoming QUIC handshake")
            .await
            .unwrap()
    };
    let client_connect = async {
        client
            .connect(server_addr, "unbounded.test")
            .unwrap()
            .await
            .unwrap()
    };
    let (server_connection, client_connection) = tokio::join!(server_accept, client_connect);
    assert_eq!(server_connection.remote_address(), client_a_addr);

    let (mut client_send, mut client_recv) = client_connection.open_bi().await.unwrap();
    // Quinn does not announce a newly-opened stream until it carries data. Write before awaiting
    // accept_bi, otherwise a test that joins open_bi and accept_bi deadlocks by construction.
    client_send.write_all(b"stream open").await.unwrap();
    let (mut server_send, mut server_recv) = server_connection.accept_bi().await.unwrap();
    let mut opening = [0_u8; 11];
    server_recv.read_exact(&mut opening).await.unwrap();
    assert_eq!(&opening, b"stream open");

    exchange(
        &mut client_send,
        &mut client_recv,
        &mut server_send,
        &mut server_recv,
        b"before migration",
    )
    .await;

    let client_b_socket = VirtualUdpSocket::new(client_b_addr);
    let path_b = bridge(
        server_socket.add_path(client_b_addr, 256),
        client_b_socket.add_path(server_addr, 256),
    );
    client
        .rebind_abstract(client_b_socket as Arc<dyn AsyncUdpSocket>)
        .unwrap();

    exchange(
        &mut client_send,
        &mut client_recv,
        &mut server_send,
        &mut server_recv,
        b"after migration",
    )
    .await;

    tokio::time::timeout(Duration::from_secs(2), async {
        while server_connection.remote_address() != client_b_addr {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server did not validate and adopt the replacement path");

    assert_eq!(server_connection.remote_address(), client_b_addr);
    path_a.abort();
    path_b.abort();
    client.close(0_u8.into(), b"test complete");
    server.close(0_u8.into(), b"test complete");
}
