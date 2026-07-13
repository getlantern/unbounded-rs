use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::protocol::UnboundedPacket;
use crate::relay::{BoxTransportError, DatagramTransport};
use crate::virtual_udp::{PathReceiveError, VirtualUdpSocket};

const CGNAT_BASE: u32 = u32::from_be_bytes([100, 64, 0, 0]);
const CGNAT_HOSTS: u32 = 1 << 22;
const FIRST_PATH_HOST: u32 = 2;
const SYNTHETIC_PATH_PORT: u16 = 443;

#[derive(Debug)]
pub struct SyntheticPathAllocator {
    next_host: AtomicU32,
}

impl Default for SyntheticPathAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl SyntheticPathAllocator {
    pub fn new() -> Self {
        Self {
            next_host: AtomicU32::new(FIRST_PATH_HOST),
        }
    }

    pub fn allocate(&self) -> Result<SocketAddr, ConsumerPathError> {
        let host = self
            .next_host
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current < CGNAT_HOSTS).then_some(current + 1)
            })
            .map_err(|_| ConsumerPathError::AddressSpaceExhausted)?;
        Ok(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::from(CGNAT_BASE + host)),
            SYNTHETIC_PATH_PORT,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerPathEnd {
    PeerClosed,
    SocketClosed,
    Cancelled,
}

#[derive(Debug, thiserror::Error)]
pub enum ConsumerPathError {
    #[error("peer transport failed: {0}")]
    Transport(#[source] BoxTransportError),
    #[error("peer sent an invalid Unbounded packet: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("peer path source changed from {expected:?} to {actual:?}")]
    SourceChanged { expected: String, actual: String },
    #[error("peer path supplied an empty source identity")]
    EmptySource,
    #[error("peer path datagram could not enter the virtual UDP socket: {0}")]
    Receive(#[source] PathReceiveError),
    #[error("synthetic consumer path address space is exhausted")]
    AddressSpaceExhausted,
}

pub async fn run_consumer_path<T>(
    socket: Arc<VirtualUdpSocket>,
    remote: SocketAddr,
    queue_capacity: usize,
    mut peer: T,
    cancellation: CancellationToken,
) -> Result<ConsumerPathEnd, ConsumerPathError>
where
    T: DatagramTransport,
{
    let mut path = socket.add_path(remote, queue_capacity);
    let mut source: Option<String> = None;

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(ConsumerPathEnd::Cancelled),
            packet = peer.recv_packet() => {
                let Some(packet) = packet.map_err(ConsumerPathError::Transport)? else {
                    return Ok(ConsumerPathEnd::PeerClosed);
                };
                let packet: UnboundedPacket = serde_json::from_slice(&packet)?;
                if packet.source_addr.is_empty() {
                    return Err(ConsumerPathError::EmptySource);
                }
                match &source {
                    Some(expected) if expected != &packet.source_addr => {
                        return Err(ConsumerPathError::SourceChanged {
                            expected: expected.clone(),
                            actual: packet.source_addr,
                        });
                    }
                    None => source = Some(packet.source_addr),
                    _ => {}
                }
                match path.try_receive(packet.payload) {
                    Ok(()) | Err(PathReceiveError::QueueFull) => {}
                    Err(PathReceiveError::Closed) => return Ok(ConsumerPathEnd::SocketClosed),
                    Err(error @ PathReceiveError::Oversize { .. }) => {
                        return Err(ConsumerPathError::Receive(error));
                    }
                }
            }
            packet = path.next_outgoing() => {
                let Some(packet) = packet else {
                    return Ok(ConsumerPathEnd::SocketClosed);
                };
                peer.send_packet(packet).await.map_err(ConsumerPathError::Transport)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::future::poll_fn;
    use quinn::udp::{RecvMeta, Transmit};
    use quinn::AsyncUdpSocket;
    use std::io::IoSliceMut;
    use tokio::sync::mpsc;

    use super::*;

    struct ChannelTransport {
        incoming: mpsc::Receiver<Bytes>,
        outgoing: mpsc::Sender<Bytes>,
    }

    #[async_trait]
    impl DatagramTransport for ChannelTransport {
        async fn send_packet(&mut self, packet: Bytes) -> Result<(), BoxTransportError> {
            self.outgoing
                .send(packet)
                .await
                .map_err(|error| Box::new(error) as BoxTransportError)
        }

        async fn recv_packet(&mut self) -> Result<Option<Bytes>, BoxTransportError> {
            Ok(self.incoming.recv().await)
        }
    }

    fn channel_transport() -> (ChannelTransport, mpsc::Sender<Bytes>, mpsc::Receiver<Bytes>) {
        let (incoming_tx, incoming) = mpsc::channel(8);
        let (outgoing, outgoing_rx) = mpsc::channel(8);
        (
            ChannelTransport { incoming, outgoing },
            incoming_tx,
            outgoing_rx,
        )
    }

    fn local_addr() -> SocketAddr {
        "100.64.0.1:7000".parse().unwrap()
    }

    #[test]
    fn allocates_distinct_cgnat_path_addresses() {
        let allocator = SyntheticPathAllocator::new();
        assert_eq!(
            allocator.allocate().unwrap(),
            "100.64.0.2:443".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            allocator.allocate().unwrap(),
            "100.64.0.3:443".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn allocates_the_final_cgnat_address_then_reports_exhaustion() {
        let allocator = SyntheticPathAllocator {
            next_host: AtomicU32::new(CGNAT_HOSTS - 1),
        };
        assert_eq!(
            allocator.allocate().unwrap(),
            "100.127.255.255:443".parse::<SocketAddr>().unwrap()
        );
        assert!(matches!(
            allocator.allocate(),
            Err(ConsumerPathError::AddressSpaceExhausted)
        ));
    }

    #[tokio::test]
    async fn translates_enveloped_ingress_and_raw_egress() {
        let socket = VirtualUdpSocket::new(local_addr());
        let remote = "100.64.0.2:443".parse().unwrap();
        let (transport, incoming, mut outgoing) = channel_transport();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(run_consumer_path(
            socket.clone(),
            remote,
            8,
            transport,
            cancellation.clone(),
        ));

        let packet =
            serde_json::to_vec(&UnboundedPacket::new("egress-path-a", b"incoming")).unwrap();
        incoming.send(packet.into()).await.unwrap();

        let mut storage = [0_u8; 64];
        let mut bufs = [IoSliceMut::new(&mut storage)];
        let mut meta = [RecvMeta::default()];
        let count = poll_fn(|cx| socket.poll_recv(cx, &mut bufs, &mut meta))
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(&storage[..meta[0].len], b"incoming");
        assert_eq!(meta[0].addr, remote);

        socket
            .try_send(&Transmit {
                destination: remote,
                ecn: None,
                contents: b"outgoing",
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
        assert_eq!(
            outgoing.recv().await.unwrap(),
            Bytes::from_static(b"outgoing")
        );

        cancellation.cancel();
        assert_eq!(task.await.unwrap().unwrap(), ConsumerPathEnd::Cancelled);
    }

    #[tokio::test]
    async fn rejects_a_source_change_within_one_peer_path() {
        let socket = VirtualUdpSocket::new(local_addr());
        let remote = "100.64.0.2:443".parse().unwrap();
        let (transport, incoming, _) = channel_transport();
        let task = tokio::spawn(run_consumer_path(
            socket,
            remote,
            8,
            transport,
            CancellationToken::new(),
        ));

        for source in ["egress-path-a", "egress-path-b"] {
            incoming
                .send(
                    serde_json::to_vec(&UnboundedPacket::new(source, b"packet"))
                        .unwrap()
                        .into(),
                )
                .await
                .unwrap();
        }

        assert!(matches!(
            task.await.unwrap(),
            Err(ConsumerPathError::SourceChanged { .. })
        ));
    }
}
