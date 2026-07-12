use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;

pub type BoxTransportError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait DatagramTransport: Send {
    async fn send_packet(&mut self, packet: Bytes) -> Result<(), BoxTransportError>;
    async fn recv_packet(&mut self) -> Result<Option<Bytes>, BoxTransportError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayEnd {
    PeerClosed,
    EgressClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("peer transport failed: {0}")]
    Peer(#[source] BoxTransportError),
    #[error("egress transport failed: {0}")]
    Egress(#[source] BoxTransportError),
}

pub async fn relay<P, E>(peer: &mut P, egress: &mut E) -> Result<RelayEnd, RelayError>
where
    P: DatagramTransport,
    E: DatagramTransport,
{
    loop {
        tokio::select! {
            packet = peer.recv_packet() => {
                let Some(packet) = packet.map_err(RelayError::Peer)? else {
                    return Ok(RelayEnd::PeerClosed);
                };
                egress.send_packet(packet).await.map_err(RelayError::Egress)?;
            }
            packet = egress.recv_packet() => {
                let Some(packet) = packet.map_err(RelayError::Egress)? else {
                    return Ok(RelayEnd::EgressClosed);
                };
                peer.send_packet(packet).await.map_err(RelayError::Peer)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
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

    #[tokio::test]
    async fn relays_opaque_packets_in_both_directions() {
        let (mut peer, peer_in, mut peer_out) = channel_transport();
        let (mut egress, egress_in, mut egress_out) = channel_transport();

        let relay = tokio::spawn(async move { relay(&mut peer, &mut egress).await });
        peer_in
            .send(Bytes::from_static(b"consumer to egress"))
            .await
            .unwrap();
        assert_eq!(
            egress_out.recv().await.unwrap(),
            Bytes::from_static(b"consumer to egress")
        );

        egress_in
            .send(Bytes::from_static(b"egress to consumer"))
            .await
            .unwrap();
        assert_eq!(
            peer_out.recv().await.unwrap(),
            Bytes::from_static(b"egress to consumer")
        );

        drop(peer_in);
        assert_eq!(relay.await.unwrap().unwrap(), RelayEnd::PeerClosed);
    }
}
