use std::collections::HashMap;
use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::task::{Context, Poll};

use bytes::Bytes;
use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use tokio::sync::mpsc;

pub const DEFAULT_QUEUE_CAPACITY: usize = 4096;
pub const DEFAULT_PATH_MTU: usize = 1200;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PathReceiveError {
    #[error("datagram is {actual} bytes, exceeding the path MTU of {mtu}")]
    Oversize { actual: usize, mtu: usize },
    #[error("virtual UDP receive queue is full")]
    QueueFull,
    #[error("virtual UDP socket is closed")]
    Closed,
}

#[derive(Debug)]
struct ReceivedDatagram {
    remote: SocketAddr,
    payload: Bytes,
}

pub struct VirtualUdpSocket {
    local_addr: SocketAddr,
    mtu: usize,
    incoming_tx: mpsc::Sender<ReceivedDatagram>,
    incoming_rx: Mutex<mpsc::Receiver<ReceivedDatagram>>,
    routes: RwLock<HashMap<SocketAddr, mpsc::Sender<Bytes>>>,
}

impl fmt::Debug for VirtualUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VirtualUdpSocket")
            .field("local_addr", &self.local_addr)
            .field("mtu", &self.mtu)
            .field("paths", &self.routes.read().unwrap().len())
            .finish()
    }
}

impl VirtualUdpSocket {
    pub fn new(local_addr: SocketAddr) -> Arc<Self> {
        Self::with_options(local_addr, DEFAULT_PATH_MTU, DEFAULT_QUEUE_CAPACITY)
    }

    pub fn with_options(local_addr: SocketAddr, mtu: usize, queue_capacity: usize) -> Arc<Self> {
        assert!(mtu > 0, "path MTU must be non-zero");
        assert!(queue_capacity > 0, "queue capacity must be non-zero");
        let (incoming_tx, incoming_rx) = mpsc::channel(queue_capacity);
        Arc::new(Self {
            local_addr,
            mtu,
            incoming_tx,
            incoming_rx: Mutex::new(incoming_rx),
            routes: RwLock::new(HashMap::new()),
        })
    }

    pub fn add_path(self: &Arc<Self>, remote: SocketAddr, queue_capacity: usize) -> VirtualPath {
        assert!(queue_capacity > 0, "queue capacity must be non-zero");
        let (outgoing_tx, outgoing_rx) = mpsc::channel(queue_capacity);
        self.routes
            .write()
            .unwrap()
            .insert(remote, outgoing_tx.clone());
        VirtualPath {
            remote,
            mtu: self.mtu,
            incoming: self.incoming_tx.clone(),
            outgoing_tx,
            outgoing: outgoing_rx,
            socket: Arc::downgrade(self),
        }
    }

    pub fn path_count(&self) -> usize {
        self.routes.read().unwrap().len()
    }
}

pub struct VirtualPath {
    remote: SocketAddr,
    mtu: usize,
    incoming: mpsc::Sender<ReceivedDatagram>,
    outgoing_tx: mpsc::Sender<Bytes>,
    outgoing: mpsc::Receiver<Bytes>,
    socket: Weak<VirtualUdpSocket>,
}

impl fmt::Debug for VirtualPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VirtualPath")
            .field("remote", &self.remote)
            .field("mtu", &self.mtu)
            .finish_non_exhaustive()
    }
}

impl VirtualPath {
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote
    }

    pub fn try_receive(&self, payload: impl Into<Bytes>) -> Result<(), PathReceiveError> {
        let payload = payload.into();
        if payload.len() > self.mtu {
            return Err(PathReceiveError::Oversize {
                actual: payload.len(),
                mtu: self.mtu,
            });
        }
        self.incoming
            .try_send(ReceivedDatagram {
                remote: self.remote,
                payload,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => PathReceiveError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => PathReceiveError::Closed,
            })
    }

    pub async fn next_outgoing(&mut self) -> Option<Bytes> {
        self.outgoing.recv().await
    }
}

impl Drop for VirtualPath {
    fn drop(&mut self) {
        let Some(socket) = self.socket.upgrade() else {
            return;
        };
        let mut routes = socket.routes.write().unwrap();
        if routes
            .get(&self.remote)
            .is_some_and(|current| current.same_channel(&self.outgoing_tx))
        {
            routes.remove(&self.remote);
        }
    }
}

#[derive(Debug)]
struct AlwaysWritable;

impl UdpPoller for AlwaysWritable {
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncUdpSocket for VirtualUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(AlwaysWritable)
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        if transmit.contents.len() > self.mtu {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "QUIC datagram is {} bytes, exceeding the virtual path MTU of {}",
                    transmit.contents.len(),
                    self.mtu
                ),
            ));
        }

        let route = self
            .routes
            .read()
            .unwrap()
            .get(&transmit.destination)
            .cloned();
        if let Some(route) = route {
            // A full or just-closed peer path is packet loss, not a fatal socket error. QUIC
            // performs its own recovery, and a replacement peer may be installed shortly.
            let _ = route.try_send(Bytes::copy_from_slice(transmit.contents));
        }
        // Missing routes are handled the same way. Keeping the virtual socket alive is essential:
        // closing it when one WebRTC peer disappears would destroy the QUIC server connection
        // before the Go egress can migrate onto the replacement peer.
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut incoming = self.incoming_rx.lock().unwrap();
        match incoming.poll_recv(cx) {
            Poll::Ready(Some(datagram)) => {
                let len = datagram.payload.len().min(bufs[0].len());
                bufs[0][..len].copy_from_slice(&datagram.payload[..len]);
                meta[0] = RecvMeta {
                    addr: datagram.remote,
                    len,
                    stride: len,
                    ecn: None,
                    dst_ip: Some(self.local_addr.ip()),
                };
                Poll::Ready(Ok(1))
            }
            Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "virtual UDP receive queue closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::poll_fn;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(last_octet: u8, port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(100, 64, 0, last_octet)), port)
    }

    #[tokio::test]
    async fn path_preserves_datagram_and_remote_address() {
        let socket = VirtualUdpSocket::new(addr(1, 7000));
        let path = socket.add_path(addr(2, 8000), 8);
        path.try_receive(Bytes::from_static(b"quic packet"))
            .unwrap();

        let mut storage = [0_u8; 64];
        let mut bufs = [IoSliceMut::new(&mut storage)];
        let mut meta = [RecvMeta::default()];
        let count = poll_fn(|cx| socket.poll_recv(cx, &mut bufs, &mut meta))
            .await
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(&storage[..meta[0].len], b"quic packet");
        assert_eq!(meta[0].addr, addr(2, 8000));
    }

    #[tokio::test]
    async fn outgoing_datagram_is_routed_by_quinn_destination() {
        let socket = VirtualUdpSocket::new(addr(1, 7000));
        let mut path = socket.add_path(addr(2, 8000), 8);
        socket
            .try_send(&Transmit {
                destination: addr(2, 8000),
                ecn: None,
                contents: b"path response",
                segment_size: None,
                src_ip: None,
            })
            .unwrap();

        assert_eq!(
            path.next_outgoing().await.unwrap(),
            Bytes::from_static(b"path response")
        );
    }

    #[test]
    fn losing_one_path_does_not_close_the_socket() {
        let socket = VirtualUdpSocket::new(addr(1, 7000));
        let path_a = socket.add_path(addr(2, 8000), 8);
        let path_b = socket.add_path(addr(3, 8001), 8);
        assert_eq!(socket.path_count(), 2);
        drop(path_a);
        assert_eq!(socket.path_count(), 1);
        assert_eq!(socket.local_addr().unwrap(), addr(1, 7000));
        drop(path_b);
        assert_eq!(socket.path_count(), 0);
        assert_eq!(socket.local_addr().unwrap(), addr(1, 7000));
    }

    #[test]
    fn path_mtu_rejects_oversize_ingress() {
        let socket = VirtualUdpSocket::with_options(addr(1, 7000), 1200, 8);
        let path = socket.add_path(addr(2, 8000), 8);
        assert_eq!(
            path.try_receive(vec![0; 1201]),
            Err(PathReceiveError::Oversize {
                actual: 1201,
                mtu: 1200
            })
        );
    }
}
