use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use webrtc::ice_transport::ice_candidate::RTCIceCandidate;
use webrtc::ice_transport::ice_protocol::RTCIceProtocol;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::consumer_path::{
    run_consumer_path, ConsumerPathEnd, ConsumerPathError, SyntheticPathAllocator,
};
use crate::protocol::{SignalMessage, SignalMessageType};
use crate::rtc::{build_api_with_options, create_unreliable_data_channel, WebRtcDatagrams};
use crate::signaling::{ConsumerSignaler, SignalingError};
use crate::virtual_udp::{VirtualUdpSocket, DEFAULT_QUEUE_CAPACITY};

type DataChannelWaitSender =
    Arc<Mutex<Option<oneshot::Sender<Result<WebRtcDatagrams, RTCPeerConnectionState>>>>>;

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub signaler: Arc<dyn ConsumerSignaler>,
    pub socket: Arc<VirtualUdpSocket>,
    pub path_allocator: Arc<SyntheticPathAllocator>,
    pub consumer_session_id: String,
    pub stun_urls: Vec<String>,
    pub tag: String,
    pub patience: Duration,
    pub nat_timeout: Duration,
    pub path_queue_capacity: usize,
    pub enable_ipv6: bool,
}

impl ConsumerConfig {
    pub fn new(
        signaler: Arc<dyn ConsumerSignaler>,
        socket: Arc<VirtualUdpSocket>,
        path_allocator: Arc<SyntheticPathAllocator>,
        consumer_session_id: impl Into<String>,
    ) -> Self {
        Self {
            signaler,
            socket,
            path_allocator,
            consumer_session_id: consumer_session_id.into(),
            stun_urls: Vec::new(),
            tag: String::new(),
            patience: Duration::from_millis(500),
            nat_timeout: Duration::from_secs(5),
            path_queue_capacity: DEFAULT_QUEUE_CAPACITY,
            enable_ipv6: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerOutcome {
    pub remote: std::net::SocketAddr,
    pub path_end: ConsumerPathEnd,
}

#[derive(Debug, Clone)]
pub struct ConsumerSupervisorConfig {
    pub consumer: ConsumerConfig,
    pub retry_delay: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerSummary {
    pub attempts: u64,
    pub completed_paths: u64,
    pub failed_attempts: u64,
}

#[derive(Debug)]
pub enum ConsumerEvent {
    AttemptStarted {
        attempt: u64,
    },
    PathEnded {
        attempt: u64,
        outcome: ConsumerOutcome,
    },
    AttemptFailed {
        attempt: u64,
        error: String,
    },
    Stopped {
        summary: ConsumerSummary,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("Freddie signaling failed: {0}")]
    Signaling(#[from] SignalingError),
    #[error("WebRTC failed: {0}")]
    WebRtc(#[from] webrtc::Error),
    #[error("invalid consumer signaling JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Freddie advertisement stream ended without a genesis message")]
    NoGenesis,
    #[error("Freddie returned {actual:?} while {expected:?} was required")]
    UnexpectedSignal {
        expected: SignalMessageType,
        actual: SignalMessageType,
    },
    #[error("Freddie returned no answer to the consumer offer")]
    MissingAnswer,
    #[error("timed out waiting for the peer WebRTC DataChannel")]
    NatTimeout,
    #[error("peer WebRTC DataChannel callback ended before opening the channel")]
    DataChannelClosed,
    #[error("peer WebRTC connection became {0} before opening a DataChannel")]
    PeerConnectionEnded(RTCPeerConnectionState),
    #[error("consumer path failed: {0}")]
    Path(#[from] ConsumerPathError),
    #[error("consumer session cancelled")]
    Cancelled,
}

#[derive(Debug, Serialize)]
struct OfferMessage {
    #[serde(rename = "SDP")]
    sdp: RTCSessionDescription,
    #[serde(rename = "Tag")]
    tag: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct IceMessage {
    #[serde(rename = "Candidates")]
    candidates: Vec<ConsumerIceCandidate>,
    #[serde(rename = "ConsumerSessionID")]
    consumer_session_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerIceCandidate {
    foundation: String,
    priority: u32,
    address: String,
    protocol: u8,
    port: u16,
    #[serde(rename = "type")]
    typ: webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType,
    component: u16,
    #[serde(rename = "relatedAddress")]
    related_address: String,
    #[serde(rename = "relatedPort")]
    related_port: u16,
    #[serde(rename = "tcpType")]
    tcp_type: String,
}

impl From<&RTCIceCandidate> for ConsumerIceCandidate {
    fn from(candidate: &RTCIceCandidate) -> Self {
        Self {
            foundation: candidate.foundation.clone(),
            priority: candidate.priority,
            address: candidate.address.clone(),
            protocol: match candidate.protocol {
                RTCIceProtocol::Udp => 1,
                RTCIceProtocol::Tcp => 2,
                RTCIceProtocol::Unspecified => 0,
            },
            port: candidate.port,
            typ: candidate.typ,
            component: candidate.component,
            related_address: candidate.related_address.clone(),
            related_port: candidate.related_port,
            tcp_type: candidate.tcp_type.clone(),
        }
    }
}

#[cfg(test)]
impl ConsumerIceCandidate {
    fn to_rtc_candidate(&self) -> RTCIceCandidate {
        RTCIceCandidate {
            stats_id: String::new(),
            foundation: self.foundation.clone(),
            priority: self.priority,
            address: self.address.clone(),
            protocol: match self.protocol {
                1 => RTCIceProtocol::Udp,
                2 => RTCIceProtocol::Tcp,
                _ => RTCIceProtocol::Unspecified,
            },
            port: self.port,
            typ: self.typ,
            component: self.component,
            related_address: self.related_address.clone(),
            related_port: self.related_port,
            tcp_type: self.tcp_type.clone(),
        }
    }
}

pub async fn run_consumer_session(
    config: ConsumerConfig,
    cancellation: CancellationToken,
) -> Result<ConsumerOutcome, ConsumerError> {
    let api = build_api_with_options(config.enable_ipv6, false)?;
    let ice_servers = if config.stun_urls.is_empty() {
        Vec::new()
    } else {
        vec![RTCIceServer {
            urls: config.stun_urls.clone(),
            ..Default::default()
        }]
    };
    let connection = Arc::new(
        api.new_peer_connection(RTCConfiguration {
            ice_servers,
            ..Default::default()
        })
        .await?,
    );

    let channel = create_unreliable_data_channel(&connection).await?;
    let (data_channel_tx, data_channel_rx) = oneshot::channel();
    let data_channel_tx: DataChannelWaitSender = Arc::new(Mutex::new(Some(data_channel_tx)));
    let open_tx = data_channel_tx.clone();
    let channel_for_open = channel.clone();
    channel.on_open(Box::new(move || {
        if let Some(tx) = open_tx.lock().unwrap().take() {
            let _ = tx.send(Ok(WebRtcDatagrams::new(channel_for_open.clone())));
        }
        Box::pin(async {})
    }));
    let state_tx = data_channel_tx.clone();
    connection.on_peer_connection_state_change(Box::new(move |state| {
        if is_terminal_peer_state(state) {
            if let Some(tx) = state_tx.lock().unwrap().take() {
                let _ = tx.send(Err(state));
            }
        }
        Box::pin(async {})
    }));

    let candidates = Arc::new(Mutex::new(Vec::new()));
    let gathered_candidates = candidates.clone();
    connection.on_ice_candidate(Box::new(move |candidate| {
        if let Some(candidate) = candidate {
            gathered_candidates.lock().unwrap().push(candidate);
        }
        Box::pin(async {})
    }));

    let session = async {
        let genesis =
            select_genesis(config.signaler.as_ref(), config.patience, &cancellation).await?;
        let offer = connection.create_offer(None).await?;
        let offer_payload = serde_json::to_string(&OfferMessage {
            sdp: offer.clone(),
            tag: config.tag.clone(),
        })?;
        let answer_signal = config
            .signaler
            .exchange(&genesis.reply_to, SignalMessageType::Offer, &offer_payload)
            .await?
            .ok_or(ConsumerError::MissingAnswer)?;
        expect_signal(&answer_signal, SignalMessageType::Answer)?;
        let answer: RTCSessionDescription = answer_signal.decode_payload()?;

        let mut gathering_complete = connection.gathering_complete_promise().await;
        connection.set_local_description(offer).await?;
        connection.set_remote_description(answer).await?;
        let _ = gathering_complete.recv().await;

        let ice_payload = serde_json::to_string(&IceMessage {
            candidates: candidates
                .lock()
                .unwrap()
                .iter()
                .map(ConsumerIceCandidate::from)
                .collect(),
            consumer_session_id: config.consumer_session_id.clone(),
        })?;
        let _ = config
            .signaler
            .exchange(
                &answer_signal.reply_to,
                SignalMessageType::Ice,
                &ice_payload,
            )
            .await?;

        let datagrams = tokio::time::timeout(config.nat_timeout, data_channel_rx)
            .await
            .map_err(|_| ConsumerError::NatTimeout)?
            .map_err(|_| ConsumerError::DataChannelClosed)?
            .map_err(ConsumerError::PeerConnectionEnded)?;
        let remote = config.path_allocator.allocate()?;
        let path_end = run_consumer_path(
            config.socket,
            remote,
            config.path_queue_capacity,
            datagrams,
            cancellation.clone(),
        )
        .await?;
        Ok(ConsumerOutcome { remote, path_end })
    };

    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(ConsumerError::Cancelled),
        result = session => result,
    };
    let _ = connection.close().await;
    result
}

pub async fn maintain_consumer(
    config: ConsumerSupervisorConfig,
    cancellation: CancellationToken,
    events: Option<mpsc::UnboundedSender<ConsumerEvent>>,
) -> ConsumerSummary {
    let mut summary = ConsumerSummary::default();
    loop {
        if cancellation.is_cancelled() {
            break;
        }
        summary.attempts += 1;
        let attempt = summary.attempts;
        emit(&events, ConsumerEvent::AttemptStarted { attempt });
        let result =
            run_consumer_session(config.consumer.clone(), cancellation.child_token()).await;
        if result.is_err() && cancellation.is_cancelled() {
            break;
        }
        match result {
            Ok(outcome) if outcome.path_end == ConsumerPathEnd::Cancelled => break,
            Ok(outcome) => {
                summary.completed_paths += 1;
                emit(&events, ConsumerEvent::PathEnded { attempt, outcome });
            }
            Err(error) => {
                summary.failed_attempts += 1;
                emit(
                    &events,
                    ConsumerEvent::AttemptFailed {
                        attempt,
                        error: error.to_string(),
                    },
                );
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(config.retry_delay) => {}
                }
            }
        }
    }
    emit(
        &events,
        ConsumerEvent::Stopped {
            summary: summary.clone(),
        },
    );
    summary
}

fn emit(events: &Option<mpsc::UnboundedSender<ConsumerEvent>>, event: ConsumerEvent) {
    if let Some(events) = events {
        let _ = events.send(event);
    }
}

async fn select_genesis(
    signaler: &dyn ConsumerSignaler,
    patience: Duration,
    cancellation: &CancellationToken,
) -> Result<SignalMessage, ConsumerError> {
    let mut advertisements = signaler.advertisements().await?;
    let first = loop {
        let message = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ConsumerError::Cancelled),
            message = advertisements.next() => message?,
        };
        let Some(message) = message else {
            return Err(ConsumerError::NoGenesis);
        };
        if message.kind == SignalMessageType::Genesis {
            break message;
        }
    };

    let mut candidates = vec![first];
    let patience = tokio::time::sleep(patience);
    tokio::pin!(patience);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ConsumerError::Cancelled),
            _ = &mut patience => break,
            message = advertisements.next() => match message? {
                Some(message) if message.kind == SignalMessageType::Genesis => candidates.push(message),
                Some(_) => {}
                None => break,
            }
        }
    }

    Ok(candidates.choose(&mut rand::rng()).unwrap().clone())
}

fn expect_signal(signal: &SignalMessage, expected: SignalMessageType) -> Result<(), ConsumerError> {
    if signal.kind == expected {
        Ok(())
    } else {
        Err(ConsumerError::UnexpectedSignal {
            expected,
            actual: signal.kind,
        })
    }
}

fn is_terminal_peer_state(state: RTCPeerConnectionState) -> bool {
    matches!(
        state,
        RTCPeerConnectionState::Disconnected
            | RTCPeerConnectionState::Failed
            | RTCPeerConnectionState::Closed
    )
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::*;
    use crate::protocol::UnboundedPacket;
    use crate::signaling::{AdvertisementSource, Signaler};
    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::oneshot;
    use webrtc::data_channel::data_channel_state::RTCDataChannelState;
    use webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType;

    struct SingleAdvertisement(Option<SignalMessage>);

    #[async_trait]
    impl AdvertisementSource for SingleAdvertisement {
        async fn next(&mut self) -> Result<Option<SignalMessage>, SignalingError> {
            Ok(self.0.take())
        }
    }

    struct PeerSignaler {
        connection: Mutex<Option<Arc<webrtc::peer_connection::RTCPeerConnection>>>,
        channel_rx: tokio::sync::Mutex<
            Option<oneshot::Receiver<Arc<webrtc::data_channel::RTCDataChannel>>>,
        >,
    }

    impl PeerSignaler {
        fn new() -> Self {
            Self {
                connection: Mutex::new(None),
                channel_rx: tokio::sync::Mutex::new(None),
            }
        }
    }

    impl fmt::Debug for PeerSignaler {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("PeerSignaler").finish_non_exhaustive()
        }
    }

    #[async_trait]
    impl Signaler for PeerSignaler {
        async fn exchange(
            &self,
            _send_to: &str,
            kind: SignalMessageType,
            payload: &str,
        ) -> Result<Option<SignalMessage>, SignalingError> {
            match kind {
                SignalMessageType::Offer => {
                    let offer: serde_json::Value = serde_json::from_str(payload)?;
                    let offer: RTCSessionDescription =
                        serde_json::from_value(offer["SDP"].clone())?;
                    let api = build_api_with_options(false, false).unwrap();
                    let connection = Arc::new(
                        api.new_peer_connection(RTCConfiguration::default())
                            .await
                            .unwrap(),
                    );
                    let (channel_tx, channel_rx) = oneshot::channel();
                    let channel_tx = Arc::new(Mutex::new(Some(channel_tx)));
                    connection.on_data_channel(Box::new(move |channel| {
                        if let Some(tx) = channel_tx.lock().unwrap().take() {
                            let _ = tx.send(channel);
                        }
                        Box::pin(async {})
                    }));
                    connection.set_remote_description(offer).await.unwrap();
                    let answer = connection.create_answer(None).await.unwrap();
                    let mut gathering = connection.gathering_complete_promise().await;
                    connection.set_local_description(answer).await.unwrap();
                    let _ = gathering.recv().await;
                    let answer = connection.local_description().await.unwrap();
                    *self.connection.lock().unwrap() = Some(connection);
                    *self.channel_rx.lock().await = Some(channel_rx);
                    Ok(Some(SignalMessage {
                        reply_to: "ice-request".into(),
                        kind: SignalMessageType::Answer,
                        payload: serde_json::to_string(&answer)?,
                    }))
                }
                SignalMessageType::Ice => {
                    let ice: IceMessage = serde_json::from_str(payload)?;
                    assert_eq!(ice.consumer_session_id, "stable-consumer-id");
                    let connection = self.connection.lock().unwrap().clone().unwrap();
                    for candidate in ice.candidates {
                        connection
                            .add_ice_candidate(candidate.to_rtc_candidate().to_json().unwrap())
                            .await
                            .unwrap();
                    }
                    let channel_rx = self.channel_rx.lock().await.take().unwrap();
                    tokio::spawn(async move {
                        let channel = channel_rx.await.unwrap();
                        tokio::time::timeout(Duration::from_secs(5), async {
                            while channel.ready_state() != RTCDataChannelState::Open {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                        channel
                            .send(&Bytes::from(
                                serde_json::to_vec(&UnboundedPacket::new(
                                    "test-egress-path",
                                    b"quic packet",
                                ))
                                .unwrap(),
                            ))
                            .await
                            .unwrap();
                        channel.close().await.unwrap();
                    });
                    Ok(None)
                }
                _ => panic!("unexpected signaling message {kind:?}"),
            }
        }
    }

    #[async_trait]
    impl ConsumerSignaler for PeerSignaler {
        async fn advertisements(&self) -> Result<Box<dyn AdvertisementSource>, SignalingError> {
            Ok(Box::new(SingleAdvertisement(Some(SignalMessage {
                reply_to: "genesis-request".into(),
                kind: SignalMessageType::Genesis,
                payload: "{}".into(),
            }))))
        }
    }

    #[test]
    fn serializes_pion_compatible_candidate_protocol_numbers() {
        let candidate = RTCIceCandidate {
            stats_id: String::new(),
            foundation: "1234".into(),
            priority: 1_694_498_815,
            address: "203.0.113.8".into(),
            protocol: RTCIceProtocol::Udp,
            port: 54_321,
            typ: RTCIceCandidateType::Srflx,
            component: 1,
            related_address: "192.168.1.20".into(),
            related_port: 60_000,
            tcp_type: String::new(),
        };
        let encoded = serde_json::to_value(ConsumerIceCandidate::from(&candidate)).unwrap();
        assert_eq!(encoded["protocol"], 1);
        assert_eq!(encoded["type"], "srflx");
        assert_eq!(encoded["relatedAddress"], "192.168.1.20");
        assert!(encoded.get("sdpMid").is_none());
    }

    #[tokio::test]
    async fn completes_consumer_signaling_and_opens_a_peer_path() {
        let signaler = Arc::new(PeerSignaler::new());
        let socket = VirtualUdpSocket::new("100.64.0.1:7000".parse().unwrap());
        let mut config = ConsumerConfig::new(
            signaler,
            socket,
            Arc::new(SyntheticPathAllocator::new()),
            "stable-consumer-id",
        );
        config.patience = Duration::ZERO;
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            run_consumer_session(config, CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            outcome.remote,
            "100.64.0.2:443".parse::<std::net::SocketAddr>().unwrap()
        );
        assert_eq!(outcome.path_end, ConsumerPathEnd::PeerClosed);
    }
}
