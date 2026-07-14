use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use webrtc::ice_transport::ice_candidate::RTCIceCandidate;
use webrtc::ice_transport::ice_candidate_pair::RTCIceCandidatePair;
use webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType;
use webrtc::ice_transport::ice_protocol::RTCIceProtocol;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::egress::{EgressError, EgressTunnel};
use crate::protocol::{is_subprotocol_token, GenesisMessage, PathAssertion, SignalMessageType};
use crate::relay::{relay, RelayEnd, RelayError};
use crate::rtc::{build_api_with_options, WebRtcDatagrams, DATA_CHANNEL_LABEL};
use crate::signaling::{Signaler, SignalingError};
use crate::supervisor::SupervisorEvent;

type DataChannelWaitSender<T> =
    Arc<Mutex<Option<oneshot::Sender<Result<T, RTCPeerConnectionState>>>>>;

#[derive(Debug, Clone)]
pub struct PeerProxyConfig {
    pub signaler: Arc<dyn Signaler>,
    pub egress_url: String,
    pub stun_urls: Vec<String>,
    pub nat_timeout: Duration,
    pub enable_ipv6: bool,
    pub randomize_dtls: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerProxyOutcome {
    pub consumer_session_id: String,
    pub relay_end: RelayEnd,
    pub relay_duration: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum PeerProxyError {
    #[error("Freddie signaling failed: {0}")]
    Signaling(#[from] SignalingError),
    #[error("WebRTC failed: {0}")]
    WebRtc(#[from] webrtc::Error),
    #[error("invalid {step} signaling payload: {source}")]
    Decode {
        step: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("Freddie returned no {0} signaling response")]
    MissingResponse(&'static str),
    #[error("Freddie returned {actual:?} while {expected:?} was required")]
    UnexpectedSignal {
        expected: SignalMessageType,
        actual: SignalMessageType,
    },
    #[error("consumer supplied no session ID")]
    MissingConsumerSessionId,
    #[error("consumer supplied a malformed session ID")]
    InvalidConsumerSessionId,
    #[error("consumer supplied an invalid ICE candidate: {0}")]
    InvalidIceCandidate(#[source] webrtc::Error),
    #[error("timed out waiting for the consumer WebRTC DataChannel")]
    NatTimeout,
    #[error("consumer WebRTC DataChannel callback ended before opening the channel")]
    DataChannelClosed,
    #[error("consumer WebRTC connection became {0} before opening a DataChannel")]
    PeerConnectionEnded(RTCPeerConnectionState),
    #[error("egress tunnel failed: {0}")]
    Egress(#[from] EgressError),
    #[error("packet relay failed after {relay_duration:?}: {source}")]
    Relay {
        relay_duration: Duration,
        #[source]
        source: RelayError,
    },
    #[error("peer proxy session cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OfferMessage {
    #[serde(rename = "SDP")]
    sdp: RTCSessionDescription,
    #[serde(rename = "Tag")]
    tag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IceMessage {
    #[serde(rename = "Candidates")]
    candidates: Vec<PionIceCandidate>,
    #[serde(rename = "ConsumerSessionID")]
    consumer_session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PionIceCandidate {
    foundation: String,
    priority: u32,
    address: String,
    protocol: u8,
    port: u16,
    #[serde(rename = "type")]
    typ: RTCIceCandidateType,
    component: u16,
    #[serde(rename = "relatedAddress")]
    related_address: String,
    #[serde(rename = "relatedPort")]
    related_port: u16,
    #[serde(rename = "tcpType")]
    tcp_type: String,
    #[serde(rename = "sdpMid", default)]
    sdp_mid: Option<String>,
    #[serde(rename = "sdpMLineIndex", default)]
    sdp_mline_index: Option<u16>,
}

impl PionIceCandidate {
    fn to_rust_candidate(&self) -> RTCIceCandidate {
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

pub async fn run_peer_proxy(config: PeerProxyConfig) -> Result<PeerProxyOutcome, PeerProxyError> {
    run_peer_proxy_until_cancelled(config, CancellationToken::new(), None).await
}

pub async fn run_peer_proxy_until_cancelled(
    config: PeerProxyConfig,
    cancellation: CancellationToken,
    events: Option<mpsc::UnboundedSender<SupervisorEvent>>,
) -> Result<PeerProxyOutcome, PeerProxyError> {
    let api = build_api_with_options(config.enable_ipv6, config.randomize_dtls)?;
    let ice_servers = if config.stun_urls.is_empty() {
        Vec::new()
    } else {
        vec![RTCIceServer {
            urls: config.stun_urls,
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

    let (data_channel_tx, data_channel_rx) = oneshot::channel();
    let data_channel_tx = Arc::new(Mutex::new(Some(data_channel_tx)));
    let state_data_channel_tx = data_channel_tx.clone();

    // The consumer session ID is not known until the ICE signaling message is
    // decoded, which happens after this handler is registered. It is always set
    // before ICE candidates are added, so it is guaranteed present by the time
    // the connection can reach Connected. `PeerConnected`/`PeerDisconnected` are
    // only emitted once the ID is known.
    let session_id: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    // Guards emitting exactly one `PeerDisconnected` per connected session.
    let disconnect_emitted = Arc::new(AtomicBool::new(false));
    // Only emit `PeerDisconnected` for a session that actually reached `Connected`,
    // so a session that fails straight to Failed/Closed emits nothing.
    let connect_emitted = Arc::new(AtomicBool::new(false));

    // Hold a `Weak` here, not a strong `Arc`: a strong clone captured by the
    // state-change callback (which the connection itself owns) forms a reference
    // cycle that leaks the whole `RTCPeerConnection` — and keeps the `events`
    // sender clone below alive — for every session, hanging pool shutdown.
    let state_connection = Arc::downgrade(&connection);
    let state_events = events.clone();
    let state_session_id = session_id.clone();
    let state_disconnect_emitted = disconnect_emitted.clone();
    let state_connect_emitted = connect_emitted.clone();
    connection.on_peer_connection_state_change(Box::new(move |state| {
        fail_data_channel_wait(state, &state_data_channel_tx);
        let connection = state_connection.clone();
        let events = state_events.clone();
        let session_id = state_session_id.clone();
        let disconnect_emitted = state_disconnect_emitted.clone();
        let connect_emitted = state_connect_emitted.clone();
        Box::pin(async move {
            let Some(events) = events else {
                return;
            };
            // Convention: no unwrap() outside tests. A poisoned lock (only if a
            // holder panicked) is treated like "id not yet known" — skip the event.
            // The guard is scoped to this block so it is dropped before the await
            // below — a MutexGuard is !Send and must not live across an await point.
            let session_id = {
                let Ok(guard) = session_id.lock() else {
                    return;
                };
                let Some(id) = guard.clone() else {
                    return;
                };
                id
            };
            match state {
                RTCPeerConnectionState::Connected => {
                    connect_emitted.store(true, Ordering::SeqCst);
                    // The connection is normally still alive here; read the selected
                    // pair when it is, otherwise fall back to `None` (remote unknown).
                    let selected_pair = match connection.upgrade() {
                        Some(connection) => selected_candidate_pair(&connection).await,
                        None => None,
                    };
                    let _ = events.send(peer_connected_event(session_id, selected_pair.as_ref()));
                }
                // Emit a disconnect only for a session that reached Connected, and
                // only on the first terminal transition (Disconnected → Failed →
                // Closed yields one event).
                state
                    if is_disconnect_state(state)
                        && connect_emitted.load(Ordering::SeqCst)
                        && !disconnect_emitted.swap(true, Ordering::SeqCst) =>
                {
                    let _ = events.send(peer_disconnected_event(session_id));
                }
                _ => {}
            }
        })
    }));
    connection.on_data_channel(Box::new(move |channel| {
        if channel.label() == DATA_CHANNEL_LABEL {
            let datagrams = Arc::new(Mutex::new(Some(WebRtcDatagrams::new(channel.clone()))));
            let data_channel_tx = data_channel_tx.clone();
            channel.on_open(Box::new(move || {
                let tx = data_channel_tx.lock().unwrap().take();
                let datagrams = datagrams.lock().unwrap().take();
                if let (Some(tx), Some(datagrams)) = (tx, datagrams) {
                    let _ = tx.send(Ok(datagrams));
                }
                Box::pin(async {})
            }));
        }
        Box::pin(async {})
    }));

    let session = async {
        let genesis = serde_json::to_string(&GenesisMessage {
            path_assertion: PathAssertion::all_hosts_on_request(),
        })
        .map_err(SignalingError::Decode)?;
        let offer_signal = config
            .signaler
            .exchange("genesis", SignalMessageType::Genesis, &genesis)
            .await?
            .ok_or(PeerProxyError::MissingResponse("offer"))?;
        expect_signal(&offer_signal, SignalMessageType::Offer)?;
        let offer: OfferMessage =
            offer_signal
                .decode_payload()
                .map_err(|source| PeerProxyError::Decode {
                    step: "offer",
                    source,
                })?;
        connection.set_remote_description(offer.sdp).await?;

        let answer = connection.create_answer(None).await?;
        let mut gathering_complete = connection.gathering_complete_promise().await;
        connection.set_local_description(answer).await?;
        let _ = gathering_complete.recv().await;
        let final_answer = connection
            .local_description()
            .await
            .ok_or(PeerProxyError::MissingResponse("local ICE gathering"))?;

        let final_answer = serde_json::to_string(&final_answer).map_err(SignalingError::Decode)?;
        let ice_signal = config
            .signaler
            .exchange(
                &offer_signal.reply_to,
                SignalMessageType::Answer,
                &final_answer,
            )
            .await?
            .ok_or(PeerProxyError::MissingResponse("consumer ICE candidates"))?;
        expect_signal(&ice_signal, SignalMessageType::Ice)?;
        let ice: IceMessage =
            ice_signal
                .decode_payload()
                .map_err(|source| PeerProxyError::Decode {
                    step: "ICE",
                    source,
                })?;
        if ice.consumer_session_id.is_empty() {
            return Err(PeerProxyError::MissingConsumerSessionId);
        }
        // The session ID is peer-supplied and flows into emitted peer-state events
        // (and is logged by bin/peer-proxy). Reject anything that isn't a clean
        // subprotocol token before it can be published, to prevent log/telemetry
        // injection via commas, whitespace, or control bytes.
        if !is_subprotocol_token(&ice.consumer_session_id) {
            return Err(PeerProxyError::InvalidConsumerSessionId);
        }
        // Publish the session ID before adding candidates so the peer-connection
        // state-change handler can label its events once ICE completes.
        *session_id.lock().unwrap() = Some(ice.consumer_session_id.clone());

        for candidate in &ice.candidates {
            let mut init = candidate
                .to_rust_candidate()
                .to_json()
                .map_err(PeerProxyError::InvalidIceCandidate)?;
            init.sdp_mid = candidate.sdp_mid.clone();
            init.sdp_mline_index = candidate.sdp_mline_index;
            connection.add_ice_candidate(init).await?;
        }

        let mut peer = tokio::time::timeout(config.nat_timeout, data_channel_rx)
            .await
            .map_err(|_| PeerProxyError::NatTimeout)?
            .map_err(|_| PeerProxyError::DataChannelClosed)?
            .map_err(PeerProxyError::PeerConnectionEnded)?;
        let mut egress =
            EgressTunnel::connect(&config.egress_url, &ice.consumer_session_id).await?;
        let relay_started = tokio::time::Instant::now();
        let relay_end =
            relay(&mut peer, &mut egress)
                .await
                .map_err(|source| PeerProxyError::Relay {
                    relay_duration: relay_started.elapsed(),
                    source,
                })?;
        Ok(PeerProxyOutcome {
            consumer_session_id: ice.consumer_session_id,
            relay_end,
            relay_duration: relay_started.elapsed(),
        })
    };

    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(PeerProxyError::Cancelled),
        result = session => result,
    };

    let _ = connection.close().await;
    result
}

fn is_terminal_peer_state(state: RTCPeerConnectionState) -> bool {
    matches!(
        state,
        RTCPeerConnectionState::Disconnected
            | RTCPeerConnectionState::Failed
            | RTCPeerConnectionState::Closed
    )
}

/// States that mean a previously connected peer has gone away. Kept as a named
/// seam so the exactly-once disconnect logic can be unit-tested in isolation.
fn is_disconnect_state(state: RTCPeerConnectionState) -> bool {
    is_terminal_peer_state(state)
}

/// Reads the connected peer's address from a selected ICE candidate pair's
/// remote candidate. Returns `None` when the address is not an IP literal (e.g.
/// an mDNS `.local` hostname).
fn socket_addr_from_candidate(candidate: &RTCIceCandidate) -> Option<SocketAddr> {
    let ip = candidate.address.parse().ok()?;
    Some(SocketAddr::new(ip, candidate.port))
}

/// Builds the `PeerConnected` event for a session, extracting the remote address
/// from the selected candidate pair when one is available.
fn peer_connected_event(
    session_id: String,
    selected_pair: Option<&RTCIceCandidatePair>,
) -> SupervisorEvent {
    let remote = selected_pair.and_then(|pair| socket_addr_from_candidate(&pair.remote));
    SupervisorEvent::PeerConnected { session_id, remote }
}

/// Builds the `PeerDisconnected` event for a session.
fn peer_disconnected_event(session_id: String) -> SupervisorEvent {
    SupervisorEvent::PeerDisconnected { session_id }
}

/// Reads the selected ICE candidate pair for a peer connection, walking
/// SCTP → DTLS → ICE. Returns `None` when any layer is unavailable.
async fn selected_candidate_pair(connection: &RTCPeerConnection) -> Option<RTCIceCandidatePair> {
    connection
        .sctp()
        .transport()
        .ice_transport()
        .get_selected_candidate_pair()
        .await
}

fn fail_data_channel_wait<T>(state: RTCPeerConnectionState, sender: &DataChannelWaitSender<T>) {
    if is_terminal_peer_state(state) {
        if let Some(tx) = sender.lock().unwrap().take() {
            let _ = tx.send(Err(state));
        }
    }
}

impl PeerProxyError {
    pub fn relay_duration(&self) -> Option<Duration> {
        match self {
            Self::Relay { relay_duration, .. } => Some(*relay_duration),
            _ => None,
        }
    }
}

fn expect_signal(
    signal: &crate::protocol::SignalMessage,
    expected: SignalMessageType,
) -> Result<(), PeerProxyError> {
    if signal.kind == expected {
        Ok(())
    } else {
        Err(PeerProxyError::UnexpectedSignal {
            expected,
            actual: signal.kind,
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "reqwest-client")]
    use axum::http::StatusCode;
    #[cfg(feature = "reqwest-client")]
    use axum::routing::post;
    #[cfg(feature = "reqwest-client")]
    use axum::Router;

    use super::*;
    #[cfg(feature = "reqwest-client")]
    use crate::signaling::FreddieClient;
    use webrtc::ice_transport::ice_candidate_pair::RTCIceCandidatePair;

    #[test]
    fn decodes_pion_candidate_and_builds_rust_candidate_init() {
        let raw = r#"{
            "foundation":"1234",
            "priority":1694498815,
            "address":"203.0.113.8",
            "protocol":1,
            "port":54321,
            "type":"srflx",
            "component":1,
            "relatedAddress":"192.168.1.20",
            "relatedPort":60000,
            "tcpType":"",
            "sdpMid":"0",
            "sdpMLineIndex":0
        }"#;
        let candidate: PionIceCandidate = serde_json::from_str(raw).unwrap();
        let mut init = candidate.to_rust_candidate().to_json().unwrap();
        init.sdp_mid = candidate.sdp_mid.clone();
        init.sdp_mline_index = candidate.sdp_mline_index;

        assert!(init.candidate.starts_with("candidate:1234 1 udp"));
        assert!(init.candidate.contains("203.0.113.8 54321 typ srflx"));
        assert_eq!(init.sdp_mid.as_deref(), Some("0"));
        assert_eq!(init.sdp_mline_index, Some(0));
    }

    #[test]
    fn preserves_absent_pion_candidate_metadata() {
        let raw = r#"{
            "foundation":"1234",
            "priority":1694498815,
            "address":"203.0.113.8",
            "protocol":1,
            "port":54321,
            "type":"host",
            "component":1,
            "relatedAddress":"",
            "relatedPort":0,
            "tcpType":""
        }"#;
        let candidate: PionIceCandidate = serde_json::from_str(raw).unwrap();
        let mut init = candidate.to_rust_candidate().to_json().unwrap();
        init.sdp_mid = candidate.sdp_mid.clone();
        init.sdp_mline_index = candidate.sdp_mline_index;

        assert_eq!(init.sdp_mid, None);
        assert_eq!(init.sdp_mline_index, None);
    }

    fn test_candidate(address: &str, port: u16) -> RTCIceCandidate {
        RTCIceCandidate {
            address: address.to_string(),
            port,
            ..Default::default()
        }
    }

    #[test]
    fn reads_remote_socket_addr_from_ipv4_candidate() {
        let candidate = test_candidate("203.0.113.8", 54321);
        assert_eq!(
            socket_addr_from_candidate(&candidate),
            Some("203.0.113.8:54321".parse().unwrap())
        );
    }

    #[test]
    fn reads_remote_socket_addr_from_ipv6_candidate() {
        let candidate = test_candidate("2001:db8::1", 443);
        assert_eq!(
            socket_addr_from_candidate(&candidate),
            Some("[2001:db8::1]:443".parse().unwrap())
        );
    }

    #[test]
    fn unparseable_candidate_address_yields_no_remote() {
        // mDNS candidates carry a ".local" hostname rather than an IP literal.
        let candidate = test_candidate("abc-123.local", 54321);
        assert_eq!(socket_addr_from_candidate(&candidate), None);
    }

    #[test]
    fn peer_connected_event_carries_session_id_and_remote() {
        let pair = RTCIceCandidatePair::new(
            test_candidate("192.0.2.1", 3000),
            test_candidate("203.0.113.8", 54321),
        );
        let event = peer_connected_event("session-42".to_string(), Some(&pair));
        match event {
            SupervisorEvent::PeerConnected { session_id, remote } => {
                assert_eq!(session_id, "session-42");
                assert_eq!(remote, Some("203.0.113.8:54321".parse().unwrap()));
            }
            other => panic!("expected PeerConnected, got {other:?}"),
        }
    }

    #[test]
    fn peer_connected_event_without_pair_has_no_remote() {
        let event = peer_connected_event("session-42".to_string(), None);
        match event {
            SupervisorEvent::PeerConnected { session_id, remote } => {
                assert_eq!(session_id, "session-42");
                assert_eq!(remote, None);
            }
            other => panic!("expected PeerConnected, got {other:?}"),
        }
    }

    #[test]
    fn peer_disconnected_event_carries_session_id() {
        let event = peer_disconnected_event("session-42".to_string());
        match event {
            SupervisorEvent::PeerDisconnected { session_id } => {
                assert_eq!(session_id, "session-42");
            }
            other => panic!("expected PeerDisconnected, got {other:?}"),
        }
    }

    #[test]
    fn only_terminal_states_are_disconnects() {
        assert!(is_disconnect_state(RTCPeerConnectionState::Disconnected));
        assert!(is_disconnect_state(RTCPeerConnectionState::Failed));
        assert!(is_disconnect_state(RTCPeerConnectionState::Closed));
        assert!(!is_disconnect_state(RTCPeerConnectionState::Connected));
        assert!(!is_disconnect_state(RTCPeerConnectionState::Connecting));
        assert!(!is_disconnect_state(RTCPeerConnectionState::New));
    }

    #[test]
    fn identifies_terminal_peer_states() {
        assert!(is_terminal_peer_state(RTCPeerConnectionState::Disconnected));
        assert!(is_terminal_peer_state(RTCPeerConnectionState::Failed));
        assert!(is_terminal_peer_state(RTCPeerConnectionState::Closed));
        assert!(!is_terminal_peer_state(RTCPeerConnectionState::New));
        assert!(!is_terminal_peer_state(RTCPeerConnectionState::Connecting));
        assert!(!is_terminal_peer_state(RTCPeerConnectionState::Connected));
    }

    #[tokio::test]
    async fn terminal_peer_state_wakes_data_channel_wait() {
        let (tx, rx) = oneshot::channel::<Result<(), RTCPeerConnectionState>>();
        let tx = Arc::new(Mutex::new(Some(tx)));
        fail_data_channel_wait(RTCPeerConnectionState::Failed, &tx);
        assert_eq!(rx.await.unwrap(), Err(RTCPeerConnectionState::Failed));
    }

    #[test]
    fn rejects_unexpected_signaling_step() {
        let signal = crate::protocol::SignalMessage {
            reply_to: "request".into(),
            kind: SignalMessageType::Answer,
            payload: "{}".into(),
        };
        assert!(matches!(
            expect_signal(&signal, SignalMessageType::Offer),
            Err(PeerProxyError::UnexpectedSignal { .. })
        ));
    }

    #[tokio::test]
    #[cfg(feature = "reqwest-client")]
    async fn cancellation_interrupts_signaling_and_closes_the_session() {
        let (request_tx, request_rx) = oneshot::channel();
        let request_tx = Arc::new(Mutex::new(Some(request_tx)));
        let app = Router::new().route(
            "/v1/signal",
            post(move || {
                let request_tx = request_tx.clone();
                async move {
                    if let Some(tx) = request_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    std::future::pending::<()>().await;
                    StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/signal", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let cancellation = CancellationToken::new();
        let session = tokio::spawn(run_peer_proxy_until_cancelled(
            PeerProxyConfig {
                signaler: Arc::new(FreddieClient::new(endpoint).unwrap()),
                egress_url: "ws://127.0.0.1:1/ws".into(),
                stun_urls: Vec::new(),
                nat_timeout: Duration::from_secs(1),
                enable_ipv6: false,
                randomize_dtls: false,
            },
            cancellation.clone(),
            None,
        ));
        tokio::time::timeout(Duration::from_secs(2), request_rx)
            .await
            .expect("Freddie request was not started")
            .unwrap();
        cancellation.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .expect("cancelled session did not stop")
            .unwrap();
        assert!(matches!(result, Err(PeerProxyError::Cancelled)));
    }
}
