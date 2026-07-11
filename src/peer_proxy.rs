use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use webrtc::ice_transport::ice_candidate::RTCIceCandidate;
use webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType;
use webrtc::ice_transport::ice_protocol::RTCIceProtocol;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::egress::{EgressError, EgressTunnel};
use crate::protocol::{GenesisMessage, PathAssertion, SignalMessageType};
use crate::relay::{relay, RelayEnd, RelayError};
use crate::rtc::{build_api_with_ipv6, WebRtcDatagrams, DATA_CHANNEL_LABEL};
use crate::signaling::{FreddieClient, SignalingError};

#[derive(Debug, Clone)]
pub struct PeerProxyConfig {
    pub freddie_endpoint: String,
    pub egress_url: String,
    pub stun_urls: Vec<String>,
    pub nat_timeout: Duration,
    pub enable_ipv6: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerProxyOutcome {
    pub consumer_session_id: String,
    pub relay_end: RelayEnd,
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
    #[error("consumer supplied an invalid ICE candidate: {0}")]
    InvalidIceCandidate(#[source] webrtc::Error),
    #[error("timed out waiting for the consumer WebRTC DataChannel")]
    NatTimeout,
    #[error("consumer WebRTC connection closed before opening a DataChannel")]
    DataChannelClosed,
    #[error("egress tunnel failed: {0}")]
    Egress(#[from] EgressError),
    #[error("packet relay failed: {0}")]
    Relay(#[from] RelayError),
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
    sdp_mid: String,
    #[serde(rename = "sdpMLineIndex", default)]
    sdp_mline_index: u16,
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
    run_peer_proxy_until_cancelled(config, CancellationToken::new()).await
}

pub async fn run_peer_proxy_until_cancelled(
    config: PeerProxyConfig,
    cancellation: CancellationToken,
) -> Result<PeerProxyOutcome, PeerProxyError> {
    let freddie = FreddieClient::new(&config.freddie_endpoint)?;
    let api = build_api_with_ipv6(config.enable_ipv6)?;
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
    connection.on_data_channel(Box::new(move |channel| {
        if channel.label() == DATA_CHANNEL_LABEL {
            let datagrams = Arc::new(Mutex::new(Some(WebRtcDatagrams::new(channel.clone()))));
            let data_channel_tx = data_channel_tx.clone();
            channel.on_open(Box::new(move || {
                let tx = data_channel_tx.lock().unwrap().take();
                let datagrams = datagrams.lock().unwrap().take();
                if let (Some(tx), Some(datagrams)) = (tx, datagrams) {
                    let _ = tx.send(datagrams);
                }
                Box::pin(async {})
            }));
        }
        Box::pin(async {})
    }));

    let session = async {
        let offer_signal = freddie
            .exchange_json(
                "genesis",
                SignalMessageType::Genesis,
                &GenesisMessage {
                    path_assertion: PathAssertion::all_hosts_on_request(),
                },
            )
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

        let ice_signal = freddie
            .exchange_json(
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

        for candidate in &ice.candidates {
            let mut init = candidate
                .to_rust_candidate()
                .to_json()
                .map_err(PeerProxyError::InvalidIceCandidate)?;
            init.sdp_mid = Some(candidate.sdp_mid.clone());
            init.sdp_mline_index = Some(candidate.sdp_mline_index);
            connection.add_ice_candidate(init).await?;
        }

        let mut peer = tokio::time::timeout(config.nat_timeout, data_channel_rx)
            .await
            .map_err(|_| PeerProxyError::NatTimeout)?
            .map_err(|_| PeerProxyError::DataChannelClosed)?;
        let mut egress =
            EgressTunnel::connect(&config.egress_url, &ice.consumer_session_id).await?;
        let relay_end = relay(&mut peer, &mut egress).await?;
        Ok(PeerProxyOutcome {
            consumer_session_id: ice.consumer_session_id,
            relay_end,
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
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;

    use super::*;

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
        init.sdp_mid = Some(candidate.sdp_mid.clone());
        init.sdp_mline_index = Some(candidate.sdp_mline_index);

        assert!(init.candidate.starts_with("candidate:1234 1 udp"));
        assert!(init.candidate.contains("203.0.113.8 54321 typ srflx"));
        assert_eq!(init.sdp_mid.as_deref(), Some("0"));
        assert_eq!(init.sdp_mline_index, Some(0));
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
                freddie_endpoint: endpoint,
                egress_url: "ws://127.0.0.1:1/ws".into(),
                stun_urls: Vec::new(),
                nat_timeout: Duration::from_secs(1),
                enable_ipv6: false,
            },
            cancellation.clone(),
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
