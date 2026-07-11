use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::{APIBuilder, API};
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice::network_type::NetworkType;
use webrtc::peer_connection::RTCPeerConnection;

use crate::relay::{BoxTransportError, DatagramTransport};

pub const DATA_CHANNEL_LABEL: &str = "data";
pub const DEFAULT_DATA_CHANNEL_QUEUE: usize = 4096;

pub fn build_api() -> Result<API, webrtc::Error> {
    build_api_with_options(true, true)
}

pub fn build_api_with_ipv6(enable_ipv6: bool) -> Result<API, webrtc::Error> {
    build_api_with_options(enable_ipv6, true)
}

pub fn build_api_with_options(
    enable_ipv6: bool,
    randomize_dtls: bool,
) -> Result<API, webrtc::Error> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;
    let registry = register_default_interceptors(
        webrtc::interceptor::registry::Registry::new(),
        &mut media_engine,
    )?;
    let mut settings = SettingEngine::default();
    let mut network_types = vec![NetworkType::Udp4];
    if enable_ipv6 {
        network_types.push(NetworkType::Udp6);
    }
    settings.set_network_types(network_types);
    if randomize_dtls {
        settings.set_dtls_client_hello_message_hook(Arc::new(|mut hello| {
            let mut rng = rand::rng();
            let mut cipher_suites = hello.cipher_suites().to_vec();
            cipher_suites.shuffle(&mut rng);
            hello.set_cipher_suites(cipher_suites);
            let mut extensions = hello.extensions().to_vec();
            extensions.shuffle(&mut rng);
            hello.set_extensions(extensions);
            hello
        }));
    }
    Ok(APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_setting_engine(settings)
        .build())
}

#[derive(Debug, thiserror::Error)]
pub enum WebRtcDatagramError {
    #[error("WebRTC DataChannel sent text on the binary packet path")]
    TextMessage,
    #[error("WebRTC DataChannel failed: {0}")]
    WebRtc(#[from] webrtc::Error),
}

#[derive(Debug)]
enum ChannelEvent {
    Packet(Bytes),
    Text,
    Closed,
}

pub struct WebRtcDatagrams {
    channel: Arc<RTCDataChannel>,
    events: mpsc::Receiver<ChannelEvent>,
}

impl WebRtcDatagrams {
    pub fn new(channel: Arc<RTCDataChannel>) -> Self {
        Self::with_capacity(channel, DEFAULT_DATA_CHANNEL_QUEUE)
    }

    pub fn with_capacity(channel: Arc<RTCDataChannel>, capacity: usize) -> Self {
        assert!(capacity > 0, "DataChannel queue capacity must be non-zero");
        let (events_tx, events) = mpsc::channel(capacity);
        let message_tx = events_tx.clone();
        channel.on_message(Box::new(move |message: DataChannelMessage| {
            let message_tx = message_tx.clone();
            Box::pin(async move {
                let event = if message.is_string {
                    ChannelEvent::Text
                } else {
                    ChannelEvent::Packet(message.data)
                };
                // Congestion at this boundary is packet loss. Blocking the WebRTC callback would
                // couple SCTP backpressure to QUIC and risks head-of-line blocking.
                let _ = message_tx.try_send(event);
            })
        }));
        channel.on_close(Box::new(move || {
            let events_tx = events_tx.clone();
            Box::pin(async move {
                let _ = events_tx.send(ChannelEvent::Closed).await;
            })
        }));
        Self { channel, events }
    }

    pub fn channel(&self) -> &Arc<RTCDataChannel> {
        &self.channel
    }
}

pub async fn create_unreliable_data_channel(
    connection: &RTCPeerConnection,
) -> Result<Arc<RTCDataChannel>, webrtc::Error> {
    connection
        .create_data_channel(
            DATA_CHANNEL_LABEL,
            Some(RTCDataChannelInit {
                ordered: Some(false),
                max_retransmits: Some(0),
                ..Default::default()
            }),
        )
        .await
}

#[async_trait]
impl DatagramTransport for WebRtcDatagrams {
    async fn send_packet(&mut self, packet: Bytes) -> Result<(), BoxTransportError> {
        self.channel
            .send(&packet)
            .await
            .map(|_| ())
            .map_err(|error| Box::new(WebRtcDatagramError::WebRtc(error)) as BoxTransportError)
    }

    async fn recv_packet(&mut self) -> Result<Option<Bytes>, BoxTransportError> {
        match self.events.recv().await {
            Some(ChannelEvent::Packet(packet)) => Ok(Some(packet)),
            Some(ChannelEvent::Text) => {
                Err(Box::new(WebRtcDatagramError::TextMessage) as BoxTransportError)
            }
            Some(ChannelEvent::Closed) | None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;
    use webrtc::data_channel::data_channel_state::RTCDataChannelState;
    use webrtc::peer_connection::configuration::RTCConfiguration;

    use super::*;

    async fn signal_pair(offer: &RTCPeerConnection, answer: &RTCPeerConnection) {
        let offer_sdp = offer.create_offer(None).await.unwrap();
        let mut offer_gathering = offer.gathering_complete_promise().await;
        offer.set_local_description(offer_sdp).await.unwrap();
        let _ = offer_gathering.recv().await;
        answer
            .set_remote_description(offer.local_description().await.unwrap())
            .await
            .unwrap();

        let answer_sdp = answer.create_answer(None).await.unwrap();
        let mut answer_gathering = answer.gathering_complete_promise().await;
        answer.set_local_description(answer_sdp).await.unwrap();
        let _ = answer_gathering.recv().await;
        offer
            .set_remote_description(answer.local_description().await.unwrap())
            .await
            .unwrap();
    }

    async fn wait_until_open(channel: &RTCDataChannel) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while channel.ready_state() != RTCDataChannelState::Open {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("DataChannel did not open");
    }

    #[tokio::test]
    async fn unreliable_unordered_channel_relays_binary_datagrams() {
        let api = build_api().unwrap();
        let offer = api
            .new_peer_connection(RTCConfiguration::default())
            .await
            .unwrap();
        let answer = api
            .new_peer_connection(RTCConfiguration::default())
            .await
            .unwrap();

        let offer_channel = create_unreliable_data_channel(&offer).await.unwrap();
        assert!(!offer_channel.ordered());
        assert_eq!(offer_channel.max_retransmits(), Some(0));
        let mut offer_datagrams = WebRtcDatagrams::new(offer_channel.clone());

        let (answer_channel_tx, answer_channel_rx) = oneshot::channel();
        let answer_channel_tx = Arc::new(std::sync::Mutex::new(Some(answer_channel_tx)));
        answer.on_data_channel(Box::new(move |channel| {
            if channel.label() == DATA_CHANNEL_LABEL {
                if let Some(tx) = answer_channel_tx.lock().unwrap().take() {
                    let _ = tx.send(channel);
                }
            }
            Box::pin(async {})
        }));

        signal_pair(&offer, &answer).await;
        let answer_channel = answer_channel_rx.await.unwrap();
        assert!(!answer_channel.ordered());
        assert_eq!(answer_channel.max_retransmits(), Some(0));
        let mut answer_datagrams = WebRtcDatagrams::new(answer_channel.clone());
        wait_until_open(&offer_channel).await;
        wait_until_open(&answer_channel).await;

        offer_datagrams
            .send_packet(Bytes::from_static(b"consumer packet"))
            .await
            .unwrap();
        assert_eq!(
            answer_datagrams.recv_packet().await.unwrap(),
            Some(Bytes::from_static(b"consumer packet"))
        );

        answer_datagrams
            .send_packet(Bytes::from_static(b"egress packet"))
            .await
            .unwrap();
        assert_eq!(
            offer_datagrams.recv_packet().await.unwrap(),
            Some(Bytes::from_static(b"egress packet"))
        );

        offer.close().await.unwrap();
        answer.close().await.unwrap();
    }

    #[test]
    fn covert_dtls_can_be_disabled_for_diagnostics() {
        assert!(build_api_with_options(false, false).is_ok());
    }
}
