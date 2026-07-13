use std::fmt::Debug;
#[cfg(feature = "reqwest-client")]
use std::time::Duration;

use async_trait::async_trait;
#[cfg(feature = "reqwest-client")]
use reqwest::{Client, StatusCode, Url};
#[cfg(feature = "reqwest-client")]
use serde::Serialize;

use crate::protocol::{SignalMessage, SignalMessageType};
#[cfg(feature = "reqwest-client")]
use crate::protocol::{PROTOCOL_VERSION, VERSION_HEADER};

#[cfg(feature = "reqwest-client")]
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum SignalingError {
    #[cfg(feature = "reqwest-client")]
    #[error("invalid Freddie endpoint: {0}")]
    InvalidEndpoint(#[from] url::ParseError),
    #[cfg(feature = "reqwest-client")]
    #[error("Freddie request failed: {0}")]
    NativeTransport(#[from] reqwest::Error),
    #[error("Freddie request failed: {0}")]
    Transport(String),
    #[error("Freddie rejected protocol version {0}")]
    ProtocolVersion(String),
    #[error("Freddie signaling recipient is no longer available")]
    RecipientGone,
    #[error("Freddie returned HTTP {0}")]
    Http(u16),
    #[error("invalid signaling JSON: {0}")]
    Decode(#[from] serde_json::Error),
}

#[async_trait]
pub trait Signaler: Send + Sync + Debug {
    async fn exchange(
        &self,
        send_to: &str,
        kind: SignalMessageType,
        payload: &str,
    ) -> Result<Option<SignalMessage>, SignalingError>;
}

#[async_trait]
pub trait AdvertisementSource: Send {
    async fn next(&mut self) -> Result<Option<SignalMessage>, SignalingError>;
}

#[async_trait]
pub trait ConsumerSignaler: Signaler {
    async fn advertisements(&self) -> Result<Box<dyn AdvertisementSource>, SignalingError>;
}

#[cfg(feature = "reqwest-client")]
#[derive(Debug, Clone)]
pub struct FreddieClient {
    client: Client,
    endpoint: Url,
    version: String,
}

#[cfg(feature = "reqwest-client")]
#[derive(Debug)]
pub struct AdvertisementStream {
    response: reqwest::Response,
    buffered: Vec<u8>,
}

#[cfg(feature = "reqwest-client")]
impl FreddieClient {
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self, SignalingError> {
        let client = Client::builder()
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .build()?;
        Self::with_client(client, endpoint, PROTOCOL_VERSION)
    }

    pub fn with_client(
        client: Client,
        endpoint: impl AsRef<str>,
        version: impl Into<String>,
    ) -> Result<Self, SignalingError> {
        Ok(Self {
            client,
            endpoint: Url::parse(endpoint.as_ref())?,
            version: version.into(),
        })
    }

    pub async fn exchange_json<T>(
        &self,
        send_to: &str,
        kind: SignalMessageType,
        payload: &T,
    ) -> Result<Option<SignalMessage>, SignalingError>
    where
        T: Serialize + ?Sized,
    {
        self.exchange(send_to, kind, &serde_json::to_string(payload)?)
            .await
    }

    pub async fn exchange(
        &self,
        send_to: &str,
        kind: SignalMessageType,
        payload: &str,
    ) -> Result<Option<SignalMessage>, SignalingError> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(VERSION_HEADER, &self.version)
            .form(&[
                ("data", payload),
                ("send-to", send_to),
                ("type", &(kind as u8).to_string()),
            ])
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {}
            StatusCode::IM_A_TEAPOT => {
                return Err(SignalingError::ProtocolVersion(self.version.clone()))
            }
            StatusCode::NOT_FOUND => return Err(SignalingError::RecipientGone),
            status => return Err(SignalingError::Http(status.as_u16())),
        }

        let body = response.bytes().await?;
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&body)?))
    }

    pub async fn advertisements(&self) -> Result<AdvertisementStream, SignalingError> {
        let response = self
            .client
            .get(self.endpoint.clone())
            .header(VERSION_HEADER, &self.version)
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => Ok(AdvertisementStream {
                response,
                buffered: Vec::new(),
            }),
            StatusCode::IM_A_TEAPOT => Err(SignalingError::ProtocolVersion(self.version.clone())),
            status => Err(SignalingError::Http(status.as_u16())),
        }
    }
}

#[cfg(feature = "reqwest-client")]
#[async_trait]
impl Signaler for FreddieClient {
    async fn exchange(
        &self,
        send_to: &str,
        kind: SignalMessageType,
        payload: &str,
    ) -> Result<Option<SignalMessage>, SignalingError> {
        FreddieClient::exchange(self, send_to, kind, payload).await
    }
}

#[cfg(feature = "reqwest-client")]
#[async_trait]
impl ConsumerSignaler for FreddieClient {
    async fn advertisements(&self) -> Result<Box<dyn AdvertisementSource>, SignalingError> {
        Ok(Box::new(FreddieClient::advertisements(self).await?))
    }
}

#[cfg(feature = "reqwest-client")]
impl AdvertisementStream {
    pub async fn next(&mut self) -> Result<Option<SignalMessage>, SignalingError> {
        loop {
            if let Some(newline) = self.buffered.iter().position(|byte| *byte == b'\n') {
                let mut line = self.buffered.drain(..=newline).collect::<Vec<_>>();
                line.pop();
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                return Ok(Some(serde_json::from_slice(&line)?));
            }

            match self.response.chunk().await? {
                Some(chunk) => self.buffered.extend_from_slice(&chunk),
                None if self.buffered.iter().all(u8::is_ascii_whitespace) => return Ok(None),
                None => {
                    let line = std::mem::take(&mut self.buffered);
                    return Ok(Some(serde_json::from_slice(&line)?));
                }
            }
        }
    }
}

#[cfg(feature = "reqwest-client")]
#[async_trait]
impl AdvertisementSource for AdvertisementStream {
    async fn next(&mut self) -> Result<Option<SignalMessage>, SignalingError> {
        AdvertisementStream::next(self).await
    }
}

#[cfg(all(test, feature = "reqwest-client"))]
mod tests {
    use std::collections::HashMap;

    use axum::extract::Form;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::Router;
    use tokio::sync::oneshot;

    use super::*;
    use crate::protocol::{GenesisMessage, PathAssertion};

    async fn spawn_freddie_stub(
        status: StatusCode,
        body: &'static str,
    ) -> (
        String,
        oneshot::Receiver<(HeaderMap, HashMap<String, String>)>,
    ) {
        let (captured_tx, captured_rx) = oneshot::channel();
        let captured_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(captured_tx)));
        let app = Router::new().route(
            "/v1/signal",
            post({
                let captured_tx = captured_tx.clone();
                move |headers: HeaderMap, Form(form): Form<HashMap<String, String>>| {
                    let captured_tx = captured_tx.clone();
                    async move {
                        if let Some(tx) = captured_tx.lock().unwrap().take() {
                            let _ = tx.send((headers, form));
                        }
                        (status, body).into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/v1/signal"), captured_rx)
    }

    async fn spawn_advertisement_stub(body: &'static str) -> String {
        let app = Router::new().route(
            "/v1/signal",
            axum::routing::get(move |headers: HeaderMap| async move {
                assert_eq!(headers[VERSION_HEADER], PROTOCOL_VERSION);
                (StatusCode::OK, body)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/v1/signal")
    }

    #[tokio::test]
    async fn posts_the_go_form_and_decodes_the_reply() {
        let body = "{\"ReplyTo\":\"offer-request\",\"Type\":1,\"Payload\":\"{\\\"SDP\\\":{}}\"}\n";
        let (endpoint, captured) = spawn_freddie_stub(StatusCode::OK, body).await;
        let client = FreddieClient::new(endpoint).unwrap();
        let reply = client
            .exchange_json(
                "genesis",
                SignalMessageType::Genesis,
                &GenesisMessage {
                    path_assertion: PathAssertion::all_hosts_on_request(),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(reply.kind, SignalMessageType::Offer);
        assert_eq!(reply.reply_to, "offer-request");
        let (headers, form) = captured.await.unwrap();
        assert_eq!(headers[VERSION_HEADER], PROTOCOL_VERSION);
        assert_eq!(form["send-to"], "genesis");
        assert_eq!(form["type"], "0");
        assert_eq!(
            form["data"],
            r#"{"PathAssertion":{"Allow":[{"Host":"$","Distance":1}],"Deny":null,"JITUnavailable":false}}"#
        );
    }

    #[tokio::test]
    async fn maps_freddie_status_codes() {
        let (endpoint, _) = spawn_freddie_stub(StatusCode::IM_A_TEAPOT, "418\n").await;
        let error = FreddieClient::new(endpoint)
            .unwrap()
            .exchange("genesis", SignalMessageType::Genesis, "{}")
            .await
            .unwrap_err();
        assert!(matches!(error, SignalingError::ProtocolVersion(_)));

        let (endpoint, _) = spawn_freddie_stub(StatusCode::NOT_FOUND, "404\n").await;
        let error = FreddieClient::new(endpoint)
            .unwrap()
            .exchange("missing", SignalMessageType::Offer, "{}")
            .await
            .unwrap_err();
        assert!(matches!(error, SignalingError::RecipientGone));
    }

    #[tokio::test]
    async fn preserves_native_transport_error_type() {
        let error = FreddieClient::new("http://127.0.0.1:0/v1/signal")
            .unwrap()
            .exchange("genesis", SignalMessageType::Genesis, "{}")
            .await
            .unwrap_err();
        assert!(matches!(error, SignalingError::NativeTransport(_)));
    }

    #[tokio::test]
    async fn empty_success_body_is_a_timeout_without_a_reply() {
        let (endpoint, _) = spawn_freddie_stub(StatusCode::OK, "\n").await;
        let reply = FreddieClient::new(endpoint)
            .unwrap()
            .exchange("request", SignalMessageType::Ice, "{}")
            .await
            .unwrap();
        assert!(reply.is_none());
    }

    #[tokio::test]
    async fn reads_newline_delimited_genesis_advertisements() {
        let endpoint = spawn_advertisement_stub(concat!(
            "{\"ReplyTo\":\"one\",\"Type\":0,\"Payload\":\"{}\"}\n",
            "{\"ReplyTo\":\"two\",\"Type\":0,\"Payload\":\"{}\"}\n"
        ))
        .await;
        let mut stream = FreddieClient::new(endpoint)
            .unwrap()
            .advertisements()
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().reply_to, "one");
        assert_eq!(stream.next().await.unwrap().unwrap().reply_to, "two");
        assert!(stream.next().await.unwrap().is_none());
    }
}
