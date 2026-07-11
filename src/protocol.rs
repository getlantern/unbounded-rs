use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

pub const PROTOCOL_VERSION: &str = "v2.3.0";
pub const SUBPROTOCOL_MAGIC_COOKIE: &str = "un80und3d";
pub const VERSION_HEADER: &str = "X-BF-Version";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnboundedPacket {
    #[serde(rename = "SourceAddr")]
    pub source_addr: String,
    #[serde(rename = "Payload", with = "base64_payload")]
    pub payload: Vec<u8>,
}

impl UnboundedPacket {
    pub fn new(source_addr: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            source_addr: source_addr.into(),
            payload: payload.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum SignalMessageType {
    Genesis = 0,
    Offer = 1,
    Answer = 2,
    Ice = 3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalMessage {
    #[serde(rename = "ReplyTo")]
    pub reply_to: String,
    #[serde(rename = "Type")]
    pub kind: SignalMessageType,
    #[serde(rename = "Payload")]
    pub payload: String,
}

impl SignalMessage {
    pub fn decode_payload<T>(&self) -> Result<T, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_str(&self.payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    #[serde(rename = "Host")]
    pub host: String,
    #[serde(rename = "Distance")]
    pub distance: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathAssertion {
    #[serde(rename = "Allow")]
    pub allow: Option<Vec<Endpoint>>,
    #[serde(rename = "Deny")]
    pub deny: Option<Vec<Endpoint>>,
    #[serde(rename = "JITUnavailable")]
    pub jit_unavailable: bool,
}

impl PathAssertion {
    pub fn all_hosts_on_request() -> Self {
        Self {
            allow: Some(vec![Endpoint {
                host: "$".into(),
                distance: 1,
            }]),
            deny: None,
            jit_unavailable: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenesisMessage {
    #[serde(rename = "PathAssertion")]
    pub path_assertion: PathAssertion,
}

pub fn egress_subprotocols(csid: &str, version: &str) -> [String; 3] {
    [
        SUBPROTOCOL_MAGIC_COOKIE.to_owned(),
        csid.to_owned(),
        version.to_owned(),
    ]
}

pub fn parse_egress_subprotocols(values: &[impl AsRef<str>]) -> Option<(&str, &str)> {
    if values.len() != 3 {
        return None;
    }
    Some((values[1].as_ref(), values[2].as_ref()))
}

/// Returns true if `value` is safe to use as a single `Sec-WebSocket-Protocol`
/// token. The consumer session ID is remote-supplied, so it must not contain the
/// list separator (`,`) or any whitespace/control/non-visible byte that would
/// split the comma-delimited header into extra subprotocol tokens. Base64, hex,
/// and UUID session IDs (`/`, `+`, `=`, `-`, `_`) remain valid.
pub fn is_subprotocol_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b',')
}

mod base64_payload {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_matches_go_encoding_json() {
        let packet = UnboundedPacket::new("WebSocket connection test", [0, 1, 2, 255]);
        let encoded = serde_json::to_string(&packet).unwrap();
        assert_eq!(
            encoded,
            r#"{"SourceAddr":"WebSocket connection test","Payload":"AAEC/w=="}"#
        );
        assert_eq!(
            serde_json::from_str::<UnboundedPacket>(&encoded).unwrap(),
            packet
        );
    }

    #[test]
    fn signal_envelope_matches_go_encoding_json() {
        let message = SignalMessage {
            reply_to: "request-42".into(),
            kind: SignalMessageType::Ice,
            payload: r#"{"ConsumerSessionID":"abc"}"#.into(),
        };
        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            r#"{"ReplyTo":"request-42","Type":3,"Payload":"{\"ConsumerSessionID\":\"abc\"}"}"#
        );
    }

    #[test]
    fn genesis_matches_go_encoding_json() {
        let genesis = GenesisMessage {
            path_assertion: PathAssertion::all_hosts_on_request(),
        };
        assert_eq!(
            serde_json::to_string(&genesis).unwrap(),
            r#"{"PathAssertion":{"Allow":[{"Host":"$","Distance":1}],"Deny":null,"JITUnavailable":false}}"#
        );
    }

    #[test]
    fn egress_subprotocols_match_go_order() {
        let values = egress_subprotocols("session-id", PROTOCOL_VERSION);
        assert_eq!(values, ["un80und3d", "session-id", "v2.3.0"]);
        assert_eq!(
            parse_egress_subprotocols(&values),
            Some(("session-id", "v2.3.0"))
        );
    }

    #[test]
    fn parser_preserves_current_go_cookie_behavior() {
        let values = ["anything", "session-id", "v2.3.0"];
        assert_eq!(
            parse_egress_subprotocols(&values),
            Some(("session-id", "v2.3.0"))
        );
    }

    #[test]
    fn subprotocol_token_accepts_ids_but_rejects_list_injection() {
        assert!(is_subprotocol_token("consumer-session-id"));
        assert!(is_subprotocol_token("a1B2/c3+d4="));
        assert!(is_subprotocol_token("550e8400-e29b-41d4-a716-446655440000"));
        assert!(!is_subprotocol_token(""));
        assert!(!is_subprotocol_token("csid, injected"));
        assert!(!is_subprotocol_token("has space"));
        assert!(!is_subprotocol_token("tab\there"));
        assert!(!is_subprotocol_token("new\nline"));
    }
}
