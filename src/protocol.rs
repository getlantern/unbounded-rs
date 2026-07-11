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
}
