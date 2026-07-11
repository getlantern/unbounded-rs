use lantern_unbounded::protocol::{egress_subprotocols, UnboundedPacket, PROTOCOL_VERSION};

fn main() {
    let packet = UnboundedPacket::new("WebSocket connection example", b"example".to_vec());
    println!("packet={}", serde_json::to_string(&packet).unwrap());
    println!(
        "subprotocols={}",
        egress_subprotocols("consumer-session-id", PROTOCOL_VERSION).join(",")
    );
}
