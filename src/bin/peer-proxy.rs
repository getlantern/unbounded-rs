use std::env;
use std::time::Duration;

use lantern_unbounded::peer_proxy::{run_peer_proxy, PeerProxyConfig};

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set"))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _ = env_logger::try_init();
    let stun_urls = env::var("UNBOUNDED_STUN_URLS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    let nat_timeout = env::var("UNBOUNDED_NAT_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);

    let outcome = run_peer_proxy(PeerProxyConfig {
        freddie_endpoint: required("UNBOUNDED_FREDDIE_ENDPOINT"),
        egress_url: required("UNBOUNDED_EGRESS_URL"),
        stun_urls,
        nat_timeout: Duration::from_secs(nat_timeout),
        enable_ipv6: env::var("UNBOUNDED_ENABLE_IPV6").is_ok_and(|value| value == "1"),
    })
    .await
    .unwrap_or_else(|error| panic!("peer proxy failed: {error:#}"));

    println!(
        "peer proxy session {} ended: {:?}",
        outcome.consumer_session_id, outcome.relay_end
    );
}
