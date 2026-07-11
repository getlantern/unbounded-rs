use std::env;
use std::sync::Arc;
use std::time::Duration;

use lantern_unbounded::peer_proxy::PeerProxyConfig;
use lantern_unbounded::signaling::FreddieClient;
use lantern_unbounded::supervisor::{
    supervise_peer_proxy_pool, PoolEvent, SupervisorConfig, SupervisorEvent,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set"))
}

fn seconds(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default),
    )
}

fn count(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
        .try_init();
    let stun_urls = env::var("UNBOUNDED_STUN_URLS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });

    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let reporter = tokio::spawn(async move {
        while let Some(PoolEvent { slot, event }) = events_rx.recv().await {
            match event {
                SupervisorEvent::AttemptStarted { attempt } => {
                    eprintln!("slot {slot}: starting peer proxy attempt {attempt}");
                }
                SupervisorEvent::SessionEnded {
                    attempt,
                    outcome,
                    duration,
                    retry_in,
                } => eprintln!(
                    "slot {slot}: attempt {attempt} session {} ended {:?} after {duration:?}; retrying in {retry_in:?}",
                    outcome.consumer_session_id, outcome.relay_end
                ),
                SupervisorEvent::AttemptFailed {
                    attempt,
                    error,
                    duration,
                    retry_in,
                } => eprintln!(
                    "slot {slot}: attempt {attempt} failed after {duration:?}: {error}; retrying in {retry_in:?}"
                ),
                SupervisorEvent::Stopped { summary } => eprintln!(
                    "slot {slot}: stopped after {} attempts ({} sessions, {} failures)",
                    summary.attempts, summary.completed_sessions, summary.failed_attempts
                ),
            }
        }
    });

    let slots = count("UNBOUNDED_CONCURRENT_SESSIONS", 5);
    let signaler = Arc::new(
        FreddieClient::new(required("UNBOUNDED_FREDDIE_ENDPOINT"))
            .unwrap_or_else(|error| panic!("invalid Freddie configuration: {error}")),
    );
    let summary = supervise_peer_proxy_pool(
        SupervisorConfig {
            peer_proxy: PeerProxyConfig {
                signaler,
                egress_url: required("UNBOUNDED_EGRESS_URL"),
                stun_urls,
                nat_timeout: seconds("UNBOUNDED_NAT_TIMEOUT_SECONDS", 10),
                enable_ipv6: env::var("UNBOUNDED_ENABLE_IPV6").is_ok_and(|value| value == "1"),
                randomize_dtls: env::var("UNBOUNDED_COVERT_DTLS")
                    .map(|value| !value.eq_ignore_ascii_case("disable"))
                    .unwrap_or(true),
            },
            initial_backoff: seconds("UNBOUNDED_RETRY_INITIAL_SECONDS", 1),
            max_backoff: seconds("UNBOUNDED_RETRY_MAX_SECONDS", 30),
            stable_session: seconds("UNBOUNDED_STABLE_SESSION_SECONDS", 30),
        },
        slots,
        cancellation,
        Some(events_tx),
    )
    .await;
    reporter.await.expect("event reporter failed");
    eprintln!(
        "peer proxy pool stopped: {slots} slots, {} attempts, {} sessions, {} failures",
        summary.attempts(),
        summary.completed_sessions(),
        summary.failed_attempts()
    );
}
