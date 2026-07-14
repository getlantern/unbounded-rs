use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::join_all;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::peer_proxy::{
    run_peer_proxy_until_cancelled, PeerProxyConfig, PeerProxyError, PeerProxyOutcome,
};

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub peer_proxy: PeerProxyConfig,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub stable_session: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SupervisorSummary {
    pub attempts: u64,
    pub completed_sessions: u64,
    pub failed_attempts: u64,
}

#[derive(Debug)]
pub enum SupervisorEvent {
    AttemptStarted {
        attempt: u64,
    },
    /// A censored consumer's WebRTC connection reached the Connected state.
    ///
    /// `session_id` is the consumer's session ID (the same value carried on
    /// [`PeerProxyOutcome::consumer_session_id`]). `remote` is the peer's
    /// address read from the selected ICE candidate pair, or `None` when it is
    /// unavailable (e.g. an mDNS candidate address that does not parse to an IP).
    PeerConnected {
        session_id: String,
        remote: Option<std::net::SocketAddr>,
    },
    /// A previously connected consumer's WebRTC connection closed, failed, or
    /// disconnected. Emitted exactly once per connected session.
    PeerDisconnected {
        session_id: String,
    },
    SessionEnded {
        attempt: u64,
        outcome: PeerProxyOutcome,
        duration: Duration,
        retry_in: Duration,
    },
    AttemptFailed {
        attempt: u64,
        error: String,
        duration: Duration,
        retry_in: Duration,
    },
    Stopped {
        summary: SupervisorSummary,
    },
}

#[derive(Debug)]
pub struct PoolEvent {
    pub slot: usize,
    pub event: SupervisorEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorPoolSummary {
    pub workers: Vec<SupervisorSummary>,
}

impl SupervisorPoolSummary {
    pub fn attempts(&self) -> u64 {
        self.workers.iter().map(|summary| summary.attempts).sum()
    }

    pub fn completed_sessions(&self) -> u64 {
        self.workers
            .iter()
            .map(|summary| summary.completed_sessions)
            .sum()
    }

    pub fn failed_attempts(&self) -> u64 {
        self.workers
            .iter()
            .map(|summary| summary.failed_attempts)
            .sum()
    }
}

pub async fn supervise_peer_proxy_pool(
    config: SupervisorConfig,
    slots: usize,
    cancellation: CancellationToken,
    events: Option<mpsc::UnboundedSender<PoolEvent>>,
) -> SupervisorPoolSummary {
    let workers = (0..slots.max(1)).map(|slot| {
        let config = config.clone();
        let cancellation = cancellation.child_token();
        let events = events.clone();
        async move {
            let (worker_events_tx, mut worker_events_rx) = mpsc::unbounded_channel();
            let mut supervise = std::pin::pin!(supervise_peer_proxy(
                config,
                cancellation,
                Some(worker_events_tx)
            ));
            // Forward inline and terminate when the supervised session finishes —
            // not when the channel closes. A session's WebRTC state-change callback
            // can outlive the session while still holding a clone of the event
            // sender; waiting for that clone to drop would hang shutdown. The final
            // `Stopped` event is enqueued before `supervise_peer_proxy` returns, so
            // draining once it completes forwards every event without loss.
            let summary = loop {
                tokio::select! {
                    biased;
                    summary = &mut supervise => {
                        while let Ok(event) = worker_events_rx.try_recv() {
                            if let Some(events) = &events {
                                let _ = events.send(PoolEvent { slot, event });
                            }
                        }
                        break summary;
                    }
                    Some(event) = worker_events_rx.recv() => {
                        if let Some(events) = &events {
                            let _ = events.send(PoolEvent { slot, event });
                        }
                    }
                }
            };
            (slot, summary)
        }
    });
    let mut workers = join_all(workers).await;
    workers.sort_by_key(|(slot, _)| *slot);
    SupervisorPoolSummary {
        workers: workers.into_iter().map(|(_, summary)| summary).collect(),
    }
}

pub async fn supervise_peer_proxy(
    config: SupervisorConfig,
    cancellation: CancellationToken,
    events: Option<mpsc::UnboundedSender<SupervisorEvent>>,
) -> SupervisorSummary {
    supervise_with(
        config,
        cancellation,
        events,
        |config, cancellation, session_events| {
            run_peer_proxy_until_cancelled(config, cancellation, session_events)
        },
    )
    .await
}

async fn supervise_with<F, Fut>(
    config: SupervisorConfig,
    cancellation: CancellationToken,
    events: Option<mpsc::UnboundedSender<SupervisorEvent>>,
    run_session: F,
) -> SupervisorSummary
where
    F: Fn(
        PeerProxyConfig,
        CancellationToken,
        Option<mpsc::UnboundedSender<SupervisorEvent>>,
    ) -> Fut,
    Fut: Future<Output = Result<PeerProxyOutcome, PeerProxyError>>,
{
    let initial_backoff = nonzero(config.initial_backoff);
    let max_backoff = config.max_backoff.max(initial_backoff);
    let mut backoff = initial_backoff;
    let mut summary = SupervisorSummary::default();

    loop {
        if cancellation.is_cancelled() {
            break;
        }

        summary.attempts += 1;
        let attempt = summary.attempts;
        emit(&events, SupervisorEvent::AttemptStarted { attempt });
        let started = tokio::time::Instant::now();
        let result = run_session(
            config.peer_proxy.clone(),
            cancellation.child_token(),
            events.clone(),
        )
        .await;
        let duration = started.elapsed();

        if result.is_err() && cancellation.is_cancelled() {
            break;
        }

        let relay_duration = match &result {
            Ok(outcome) => Some(outcome.relay_duration),
            Err(error) => error.relay_duration(),
        };
        let stable = relay_duration.is_some_and(|duration| duration >= config.stable_session);
        if stable {
            backoff = initial_backoff;
        }
        let retry_in = jittered(backoff, entropy(attempt)).min(max_backoff);

        match result {
            Ok(outcome) => {
                summary.completed_sessions += 1;
                emit(
                    &events,
                    SupervisorEvent::SessionEnded {
                        attempt,
                        outcome,
                        duration,
                        retry_in,
                    },
                );
            }
            Err(error) => {
                summary.failed_attempts += 1;
                emit(
                    &events,
                    SupervisorEvent::AttemptFailed {
                        attempt,
                        error: error.to_string(),
                        duration,
                        retry_in,
                    },
                );
            }
        }

        if !stable {
            backoff = backoff.saturating_mul(2).min(max_backoff);
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = tokio::time::sleep(retry_in) => {}
        }
    }

    emit(
        &events,
        SupervisorEvent::Stopped {
            summary: summary.clone(),
        },
    );
    summary
}

fn emit(events: &Option<mpsc::UnboundedSender<SupervisorEvent>>, event: SupervisorEvent) {
    if let Some(events) = events {
        let _ = events.send(event);
    }
}

fn nonzero(duration: Duration) -> Duration {
    if duration.is_zero() {
        Duration::from_millis(1)
    } else {
        duration
    }
}

fn entropy(attempt: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    now ^ attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

fn jittered(base: Duration, entropy: u64) -> Duration {
    let percent = 80 + entropy % 41;
    let nanos = base.as_nanos().saturating_mul(percent as u128) / 100;
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use super::*;

    #[derive(Debug)]
    struct TestSignaler;

    #[async_trait::async_trait]
    impl crate::signaling::Signaler for TestSignaler {
        async fn exchange(
            &self,
            _send_to: &str,
            _kind: crate::protocol::SignalMessageType,
            _payload: &str,
        ) -> Result<Option<crate::protocol::SignalMessage>, crate::signaling::SignalingError>
        {
            Ok(None)
        }
    }

    fn test_config() -> SupervisorConfig {
        SupervisorConfig {
            peer_proxy: PeerProxyConfig {
                signaler: Arc::new(TestSignaler),
                egress_url: "ws://127.0.0.1:1/ws".into(),
                stun_urls: Vec::new(),
                nat_timeout: Duration::from_millis(1),
                enable_ipv6: false,
                randomize_dtls: false,
            },
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            stable_session: Duration::from_secs(1),
        }
    }

    #[test]
    fn jitter_stays_within_twenty_percent() {
        let base = Duration::from_secs(10);
        for seed in 0..100 {
            let delay = jittered(base, seed);
            assert!(delay >= Duration::from_secs(8));
            assert!(delay <= Duration::from_secs(12));
        }
    }

    #[test]
    fn zero_backoff_is_made_nonzero() {
        assert_eq!(nonzero(Duration::ZERO), Duration::from_millis(1));
    }

    #[tokio::test]
    async fn retries_failures_and_stops_cleanly_on_cancellation() {
        let cancellation = CancellationToken::new();
        let attempts = Arc::new(AtomicU64::new(0));
        let summary = supervise_with(test_config(), cancellation.clone(), None, {
            let attempts = attempts.clone();
            move |_, _, _| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                let cancellation = cancellation.clone();
                async move {
                    if attempt == 3 {
                        cancellation.cancel();
                        Err(PeerProxyError::Cancelled)
                    } else {
                        Err(PeerProxyError::MissingResponse("test response"))
                    }
                }
            }
        })
        .await;

        assert_eq!(summary.attempts, 3);
        assert_eq!(summary.failed_attempts, 2);
        assert_eq!(summary.completed_sessions, 0);
    }

    #[tokio::test]
    async fn shutdown_race_does_not_count_a_transport_failure() {
        let cancellation = CancellationToken::new();
        let summary = supervise_with(test_config(), cancellation.clone(), None, {
            move |_, _, _| {
                let cancellation = cancellation.clone();
                async move {
                    cancellation.cancel();
                    Err(PeerProxyError::MissingResponse("shutdown race"))
                }
            }
        })
        .await;

        assert_eq!(summary.attempts, 1);
        assert_eq!(summary.failed_attempts, 0);
        assert_eq!(summary.completed_sessions, 0);
    }

    #[tokio::test]
    async fn stable_relay_resets_backoff_even_when_transport_fails() {
        let cancellation = CancellationToken::new();
        let attempts = Arc::new(AtomicU64::new(0));
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let mut config = test_config();
        config.initial_backoff = Duration::from_millis(10);
        config.max_backoff = Duration::from_millis(40);
        config.stable_session = Duration::from_secs(30);

        supervise_with(config, cancellation.clone(), Some(events_tx), {
            let attempts = attempts.clone();
            move |_, _, _| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                let cancellation = cancellation.clone();
                async move {
                    match attempt {
                        1 => Err(PeerProxyError::Relay {
                            relay_duration: Duration::from_secs(31),
                            source: crate::relay::RelayError::Peer(Box::new(io::Error::other(
                                "test relay failure",
                            ))),
                        }),
                        2 => Err(PeerProxyError::MissingResponse("test response")),
                        _ => {
                            cancellation.cancel();
                            Err(PeerProxyError::Cancelled)
                        }
                    }
                }
            }
        })
        .await;

        let mut second_retry = None;
        while let Ok(event) = events_rx.try_recv() {
            if let SupervisorEvent::AttemptFailed {
                attempt: 2,
                retry_in,
                ..
            } = event
            {
                second_retry = Some(retry_in);
            }
        }
        assert!(second_retry.unwrap() <= Duration::from_millis(12));
    }

    #[tokio::test]
    async fn pool_shuts_down_when_a_session_registers_a_state_callback() {
        // Regression: `run_peer_proxy_until_cancelled` registers an
        // `on_peer_connection_state_change` callback that captures a clone of the
        // per-session `SupervisorEvent` sender (and, before the fix, a strong
        // `Arc` back to the connection). WebRTC keeps that callback alive past the
        // session, so the sender clone outlived the session and kept the worker's
        // forwarder channel open — the pool future never completed after
        // cancellation and `SharingHandle::stop` hung. The pool must shut down
        // regardless of a leaked event-sender clone.
        let cancellation = CancellationToken::new();
        let (events_tx, mut events_rx) = mpsc::unbounded_channel::<PoolEvent>();
        let mut config = test_config();
        // Park in backoff after the first failure so cancellation, not the retry
        // cadence, drives shutdown.
        config.initial_backoff = Duration::from_secs(30);
        config.max_backoff = Duration::from_secs(30);

        let pool = tokio::spawn(supervise_peer_proxy_pool(
            config,
            1,
            cancellation.clone(),
            Some(events_tx),
        ));

        // Wait until the real peer proxy has attempted and failed — this proves it
        // built the RTCPeerConnection and registered the state-change callback.
        let mut saw_failure = false;
        while let Some(PoolEvent { event, .. }) =
            tokio::time::timeout(Duration::from_secs(5), events_rx.recv())
                .await
                .expect("expected peer-proxy events before timeout")
        {
            if matches!(event, SupervisorEvent::AttemptFailed { .. }) {
                saw_failure = true;
                break;
            }
        }
        assert!(saw_failure, "peer proxy should have attempted and failed");

        cancellation.cancel();
        let summary = tokio::time::timeout(Duration::from_secs(5), pool)
            .await
            .expect("pool did not shut down within 5s of cancellation")
            .expect("pool task panicked");
        assert_eq!(summary.workers.len(), 1);
        assert!(summary.attempts() >= 1);
    }

    #[test]
    fn pool_summary_aggregates_worker_counts() {
        let summary = SupervisorPoolSummary {
            workers: vec![
                SupervisorSummary {
                    attempts: 4,
                    completed_sessions: 2,
                    failed_attempts: 2,
                },
                SupervisorSummary {
                    attempts: 3,
                    completed_sessions: 1,
                    failed_attempts: 2,
                },
            ],
        };

        assert_eq!(summary.attempts(), 7);
        assert_eq!(summary.completed_sessions(), 3);
        assert_eq!(summary.failed_attempts(), 4);
    }
}
