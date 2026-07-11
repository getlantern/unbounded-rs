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
            let forwarder = tokio::spawn(async move {
                while let Some(event) = worker_events_rx.recv().await {
                    if let Some(events) = &events {
                        let _ = events.send(PoolEvent { slot, event });
                    }
                }
            });
            let summary = supervise_peer_proxy(config, cancellation, Some(worker_events_tx)).await;
            let _ = forwarder.await;
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
    supervise_with(config, cancellation, events, |config, cancellation| {
        run_peer_proxy_until_cancelled(config, cancellation)
    })
    .await
}

async fn supervise_with<F, Fut>(
    config: SupervisorConfig,
    cancellation: CancellationToken,
    events: Option<mpsc::UnboundedSender<SupervisorEvent>>,
    run_session: F,
) -> SupervisorSummary
where
    F: Fn(PeerProxyConfig, CancellationToken) -> Fut,
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
        let result = run_session(config.peer_proxy.clone(), cancellation.child_token()).await;
        let duration = started.elapsed();

        if matches!(result, Err(PeerProxyError::Cancelled)) && cancellation.is_cancelled() {
            break;
        }

        let stable = result.is_ok() && duration >= config.stable_session;
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use super::*;

    fn test_config() -> SupervisorConfig {
        SupervisorConfig {
            peer_proxy: PeerProxyConfig {
                freddie_endpoint: "http://127.0.0.1:1/v1/signal".into(),
                egress_url: "ws://127.0.0.1:1/ws".into(),
                stun_urls: Vec::new(),
                nat_timeout: Duration::from_millis(1),
                enable_ipv6: false,
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
            move |_, _| {
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
