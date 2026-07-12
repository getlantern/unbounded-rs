# unbounded-rs

Native Rust implementation of the pieces of
[`getlantern/unbounded`](https://github.com/getlantern/unbounded) needed by
Lantern clients and peer proxies.

This project is a protocol-compatible reimplementation, not a line-by-line
port. The infrastructure-owned Freddie discovery service and Go egress remain
the reference control plane and QUIC migration controller.

The two Rust roles are:

- **consumer** — the censored user. It runs a stable QUIC server over
  replaceable WebRTC peer paths.
- **peer proxy** — the sharing user. It relays opaque QUIC datagrams between a
  WebRTC DataChannel and the Go egress WebSocket.

The Go egress is the QUIC client. It owns active path migration as peer proxies
churn; the Rust consumer only needs Quinn's server-side migration support.

## Status

The protocol and migration foundation is complete. The native peer-proxy path
includes Freddie signaling, a Pion-compatible unreliable/unordered WebRTC
DataChannel, the CSID-authenticated egress WebSocket, and bidirectional packet
relay.

The censored-consumer library path now includes Freddie advertisement
selection and offerer signaling, replaceable WebRTC sessions, Go-compatible
ICE candidate encoding, egress packet-envelope decoding, synthetic path
addresses, and a stable Quinn server endpoint. `maintain_consumer` re-pairs
after peer churn while preserving the virtual UDP socket and consumer session
ID that identify the existing QUIC connection. See
[`docs/wire-protocol.md`](docs/wire-protocol.md).

```sh
cargo test
```

The native peer proxy runs continuously, returning to Freddie discovery after
each completed or failed sharing session. It shuts down cleanly on Ctrl-C and
uses bounded exponential backoff with ±20% jitter between attempts:

```sh
UNBOUNDED_FREDDIE_ENDPOINT=http://localhost:9000/v1/signal \
UNBOUNDED_EGRESS_URL=ws://localhost:8000/ws \
UNBOUNDED_STUN_URLS=stun:stun.example.org:3478 \
cargo run --bin peer-proxy
```

Operational settings are environment variables:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `UNBOUNDED_CONCURRENT_SESSIONS` | 5 | Independent consumer slots, matching the Go widget default |
| `UNBOUNDED_NAT_TIMEOUT_SECONDS` | 10 | Time allowed for the WebRTC DataChannel to open |
| `UNBOUNDED_RETRY_INITIAL_SECONDS` | 1 | Initial retry delay |
| `UNBOUNDED_RETRY_MAX_SECONDS` | 30 | Maximum retry delay, including jitter |
| `UNBOUNDED_STABLE_SESSION_SECONDS` | 30 | Session duration that resets retry backoff |
| `UNBOUNDED_COVERT_DTLS` | unset (randomize) | Unset randomizes; truthy/`randomize` keeps it on, falsey/`disable` is diagnostic-only |
| `UNBOUNDED_ENABLE_IPV6` | unset | Set to `1`, `true`, `yes`, or `on` to gather IPv6 ICE candidates |

Library embedders can consume slot-tagged `PoolEvent` and `SupervisorEvent`
values for attempt, session, failure, backoff, and shutdown reporting.
Cancellation closes every active WebRTC peer connection before the pool exits.
Third-party logs default to `error` to avoid exposing ICE candidate addresses;
operators can opt into more detail with `RUST_LOG`.

Embedders that already own an HTTP stack can exclude the native CLI and its
`reqwest`/`env_logger` dependencies, then supply Freddie signaling through the
object-safe `Signaler` trait:

```toml
lantern-unbounded = { version = "0.1", default-features = false }
```

The default `native-client` feature retains the standalone `peer-proxy` binary
and the `FreddieClient` implementation used by the command above.

## Censored-consumer embedding

`ConsumerConfig::new` accepts an object-safe `ConsumerSignaler`, the stable
`VirtualUdpSocket`, a `SyntheticPathAllocator`, and a caller-owned consumer
session ID. `FreddieClient` implements both the peer-proxy `Signaler` and the
consumer advertisement interface; Spark can provide the same interfaces from
its existing HTTP stack with default features disabled.

`ConsumerQuicServer` runs Quinn over that virtual socket and applies the Go
transport contract: 131072 incoming streams in each direction, a 60-second
idle timeout, and a 15-second keepalive. Quinn sends conservative 1200-byte
packets; the virtual path accepts up to 1452 bytes so the infrastructure Go
egress's 1280-byte initial packets and path-MTU probes are not discarded. The
supplied Quinn TLS configuration must advertise the `broflake` ALPN value
exposed as `CONSUMER_QUIC_ALPN`.

`ConsumerQuicBroker` owns the long-lived accept loop. Its cloneable
`ConsumerQuicDialer` waits for the current or next infrastructure-owned QUIC
client and opens a bidirectional stream, so an embedding proxy does not need to
own connection churn or path migration. Both the broker and pending stream
opens are cancellation-aware. A closed infrastructure connection clears the
current state, and later stream opens wait for its replacement.

`ConsumerQuicDialer::connect_socks5` adds the target-routing handshake used by
the Go consumer and egress. It supports IPv4, IPv6, and domain targets, fully
consumes the server's variable-length bound-address response, and returns the
same stream ready for transparent application bytes.

The remaining integration work is production STUN cohort sourcing/rotation,
Spark transport wiring, and live validation of the Rust consumer against the
deployed Go Freddie, peer proxy, and egress.

DTLS ClientHello randomization is enabled by default. Cipher suites and
extensions retain their negotiated contents but are independently reordered on
each ClientHello flight, preventing the stable library-default fingerprint that
has been filtered in deployed censored networks. The pinned WebRTC dependency
contains the typed hook proposed upstream in
[`webrtc-rs/webrtc#814`](https://github.com/webrtc-rs/webrtc/pull/814).

The peer proxy uses IPv4 ICE candidates by default. This avoids advertising
unroutable link-local IPv6 candidates on hosts such as macOS. Set
`UNBOUNDED_ENABLE_IPV6=1` only where IPv6 routing is known to work.

## Go interoperability

The peer-proxy path has been exercised end to end against the Go Freddie,
consumer, and egress implementations. A proxied 20 MiB HTTP response remained
open while peer proxy A was stopped and peer proxy B joined with the same
consumer session ID. The Go egress migrated its QUIC client connection to B,
the transfer resumed, and the client received the exact response length with
HTTP 200.

This is the intended ownership boundary: Rust provides replacement datagram
paths, while the infrastructure-owned Go egress decides when to probe and
switch paths. See [`docs/interoperability.md`](docs/interoperability.md) for the
validation contract.
