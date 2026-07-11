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
now includes Freddie signaling, a Pion-compatible unreliable/unordered WebRTC
DataChannel, the CSID-authenticated egress WebSocket, and bidirectional packet
relay. See [`docs/wire-protocol.md`](docs/wire-protocol.md).

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
| `UNBOUNDED_COVERT_DTLS` | `randomize` | Set to `disable` only for controlled diagnostics |
| `UNBOUNDED_ENABLE_IPV6` | unset | Set to `1` to gather IPv6 ICE candidates |

Library embedders can consume slot-tagged `PoolEvent` and `SupervisorEvent`
values for attempt, session, failure, backoff, and shutdown reporting.
Cancellation closes every active WebRTC peer connection before the pool exits.
Third-party logs default to `error` to avoid exposing ICE candidate addresses;
operators can opt into more detail with `RUST_LOG`.

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
