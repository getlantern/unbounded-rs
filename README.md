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

The diagnostic native peer proxy runs one sharing session using environment
configuration:

```sh
UNBOUNDED_FREDDIE_ENDPOINT=http://localhost:9000/v1/signal \
UNBOUNDED_EGRESS_URL=ws://localhost:8000/ws \
UNBOUNDED_STUN_URLS=stun:stun.example.org:3478 \
cargo run --bin peer-proxy
```

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
