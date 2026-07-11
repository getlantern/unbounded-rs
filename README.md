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

The initial milestone establishes the exact wire contracts and the stable
virtual datagram socket that Quinn will run on. See
[`docs/wire-protocol.md`](docs/wire-protocol.md).

```sh
cargo test
```

