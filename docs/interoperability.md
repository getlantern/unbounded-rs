# Go interoperability contract

The native peer proxy is compatible when it can replace the Go sharing peer
without changing Freddie, the censored-user consumer, or the egress service.
The essential live validation is stronger than a successful WebRTC handshake:
an already-open QUIC stream must survive a peer change.

## Migration scenario

1. Start the Go Freddie and egress services.
2. Start the Go consumer and wait for its SOCKS proxy.
3. Start Rust peer proxy A and establish a proxied HTTP transfer large enough
   to remain active during migration.
4. Stop A and start Rust peer proxy B.
5. Confirm B receives the same `ConsumerSessionID` and uses it as the egress
   WebSocket subprotocol.
6. Confirm the Go egress probes the replacement path and reports a successful
   QUIC migration.
7. Confirm the original HTTP request completes without reconnecting and its
   status and byte count are unchanged.

On 2026-07-11 this scenario completed with HTTP 200 and exactly 20 MiB received.
The Go egress reported a successful migration after a 25.145-second probe, and
the open stream resumed over peer B within the existing QUIC idle timeout.

## Compatibility details found live

- Pion encodes the ICE candidate `protocol` field numerically: `1` for UDP and
  `2` for TCP. Rust converts those values before producing a W3C candidate.
- The DataChannel label is `data`, with ordering disabled and zero retransmits.
- The egress WebSocket subprotocols are ordered as `un80und3d`, the consumer
  session ID, and `v2.3.0`.
- IPv4-only ICE is the safe peer-proxy default. Link-local IPv6 interfaces on
  macOS can produce candidates that pass signaling but never nominate.
