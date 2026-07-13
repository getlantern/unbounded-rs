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

On 2026-07-12 the inverse mixed-language path also completed: the Spark Rust
consumer and two successive Rust peers used a local Go Freddie and the deployed
Go egress at `wss://unbounded.iantem.io/ws`. Peer A was stopped after roughly
2.9 MB (decimal) of an approximately 20 MB (decimal) HTTP response. The consumer
advertised a replacement path, peer B attached with the same consumer session
ID, and the original QUIC stream resumed without an application reconnect. The
request finished with HTTP 200, 20,001,492 total bytes received on the wire, two
consumer attempts, one completed path, and zero failed attempts.

## Compatibility details found live

- Pion encodes the ICE candidate `protocol` field numerically: `1` for UDP and
  `2` for TCP. Rust converts those values before producing a W3C candidate.
- The DataChannel label is `data`, with ordering disabled and zero retransmits.
- The egress WebSocket subprotocols are ordered as `un80und3d`, the consumer
  session ID, and `v2.3.0`.
- quic-go starts paths with 1280-byte packets and can probe as high as 1452
  bytes. The Rust consumer's virtual UDP ingress must accept that full range,
  even though Quinn's own outbound path remains pinned to 1200 bytes.
- IPv4-only ICE is the safe peer-proxy default. Link-local IPv6 interfaces on
  macOS can produce candidates that pass signaling but never nominate.

## Process lifecycle

The Go producer treats each sharing slot as a resetting state machine. The Rust
supervisor preserves that behavior: signaling errors, NAT timeouts, relay
closure, and egress failures all return the slot to discovery. Retries use
bounded exponential backoff with jitter, and a session that remains active for
the configured stable interval resets the backoff.

The process runs five independent slots by default, matching the Go widget's
consumer table size. Each slot owns its WebRTC and egress lifecycles; only the
shutdown token and event sink are shared.

Cancellation is distinct from failure. It interrupts signaling, NAT traversal,
an active relay, or a retry delay; closes the current WebRTC peer connection;
and exits without incrementing the failed-attempt count.

## DTLS fingerprint resistance

The sharing peer is the WebRTC answerer and therefore the active DTLS endpoint
that sends the ClientHello. The Rust peer randomizes cipher-suite and extension
ordering for every ClientHello flight, including the retry carrying a
HelloVerifyRequest cookie. This preserves the negotiated values and the DTLS
state machine while avoiding a stable library-default fingerprint.

The current Rust DTLS stack is DTLS 1.2. It deliberately does not claim to
mimic current Chrome DTLS 1.3 fingerprints, which include unsupported cipher
suites and post-quantum key-share structures. Browser mimicry should only be
added when those messages can be represented faithfully end to end.
