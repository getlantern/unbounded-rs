# Unbounded native wire protocol

This document records the compatibility surface between a native client or
peer proxy and the existing Go services. The Go implementation remains the
reference until each item has a checked-in golden vector or interoperability
test.

## Roles and migration ownership

| Role | QUIC role | Responsibility |
| --- | --- | --- |
| Censored consumer | Server | Holds a stable QUIC endpoint above replaceable WebRTC paths |
| Peer proxy | None | Relays opaque datagrams between WebRTC and the egress WebSocket |
| Go egress | Client | Adds, probes, and switches QUIC paths when peer proxies churn |

The Rust consumer does not initiate migration. A replacement peer path causes
packets for the existing QUIC connection ID to arrive from a new logical
remote address. Quinn's server-side migration handling validates that path.

## QUIC

- ALPN: `broflake`
- Maximum incoming bidirectional streams: `131072`
- Maximum incoming unidirectional streams: `131072`
- Maximum idle timeout: 60 seconds
- Keepalive period: 15 seconds
- The consumer is the QUIC server; the Go egress is the QUIC client.

The consumer's virtual UDP socket must outlive every individual WebRTC peer.
A peer disconnect is represented by removing one route, never by returning a
fatal error from the socket.

## WebRTC data channel

- Label: `data`
- Ordered: false
- Maximum retransmits: 0
- Binary messages preserve one QUIC datagram per DataChannel message.

The unreliable unordered mode is intentional. QUIC supplies ordering,
retransmission, congestion control, and loss recovery above the DataChannel.

## Egress WebSocket

The peer proxy dials the configured egress address and endpoint with three
`Sec-WebSocket-Protocol` values, in order:

1. `un80und3d`
2. consumer session ID (CSID)
3. protocol version, currently `v2.3.0`

The egress responds with `un80und3d`. Compression is disabled. Each binary
WebSocket message contains one packet.

The current Go parser requires exactly three values but does not validate the
magic-cookie value. Rust reproduces that behavior for compatibility; clients
always emit the correct cookie.

## Packet envelope

Packets traveling from the Go egress toward the consumer are encoded using
Go's default `encoding/json` representation:

```json
{"SourceAddr":"WebSocket connection example","Payload":"ZXhhbXBsZQ=="}
```

`Payload` is standard padded base64 because Go encodes `[]byte` that way.
`SourceAddr` is the opaque identity of the egress WebSocket path. Because
Quinn's socket interface requires a `SocketAddr`, the Rust consumer maps each
active path identity to a distinct synthetic address. A replacement path must
receive a different synthetic address so Quinn observes and validates the
migration.

Packets from the consumer toward the egress are raw QUIC datagrams on the
peer-proxy WebSocket. The egress supplies the JSON envelope on the return
path.

## Freddie signaling

The parent signaling envelope uses Go field names and a numeric message type:

```json
{"ReplyTo":"request-id","Type":3,"Payload":"{...}"}
```

| Type | Value | Payload |
| --- | ---: | --- |
| Genesis | 0 | `GenesisMsg` |
| Offer | 1 | `OfferMsg` |
| Answer | 2 | WebRTC session description |
| ICE | 3 | `ICEMsg`, including `ConsumerSessionID` |

Every request carries `X-BF-Version: v2.3.0`. Signaling messages are posted as
`application/x-www-form-urlencoded` with `data`, `send-to`, and numeric `type`
fields. A `418` rejects the protocol version, `404` means the response request
has expired, and `200` with an empty body means no peer replied before the
server TTL. Consumer advertisement streams are newline-delimited signaling
envelopes returned by `GET /v1/signal`.

The peer-proxy exchange is:

1. POST Genesis to `genesis`; wait for an Offer response.
2. Apply the consumer's offer and gather local ICE candidates.
3. POST the complete Answer to the offer's `ReplyTo`; wait for ICE.
4. Convert the Pion candidate objects to W3C candidate strings and add them.
5. Use `ConsumerSessionID` from the ICE payload when opening the egress socket.

The censored-consumer exchange is:

1. Open the Freddie advertisement stream and collect Genesis candidates for
   the configured patience interval.
2. Create the unreliable `data` DataChannel and send an Offer to one selected
   Genesis request.
3. Apply the returned Answer, gather local ICE candidates, and encode Pion's
   protocol field numerically (`1` for UDP and `2` for TCP).
4. POST the candidates and the stable `ConsumerSessionID` as the ICE message.
5. Assign the opened peer path a fresh synthetic CGNAT address and relay it
   into the long-lived virtual UDP socket.
6. On peer failure, remove only that synthetic route and return to Freddie;
   the Quinn endpoint and consumer session ID remain alive for server-side
   migration initiated by the Go egress.
