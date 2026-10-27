# The LeonsDB wire protocol

LeonsDB speaks gRPC, and also a lean binary protocol over TCP, the *wire
protocol*, for the calls a Minecraft server makes most: saves, loads, player
ownership (leases) and messages. The connector uses it when the server
offers it. It runs exactly the same handlers as gRPC, with the same
authentication, permissions, results and errors; it only costs less per
call. Many calls are in flight on one connection at once, and answers that
are ready together go out in a single network write. Streams of answers to
one request, such as subscriptions to message channels, run over it too.

Streams of requests and the admin tools keep using gRPC, and so can any
client: the wire protocol is an optimization, never a requirement.

## Where it listens

`wire_listen` in the server configuration. Left empty, it is the host of
`listen` with port **7422**; `off` turns it off. It uses the same TLS
certificate as gRPC, and the same rule: reachable from the network only with
TLS, unless `insecure_allow_plaintext` is set. `ServerService.GetInfo`
reports the port (`wire_port`, 0 when off), so clients find it from their
gRPC address.

## Framing

All numbers are big-endian. Every frame is:

| bytes | field |
|---|---|
| 4 | length of the rest of the frame (1 to 16 MiB + 64) |
| 1 | type |
| … | body |

| type | direction | body |
|---|---|---|
| 1 hello | client → server | `WireHello` |
| 2 welcome | server → client | `WireWelcome` |
| 3 request | client → server | id (4), method (2), timeout in ms (4, 0 = none), request message |
| 4 response | server → client | id (4), response message |
| 5 error | server → client | id (4), `WireError` |
| 6 message | server → client | id (4), one message of a stream |
| 7 header | server → client | id (4): the stream has begun |
| 8 cancel | client → server | id (4): ends a stream |

Messages are the protobuf messages of `proto/leonsdb/v1`: the hello and
welcome are defined in `wire.proto`, requests and responses are the same
messages as the gRPC calls they stand for.

1. The client sends a hello with the protocol version (1) and its API token.
   A wrong token or version is answered with an error frame with id 0, and
   the connection is closed.
2. The server answers with a welcome: the database's instance ID, and the
   full gRPC names of the calls the connection may use, such as
   `/leonsdb.v1.DataService/Apply`. A request names its call by its index in
   that list.
3. The client sends requests with ids of its choice, without waiting for
   answers. Each is answered by one response or error frame with its id, in
   any order.
4. A request for a stream of answers (a gRPC call that streams its
   responses, such as `/leonsdb.v1.MessageService/Subscribe`) is answered by
   a header frame once the stream has begun (what gRPC sends as response
   headers; the first message also means it), a message frame per answer,
   and at its end a response frame with no message, or an error frame. The
   client ends it early with a cancel frame; its context ends on the server
   as when a gRPC client cancels. The ids of running streams and requests
   must differ.

An error frame carries the gRPC status code and message, and what gRPC
sends as trailers, such as `leonsdb-error-reason` (`STALE_EPOCH`,
`PLAYER_ONLINE`, …). The token is checked on every call, as over gRPC, so
revoking a token ends its access at once, also on an open connection.

A server runs up to 512 requests of one connection at a time and stops
reading the connection beyond that; streams do not count, and up to 256 run
at once on a connection. On shutdown it stops reading, answers the requests
that are running, ends the streams, and closes the connection.

## Clients

- The connector (Java) uses it automatically for saves, loads, leases and
  messages (publishing; subscriptions use gRPC) when the server offers it, and
  gRPC whenever it is not connected. Start the Minecraft server with
  `-Dleonsdb.wire=false` to use only gRPC.
- Go: `github.com/Leoon-X/leonsdb-connector/proto/wireclient`. A `wireclient.Client` is
  a `grpc.ClientConnInterface`, so the generated clients work over it,
  streams of answers included: `leonsdbv1.NewDataServiceClient(client)`.
