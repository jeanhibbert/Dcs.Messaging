# Light.Messaging (Lab)

Light.Messaging is a bare-bones, performance-first reimplementation of the Dcs.Messaging programming model. It moves
protobuf messages between processes on an internal network over **TCP** or **gRPC**. It keeps the concepts
Dcs.Messaging users know (named endpoints, publish/subscribe, targeted sends, request/response, protobuf-net DTOs)
and drops everything that costs time on the hot path.

It shares no code with Dcs.Messaging and can live alongside it.

- [Projects](#projects)
- [Quick start](#quick-start)
- [Running the demo](#running-the-demo)
- [API at a glance](#api-at-a-glance)
- [Performance](#performance)
- [Coming from Dcs.Messaging](#coming-from-dcsmessaging)
- [Limitations](#limitations)
- [Architecture, wire protocol and tuning](docs/ARCHITECTURE.md)

## Projects

| Project | What it is | Depends on |
|---|---|---|
| `Light.Messaging` | Core: endpoints, serializer, framing, connections, `MessagingServer`, `MessagingClient`, TCP transport | protobuf-net |
| `Light.Messaging.Grpc` | gRPC transport (`GrpcServerTransport`, `GrpcClientTransport`) on embedded Kestrel | Light.Messaging, ASP.NET Core, Grpc.AspNetCore.Server, Grpc.Net.Client |
| `Light.Messaging.Contracts` | Demo DTOs and endpoints shared by the two apps | Light.Messaging |
| `Light.Messaging.ServerApp` | Demo server | Contracts, Grpc |
| `Light.Messaging.ClientApp` | Demo client and benchmark | Contracts, Grpc |
| `Light.Messaging.Tests` | Integration tests, every scenario run over both transports | Light.Messaging, Grpc |

gRPC is a separate assembly so that a TCP-only deployment doesn't take a dependency on ASP.NET Core and Kestrel.

## Quick start

Define messages and endpoints once, in an assembly both sides reference:

```csharp
[ProtoContract]
public sealed class PricingRequest
{
    [ProtoMember(1)] public string CurrencyPair { get; set; }
    [ProtoMember(2)] public double SpotRate { get; set; }
}

public static class Endpoints
{
    public static readonly Endpoint Prices = new("Prices");
    public static readonly Endpoint Pricing = new("Pricing/Forward");
}
```

Server:

```csharp
await using var server = await MessagingServer.StartAsync(
    new TcpServerTransport("0.0.0.0", 5070),            // or new GrpcServerTransport("0.0.0.0", 5071)
    new MessagingServerOptions { Name = "pricing-server" });

using var pricing = server.Respond<PricingRequest, ForwardPrice>(Endpoints.Pricing, (request, context) => Price(request));
using var trades = server.Subscribe<TradeReport>(Endpoints.Trades, (trade, context) => Book(trade, context.Sender));

server.Publish(Endpoints.Prices, tick);                 // every subscribed client
server.Publish(Endpoints.Prices, tick, somePeer);       // one client
```

Client:

```csharp
await using var client = await MessagingClient.ConnectAsync(
    new TcpClientTransport("10.0.0.5", 5070),           // or new GrpcClientTransport("10.0.0.5", 5071)
    new MessagingClientOptions { Name = "trader-ui-7" });

using var prices = client.Subscribe<PriceTick>(Endpoints.Prices, tick => Show(tick));
var price = await client.RequestAsync<PricingRequest, ForwardPrice>(Endpoints.Pricing, request);
client.Publish(Endpoints.Trades, trade);
```

## Running the demo

```powershell
dotnet run -c Release --project Lab/Light.Messaging.ServerApp -- --transport tcp --port 5070
dotnet run -c Release --project Lab/Light.Messaging.ClientApp -- --transport tcp --port 5070
```

Use `--transport grpc` on both for gRPC. The client runs these steps:
1. Subscribes to live prices for a second.
2. Prices a forward with a request/response call.
3. Publishes a trade report, which the server logs.
4. Runs three benchmarks: sequential request latency, pipelined request throughput, and streaming throughput.

Client options are `--host`, `--port`, `--requests 20000` and `--burst 2000000`.

Run the tests with:

```powershell
dotnet test Lab/Light.Messaging.Tests -c Release
```

## API at a glance

| Type | Role |
|---|---|
| `Endpoint` | Named destination. Only its 32-bit id (FNV-1a of the address) goes on the wire. |
| `MessagingServer` | `StartAsync`, `Publish` (to subscribers or one peer), `Subscribe`, `Respond` / `RespondAsync`, `Peers`, `PeerConnected` / `PeerDisconnected`. |
| `MessagingClient` | `ConnectAsync`, `Subscribe`, `Publish`, `RequestAsync`, `Disconnected`, `Completion`. |
| `MessageContext` | Passed to handlers; `Sender` is the `Peer` the message came from. |
| `Peer` | Id, name, address, connection state, close reason, `QueuedFrames` (for flow control), `DroppedFrames`. |
| `MessagingServerOptions` / `MessagingClientOptions` | Name, serializer, queue capacity, `SlowConsumerPolicy`, frame and batch sizes, timeouts. |
| `IMessageSerializer` | Pluggable. `ProtobufNetSerializer` is the default. |
| `TcpServerTransport` / `TcpClientTransport` | Raw TCP; `SocketOptions` controls buffers and keep-alive. |
| `GrpcServerTransport` / `GrpcClientTransport` | One bidirectional gRPC stream per client over HTTP/2. |

Rules worth knowing:

- **Handlers run on the connection's receive loop.** Messages from one peer are handled in order, one at a time. A
  server runs handlers for different clients concurrently, so server-side handlers must be thread-safe. Keep handlers
  short: a slow handler delays that peer's later messages, but nobody else's.
- **`Publish` never blocks.** Each peer has its own bounded queue. When a queue is full, `SlowConsumerPolicy` decides
  what happens. The default, `Disconnect`, never loses messages silently; `DropOldest` suits prices. Publishers that
  want to throttle instead can watch `Peer.QueuedFrames`. The demo server does this in its benchmark.
- **A message object can be reused after `Publish` returns**, because it has already been serialized.
- **`RequestAsync` returns a pooled `ValueTask`.** Await it exactly once.
- **Clients do not reconnect automatically.** Handle `Disconnected`, then connect and subscribe again.

## Performance

These figures come from the demo client and server on one Windows 11 machine (20 cores, loopback), .NET 10, Release
build. They use the protobuf-net DTOs in `Light.Messaging.Contracts`.

| Benchmark | TCP | gRPC |
|---|---:|---:|
| Streaming server→client (2M `PriceTick`s) | **3.06M msg/s** | **3.49M msg/s** |
| Request/response, 256 in flight | **1.35M req/s** | **1.01M req/s** |
| Request/response, one at a time | 15,000 req/s | 8,100 req/s |
| Sequential round-trip latency p50 / p99 / p99.9 | 57 / 169 / 285 µs | 128 / 283 / 422 µs |

For comparison, the Dcs.Messaging throughput test measured about **50k msg/s over TCP and 98k msg/s over gRPC**.
That test used a different harness and message, so treat it as an order-of-magnitude comparison.

Where the gains come from (details in [the architecture document](docs/ARCHITECTURE.md)):

| | Dcs.Messaging | Light.Messaging |
|---|---|---|
| Header per message | ~120 bytes: endpoint string, sender session GUID, property dictionary | **9 bytes**, or 17 with a correlation id |
| Sending | Thread-pool hop and blocking socket write per message, behind a lock | **Enqueue only**; a writer loop per peer batches everything queued into one write |
| Fan-out | Every message to every connection, re-encoded per send | **Only to subscribed peers, serialized once**, shared by reference; nothing serialized when nobody subscribes |
| Slow subscriber | Stalls the broadcast loop and the thread pool | **Affects only itself**; bounded queue plus policy |
| Receive | Global lock, `Where` filter per subscription per message | **Lookup table, no lock**; decoded straight from the receive buffer |
| Request/response | GUID strings, Rx pipeline, `Task` per request | **64-bit correlation id, pooled `ValueTask` source** |
| Allocation | `MemoryStream` + `ToArray` + envelope per message | **Pooled frames and buffers**; the serializer writes into the frame |

## Coming from Dcs.Messaging

| Dcs.Messaging | Light.Messaging |
|---|---|
| `IEndpointDetails`, `IEndpointDetailsProvider`, endpoint keys | `Endpoint` constants in a shared contracts assembly |
| `TcpMessagingSessionBuilder` / `GrpcMessagingSessionBuilder` | `MessagingServer.StartAsync(transport)` / `MessagingClient.ConnectAsync(transport)` |
| `IMessagingService.Send(message, endpoint)` | `server.Publish(endpoint, dto)` / `client.Publish(endpoint, dto)` |
| `Send(..., targetSessionId)` | `server.Publish(endpoint, dto, peer)` |
| `GetMessageStream(endpoint)` + Rx `Subscribe` | `Subscribe<T>(endpoint, handler)` returns `IDisposable` |
| `IRequestResponder.GetRespondableRequestStream` + `Respond` | `server.Respond<TRequest, TResponse>(endpoint, handler)` |
| Hand-rolled correlation id, `Where` and `Take(1)` on the response stream | `await client.RequestAsync<TRequest, TResponse>(endpoint, request)` |
| `IBinarySerializer` (`byte[]` in and out) | `IMessageSerializer` (writes to `IBufferWriter<byte>`, reads from `ReadOnlySpan<byte>`) |
| `SenderSessionId` (string) | `MessageContext.Sender` (`Peer`) |
| TTL, persistence and arbitrary message properties | Not carried. Put what you need in the DTO. |
| `ServiceBase<T>` with operation attributes, `ICommandListener` | Not included. Register handlers directly. |
| SBE transport | Not included. The lean framing already removes the envelope overhead SBE addressed. |

## Limitations

These are deliberately out of scope for a bare-bones library:

- **Topology:** a star of one server and many clients. The server does not relay client messages to other clients; a
  handler can republish them.
- **No automatic reconnect, replay or sequence recovery.**
- **No TLS on TCP.** gRPC accepts an `https://` client address, but the server listens in cleartext (h2c).
- **Request/response runs client to server only.**
- **Latency figures are same-host.** They rely on `Stopwatch` timestamps, which can't be compared across machines.
