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
| `Light.Messaging` | Core: endpoints, serializer, framing, connections, `MessagingServer`, `MessagingClient`, TCP transport | protobuf-net, Microsoft.Extensions.Logging.Abstractions |
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
    new MessagingServerOptions { Name = "pricing-server", Logger = loggerFactory.CreateLogger("Light.Messaging") });

using var pricing = server.Respond<PricingRequest, ForwardPrice>(Endpoints.Pricing, (request, context) => Price(request));
using var trades = server.Subscribe<TradeReport>(Endpoints.Trades, (trade, context) => Book(trade, context.Sender));

server.Publish(Endpoints.Prices, tick);                 // every subscribed client
server.Publish(Endpoints.Prices, tick, somePeer);       // one client
```

Client:

```csharp
await using var client = await MessagingClient.ConnectAsync(
    new TcpClientTransport("10.0.0.5", 5070),           // or new GrpcClientTransport("10.0.0.5", 5071)
    new MessagingClientOptions { Name = "trader-ui-7", Logger = loggerFactory.CreateLogger("Light.Messaging") });

using var prices = client.Subscribe<PriceTick>(Endpoints.Prices, tick => Show(tick));
var price = await client.RequestAsync<PricingRequest, ForwardPrice>(Endpoints.Pricing, request);
client.Publish(Endpoints.Trades, trade);

// Reconnect is automatic and subscriptions are restored. Anything published during the outage is gone, so refresh.
client.Reconnected += server => RequestSnapshot();
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

To watch reconnect, run the client with `--watch true` instead. It skips the benchmarks and prints the live price
rate every second. Stop or restart the server and the client logs the disconnect and each retry, then reconnects and
restores its subscription on its own:

```
   30 prices/s   connected
20:17:59.561 warn: Light.Messaging[0] Connection to lab-server ([::ffff:127.0.0.1]:5181) lost (An existing connection was forcibly closed by the remote host.); reconnecting
    0 prices/s   reconnecting
20:18:01.697 warn: Light.Messaging[0] Reconnect attempt 1 failed (No connection could be made because the target machine actively refused it.); retrying in about 200 ms
20:18:02.904 info: Light.Messaging[0] Reconnected to lab-server ([::ffff:127.0.0.1]:5181) after 2 attempt(s); restored 1 subscription(s)
   30 prices/s   connected
```

Run the tests with:

```powershell
dotnet test Lab/Light.Messaging.Tests -c Release
```

## API at a glance

| Type | Role |
|---|---|
| `Endpoint` | Named destination. Only its 32-bit id (FNV-1a of the address) goes on the wire. |
| `MessagingServer` | `StartAsync`, `Publish` (to subscribers or one peer), `Subscribe`, `Respond` / `RespondAsync`, `Peers`, `PeerConnected` / `PeerDisconnected`. |
| `MessagingClient` | `ConnectAsync`, `Subscribe`, `Publish`, `RequestAsync`, `Disconnected`, `Reconnected`, `IsConnected`, `Completion`. |
| `MessageContext` | Passed to handlers; `Sender` is the `Peer` the message came from. |
| `Peer` | Id, name, address, connection state, close reason, `QueuedFrames` (for flow control), `DroppedFrames`. |
| `MessagingServerOptions` / `MessagingClientOptions` | Name, `Logger`, serializer, queue capacity, `SlowConsumerPolicy`, frame and batch sizes, timeouts; on the client also `AutoReconnect`, `ReconnectMinDelay`, `ReconnectMaxDelay` and `MaxReconnectAttempts`. |
| `IMessageSerializer` | Pluggable. `ProtobufNetSerializer` is the default. |
| `TcpServerTransport` / `TcpClientTransport` | Raw TCP; `SocketOptions` controls buffers and keep-alive. |
| `GrpcServerTransport` / `GrpcClientTransport` | One bidirectional gRPC stream per client over HTTP/2. |

Rules worth knowing:

- **Handlers run on the connection's receive loop.** Messages from one peer are handled in order, one at a time. A
  server runs handlers for different clients concurrently, so server-side handlers must be thread-safe. Keep handlers
  short: a slow handler delays that peer's later messages, but nobody else's.
- **`Publish` never blocks.** Each peer has its own bounded queue. When it is full, `SlowConsumerPolicy` decides what
  happens:
  - **`DropOldest` (the default)** discards the oldest queued *published* message. The consumer sees gaps but stays
    current.
  - **Only published messages are ever dropped.** Responses, requests and subscribe/unsubscribe frames always get
    through, so a slow consumer never sees a request time out for no reason or a subscription silently fail.
  - **Drops are logged as one aggregated warning per peer per second**, naming the endpoint, never one line per
    message. See the [sample warning](docs/ARCHITECTURE.md#slow-consumers-and-flow-control). The running total is
    `Peer.DroppedFrames`.
  - **`Disconnect`** closes a consumer that can't keep up instead, for streams where gaps are unacceptable.
  - Publishers that would rather throttle can watch `Peer.QueuedFrames`. The demo server does this in its benchmark.
- **A message object can be reused after `Publish` returns**, because it has already been serialized.
- **`RequestAsync` returns a pooled `ValueTask`.** Await it exactly once.
- **Clients reconnect automatically.** After a dropped connection, the client retries with exponential backoff and
  jitter (100 ms rising to 5 s by default). It restores every subscription, then raises `Reconnected`.
  - While disconnected, `Publish` and `RequestAsync` throw `MessagingException` straight away rather than queueing:
    sending an order minutes late is worse than rejecting it.
  - Requests that were in flight when the connection dropped fail rather than being resent, because the server may
    already have acted on them.
  - Messages published during the outage are not replayed. Use `Reconnected` to refresh state.
  - The first `ConnectAsync` does not retry, so a wrong address fails immediately.

## Performance

These figures come from the demo client and server on one Windows 11 machine (20 cores, loopback), .NET 10, Release
build. They use the protobuf-net DTOs in `Light.Messaging.Contracts`.

| Benchmark | TCP | gRPC |
|---|---:|---:|
| Streaming server→client (2M `PriceTick`s) | **3.06M msg/s** | **3.49M msg/s** |
| Request/response, 256 in flight | **1.35M req/s** | **1.01M req/s** |
| Request/response, one at a time | 15,000 req/s | 8,100 req/s |
| Sequential round-trip latency p50 / p99 / p99.9 | 57 / 169 / 285 µs | 128 / 283 / 422 µs |

These are best-case figures from a quiet machine. With other heavy processes running (a virtual machine, IDEs),
the same benchmark measured 20–30% lower and noticeably noisier, so compare runs made under the same conditions.

For comparison, the Dcs.Messaging throughput test measured about **50k msg/s over TCP and 98k msg/s over gRPC**.
That test used a different harness and message, so treat it as an order-of-magnitude comparison.

Where the gains come from (details in [the architecture document](docs/ARCHITECTURE.md)):

| | Dcs.Messaging | Light.Messaging |
|---|---|---|
| Header per message | ~120 bytes: endpoint string, sender session GUID, property dictionary | **9 bytes**, or 17 with a correlation id |
| Sending | Thread-pool hop and blocking socket write per message, behind a lock | **Enqueue only**; a writer loop per peer batches everything queued into one write |
| Fan-out | Every message to every connection, re-encoded per send | **Only to subscribed peers, serialized once**, shared by reference; nothing serialized when nobody subscribes |
| Slow subscriber | Stalls the broadcast loop and the thread pool | **Affects only itself**; bounded queue, drop-oldest, aggregated warning |
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
- **No replay or sequence-based gap recovery.** Reconnect restores subscriptions, not missed messages.
- **No TLS**, deliberately, for latency. Run it on a trusted, segmented network. gRPC accepts an `https://` client
  address, but the server listens in cleartext (h2c).
- **Request/response runs client to server only.**
- **Latency figures are same-host.** They rely on `Stopwatch` timestamps, which can't be compared across machines.
