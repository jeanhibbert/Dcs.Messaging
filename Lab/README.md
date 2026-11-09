# Light.Messaging (Lab)

Light.Messaging is a bare-bones, performance-first reimplementation of the Dcs.Messaging programming model. It moves
protobuf messages between processes on an internal network over **TCP** or **gRPC**. It keeps the concepts
Dcs.Messaging users know (named endpoints, publish/subscribe, targeted sends, request/response, protobuf-net DTOs)
and drops everything that costs time on the hot path.

Like Dcs.Messaging, it presents incoming messages and lifecycle events as **Reactive Extensions** (`IObservable<T>`)
streams. Unlike Dcs.Messaging, Rx is the composition surface rather than the transport plumbing: the per-message
path has no Rx operators, subjects or locks unless you choose to use them.

It shares no code with Dcs.Messaging and can live alongside it.

- [Projects](#projects)
- [Quick start](#quick-start)
- [Running the demo](#running-the-demo)
- [API at a glance](#api-at-a-glance)
- [Reactive Extensions](#reactive-extensions)
- [Performance](#performance)
- [Coming from Dcs.Messaging](#coming-from-dcsmessaging)
- [Limitations](#limitations)
- [Architecture, wire protocol and tuning](docs/ARCHITECTURE.md)

## Projects

| Project | What it is | Depends on |
|---|---|---|
| `Light.Messaging` | Core: endpoints, serializer, framing, connections, `MessagingServer`, `MessagingClient`, TCP transport | protobuf-net, System.Reactive, Microsoft.Extensions.Logging.Abstractions |
| `Light.Messaging.Grpc` | gRPC transport (`GrpcServerTransport`, `GrpcClientTransport`) on embedded Kestrel | Light.Messaging, ASP.NET Core, Grpc.AspNetCore.Server, Grpc.Net.Client |
| `Light.Messaging.Contracts` | Demo DTOs and endpoints shared by the two apps | Light.Messaging |
| `Light.Messaging.ServerApp` | Demo server | Contracts, Grpc |
| `Light.Messaging.ClientApp` | Demo client and benchmark | Contracts, Grpc |
| `Light.Messaging.Tests` | Integration tests over both transports, plus Rx tests using `TestScheduler` | Light.Messaging, Grpc, Microsoft.Reactive.Testing |

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
using var trades = server.ObserveWithSender<TradeReport>(Endpoints.Trades).Subscribe(trade => Book(trade.Message, trade.Sender));
using var joins = server.PeerConnected.Subscribe(peer => Console.WriteLine($"{peer.Name} joined"));

server.Publish(Endpoints.Prices, tick);                 // every subscribed client
server.Publish(Endpoints.Prices, tick, somePeer);       // one client
```

Client:

```csharp
await using var client = await MessagingClient.ConnectAsync(
    new TcpClientTransport("10.0.0.5", 5070),           // or new GrpcClientTransport("10.0.0.5", 5071)
    new MessagingClientOptions { Name = "trader-ui-7", Logger = loggerFactory.CreateLogger("Light.Messaging") });

using var prices = client.Observe<PriceTick>(Endpoints.Prices)
    .Where(tick => tick.CurrencyPair == "EURUSD")
    .Sample(TimeSpan.FromMilliseconds(250))             // at most four screen updates a second
    .Subscribe(Show);

var price = await client.RequestAsync<PricingRequest, ForwardPrice>(Endpoints.Pricing, request);
client.Publish(Endpoints.Trades, trade);

// Reconnect is automatic and subscriptions are restored. Anything published during the outage is gone, so refresh.
using var refresh = client.Reconnected.Subscribe(_ => RequestSnapshot());
```

## Running the demo

```powershell
dotnet run -c Release --project Lab/Light.Messaging.ServerApp -- --transport tcp --port 5070
dotnet run -c Release --project Lab/Light.Messaging.ClientApp -- --transport tcp --port 5070
```

Use `--transport grpc` on both for gRPC. The client runs these steps:
1. Observes live prices for a second, as an Rx stream.
2. Prices a forward with a request/response call, using the observable `Request` with `Retry`.
3. Publishes a trade report, which the server logs.
4. Runs three benchmarks: sequential request latency, pipelined request throughput, and streaming throughput.

Client options are `--host`, `--port`, `--requests 20000` and `--burst 2000000`.

To watch reconnect, run the client with `--watch true` instead. It skips the benchmarks and prints the live price
rate every second. Stop or restart the server and the client logs the disconnect and each retry, then reconnects and
restores its subscription on its own:

```
   27 prices/s   connected
20:45:17.406 warn: Light.Messaging[0] Connection to lab-server ([::ffff:127.0.0.1]:5196) lost (An existing connection was forcibly closed by the remote host.); reconnecting
  status: Reconnecting
    0 prices/s   reconnecting
20:45:19.512 warn: Light.Messaging[0] Reconnect attempt 1 failed (No connection could be made because the target machine actively refused it.); retrying in about 200 ms
20:45:20.753 info: Light.Messaging[0] Reconnected to lab-server ([::ffff:127.0.0.1]:5196) after 2 attempt(s); restored 1 subscription(s)
  status: Connected
   30 prices/s   connected
```

The rate line is `Observe(...).Buffer(TimeSpan.FromSeconds(1))`, and the status lines come from `client.Status`.

Run the tests with:

```powershell
dotnet test Lab/Light.Messaging.Tests -c Release
```

## API at a glance

| Type | Role |
|---|---|
| `Endpoint` | Named destination. Only its 32-bit id (FNV-1a of the address) goes on the wire. |
| `MessagingServer` | `StartAsync`, `Publish` (to subscribers or one peer), `Subscribe`, `Observe` / `ObserveWithSender`, `Respond` / `RespondAsync`, `Peers`; streams `PeerConnected`, `PeerDisconnected`, `HandlerErrors`, `SlowConsumers`. |
| `MessagingClient` | `ConnectAsync`, `Subscribe`, `Observe`, `Publish`, `RequestAsync` / `Request` (observable), `IsConnected`, `Completion`; streams `Status`, `Reconnected`, `HandlerErrors`, `SlowConsumers`. |
| `MessageContext` | Passed to handlers; `Sender` is the `Peer` the message came from. |
| `Peer` | Id, name, address, connection state, close reason, `QueuedFrames` (for flow control), `DroppedFrames`. |
| `MessagingServerOptions` / `MessagingClientOptions` | Name, `Logger`, `Scheduler`, serializer, queue capacity, `SlowConsumerPolicy`, frame and batch sizes, timeouts; on the client also `AutoReconnect`, `ReconnectMinDelay`, `ReconnectMaxDelay` and `MaxReconnectAttempts`. |
| `ConnectionStatus`, `SlowConsumerReport` | What the `Status` and `SlowConsumers` streams carry. |
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
  - **Drops are reported once per peer per second**, never once per message. Each report is logged as a warning
    naming the endpoint, and also published on the `SlowConsumers` stream. See the
    [sample warning](docs/ARCHITECTURE.md#slow-consumers-and-flow-control). The running total is
    `Peer.DroppedFrames`.
  - **`Disconnect`** closes a consumer that can't keep up instead, for streams where gaps are unacceptable.
  - Publishers that would rather throttle can watch `Peer.QueuedFrames`. The demo server does this in its benchmark.
- **A message object can be reused after `Publish` returns**, because it has already been serialized.
- **`RequestAsync` returns a pooled `ValueTask`.** Await it exactly once.
- **Clients reconnect automatically.** After a dropped connection, the client retries with exponential backoff and
  jitter (100 ms rising to 5 s by default). It restores every subscription, then publishes `Connected` on `Status`,
  which also emits on `Reconnected`.
  - While disconnected, `Publish` and `RequestAsync` throw `MessagingException` straight away rather than queueing:
    sending an order minutes late is worse than rejecting it.
  - Requests that were in flight when the connection dropped fail rather than being resent, because the server may
    already have acted on them.
  - Messages published during the outage are not replayed. Use `Reconnected` to refresh state.
  - The first `ConnectAsync` does not retry, so a wrong address fails immediately.

## Reactive Extensions

Everything that happens over time is an `IObservable<T>`:

| Stream | Emits | Completes |
|---|---|---|
| `Observe<T>(endpoint)` / `ObserveWithSender<T>(endpoint)` | Each message received on the endpoint (with its sender) | When the session is disposed |
| `server.PeerConnected` / `server.PeerDisconnected` | Each client as its handshake completes / its connection closes | When the server is disposed |
| `client.Status` | The current `ConnectionStatus` first, then every change: Connected, Reconnecting, Connected... | With a final Closed |
| `client.Reconnected` | The new server `Peer` after each reconnect, once subscriptions are restored | With `Status` |
| `HandlerErrors` | Exceptions thrown by your handlers or Rx subscribers | When the session is disposed |
| `SlowConsumers` | A `SlowConsumerReport` per peer per watchdog check while messages are being dropped | When the session is disposed |
| `client.Request<TRequest, TResponse>(...)` | The response, once. Cold: each subscription sends the request, so `.Retry()` and `.Timeout()` behave as expected | After the response |

What the library guarantees:

- **Subscribing is lazy.** An `Observe` stream subscribes on the server when the first observer subscribes. When the
  last observer goes away (`Dispose`, `Take`, `FirstAsync`, `TakeUntil`...), the server stops sending, and stops
  serializing the message too if nobody else wants it.
- **OnNext calls never overlap**, as Rx requires, even on a server receiving from many clients at once.
- **A failure doesn't end the stream.** A message that fails to deserialize, or a subscriber that throws, is reported
  on `HandlerErrors`. It does not end a long-lived feed with `OnError`.
- **The library's own logging is a subscriber** to `PeerConnected`, `PeerDisconnected`, `HandlerErrors` and
  `SlowConsumers`.
- **Time runs on `MessagingOptions.Scheduler`.** That covers the watchdog, request timeouts and reconnect backoff.
  Tests can pass Rx's `TestScheduler` and move time forward instead of waiting: `RequestTimeoutRunsInVirtualTime`
  checks a 30-second timeout instantly.

When to use which:

- **`Observe`:** for composition (filtering, sampling, buffering, joining streams). Its per-message overhead is one
  observer call, plus one uncontended lock per message on a server.
- **`Subscribe`:** keep the delegate form, and `RequestAsync` with its pooled `ValueTask`, for paths that handle
  millions of messages a second. The demo client's streaming benchmark uses `Subscribe` for this reason, while the
  rest of the demo uses Rx.

```csharp
// Fresh prices for one pair, at most four a second, re-priced through the server; retried once on failure.
using var quotes = client.Observe<PriceTick>(Endpoints.Prices)
    .Where(tick => tick.CurrencyPair == "EURUSD")
    .Sample(TimeSpan.FromMilliseconds(250))
    .SelectMany(tick => client.Request<PricingRequest, ForwardPrice>(Endpoints.Pricing, ToRequest(tick)).Retry(1))
    .Subscribe(Show);

// Page operations while a consumer is losing messages: the latest report per consumer, at most once a minute.
using var alerts = server.SlowConsumers
    .GroupBy(report => report.Peer)
    .SelectMany(perPeer => perPeer.Sample(TimeSpan.FromMinutes(1)))
    .Subscribe(report => PageOperations(report));
```

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
| Receive | Global lock, `Where` filter per subscription per message | **Lookup table, no lock**; decoded straight from the receive buffer; Rx only where you compose |
| Request/response | GUID strings, Rx pipeline, `Task` per request | **64-bit correlation id, pooled `ValueTask` source** |
| Allocation | `MemoryStream` + `ToArray` + envelope per message | **Pooled frames and buffers**; the serializer writes into the frame |

## Coming from Dcs.Messaging

| Dcs.Messaging | Light.Messaging |
|---|---|
| `IEndpointDetails`, `IEndpointDetailsProvider`, endpoint keys | `Endpoint` constants in a shared contracts assembly |
| `TcpMessagingSessionBuilder` / `GrpcMessagingSessionBuilder` | `MessagingServer.StartAsync(transport)` / `MessagingClient.ConnectAsync(transport)` |
| `IMessagingService.Send(message, endpoint)` | `server.Publish(endpoint, dto)` / `client.Publish(endpoint, dto)` |
| `Send(..., targetSessionId)` | `server.Publish(endpoint, dto, peer)` |
| `GetMessageStream(endpoint)` + Rx `Subscribe` | `Observe<T>(endpoint)` returns `IObservable<T>` directly; or `Subscribe<T>(endpoint, handler)` on hot paths |
| `IRequestResponder.GetRespondableRequestStream` + `Respond` | `server.Respond<TRequest, TResponse>(endpoint, handler)` |
| Hand-rolled correlation id, `Where` and `Take(1)` on the response stream | `await client.RequestAsync<TRequest, TResponse>(endpoint, request)`, or `client.Request(...)` as an observable |
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
