# Dcs.DataTransport

A small publish/subscribe library, independent of `Dcs.Messaging`, built to show how to move typed messages from one
server to many subscribers over **TCP** or **gRPC** with performance as the main concern.

```csharp
// Server
await using var server = new DataServer<PriceTick>(PriceTickCodec.Instance, new DataServerOptions
{
    Transport = TransportKind.Tcp,          // or TransportKind.Grpc
    Port = 5050,
    SubscriberQueueCapacity = 8192,
    SlowSubscriberPolicy = SlowSubscriberPolicy.DropOldest,
});
await server.StartAsync();
server.Publish(new PriceTick(...));         // never blocks, does no I/O

// Client
var client = new DataClient<PriceTick>(PriceTickCodec.Instance, new DataClientOptions { Transport = TransportKind.Tcp, Port = 5050 });
await client.RunAsync(tick => Handle(tick), cancellationToken);
```

Messages are encoded by an `IMessageCodec<T>`, which writes to and reads from spans. See
`Dcs.DataTransport.Demo.Shared/PriceTickCodec.cs` for a fixed-layout example.

## Design

| Decision | Why |
|---|---|
| One **bounded queue and one async writer loop per subscriber** | `Publish` only enqueues, so a subscriber that reads slowly fills its own queue and nobody else's. |
| **`SlowSubscriberPolicy`** (`DropOldest`, `DropNewest`, `Disconnect`) | A slow subscriber costs bounded memory, and you choose whether it sees gaps or gets cut off. |
| **Opportunistic batching**: each write sends everything already queued, but never waits for more | One message per write when lightly loaded (no added latency); hundreds per write under load (high throughput). |
| **`NoDelay` on**, pinned buffers reused per connection, frames decoded in place | No Nagle delay, no per-message allocation, no copying on receive. |
| Fixed **4-byte length-prefixed frames**, written by the codec straight into the send buffer | No envelope object, no reflection-based serializer. |
| gRPC: **hand-written `Method<,>` and marshallers** (no protoc); many frames carried per gRPC message | Avoids a protobuf layer and amortises the HTTP/2 and gRPC cost of each message. |
| **Send-stall watchdog** (`SendStallTimeout`) | A peer that stops reading but keeps the socket open is disconnected instead of hanging forever. |
| Ordered delivery: a single writer per subscriber, and `Publish` enqueues synchronously | Messages from one publishing thread arrive in order. |

## Running the demo

```powershell
.\scripts\Run-DataTransportDemo.ps1            # -Duration 10 -Rate 200000 -Queue 8192 -SlowWorkUs 20
```

The script runs `Dcs.DataTransport.DemoServer` and `Dcs.DataTransport.DemoClient` for each transport and scenario:

- `all-fast`: 3 fast subscribers at 200k msg/s.
- `one-slow`: 2 fast subscribers plus 1 slow one that spends 20 µs of CPU per message (so it handles at most about 50k msg/s).
- `max-rate`: the same subscribers with the publisher unthrottled.

The apps can also be run by hand; see the `--options` in each `Program.cs`.

## Results (loopback, 20-core Windows 11 machine, .NET 10, Release)

Latency is measured from publish to the client handler. These figures exclude the first 100k messages (warm-up);
the simplified demo client now measures each subscriber's last 1M messages instead, which has the same effect on
long runs. Latency is only valid with the server and client on the same host.

| Scenario | Transport | Subscriber | Msg/s | Missed | p50 µs | p99 µs | p99.9 µs |
|---|---|---|---:|---:|---:|---:|---:|
| all-fast | TCP | fast ×3 | 200,068 | 0 | 27.6 | 164–295 | 5,767–9,437 |
| one-slow | TCP | fast ×2 | 200,061 | 0 | 25.6 | 148–188 | 2,032–9,962 |
| one-slow | TCP | slow | 49,251 | 1,474,572 | 503,316 | 603,980 | 671,089 |
| max-rate | TCP | fast ×2 | **4,654,001–4,668,941** | 9,328–158,780 | 49.2 | 442–475 | 1,245–3,277 |
| all-fast | gRPC | fast ×3 | 200,556 | 0 | 59.4 | 8,913–11,534 | 16,777–18,874 |
| one-slow | gRPC | fast ×2 | 200,208–200,384 | 0 | 53.2 | 9,437–11,534 | 15,729–17,826 |
| one-slow | gRPC | slow | 49,187 | 1,496,853 | 121,635 | 176,161 | 243,270 |
| max-rate | gRPC | fast ×2 | **2,635,156–2,635,513** | ~4.76M | 73.7 | 3,932–4,194 | 15,729–17,826 |

For comparison, the `Dcs.Messaging` TCP transport managed about 50k msg/s in its own throughput test.

What the numbers show:

- **The slow subscriber does not affect the fast ones, on either transport.** The fast subscribers' throughput and
  latency are the same with and without it, and only the slow subscriber misses messages.
- **TCP is the faster transport**: about 1.8× gRPC's maximum throughput, half its median latency, and far lower p99.
  The server allocates under 1 byte per message, with no GCs during the run.
- **gRPC's p99 at 200k msg/s per stream is about 10 ms.** This is not GC (a larger gen-0 budget cut GCs from 31 to 3
  with no change) and not the HTTP/2 stream window (a 1 MB window made no difference). It comes from occasional
  stalls of up to ~15 ms inside the server's `WriteAsync`, in the HTTP/2 send path, when messages go out one per gRPC
  message. At 50k msg/s per stream the p99 drops to about 155 µs.
- **A slow subscriber's data gets stale.** With `DropOldest` it still receives the newest data, but behind its queue
  plus the kernel socket buffers: about 0.5 s on TCP and about 0.12 s on gRPC, where HTTP/2 flow control buffers less.
  To keep a slow consumer fresher, use a smaller queue, set smaller `SocketSendBufferBytes` and
  `SocketReceiveBufferBytes`, or conflate (see below).

## Not included, and natural next steps

- **Conflation per key** (keep only the latest price per currency pair in a slow subscriber's queue). This is the usual
  market-data policy and gives a slow consumer the freshest possible data.
- **Encode once for all subscribers**: publish into shared, reference-counted segments instead of encoding per
  subscriber. Worth doing when there are many subscribers or messages are expensive to encode.
- **UDP multicast / Aeron** for one-to-many on a LAN: the cost to the sender doesn't grow with the number of subscribers.
- TLS, authentication, reconnect with recovery (sequence numbers and snapshots), and client-to-server messages.
