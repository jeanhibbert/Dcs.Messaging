# LabLegacy.Neutron

A modernised version of a legacy TCP/protobuf server and its per-client connection class, renamed to
`NeutronAsyncServer<T>` and `NeutronClientConnection<T>`. The public members and events are the same as the
originals, and so is the wire format, so existing clients connect unchanged.

It also has a client the legacy code did not: `NeutronClient<T>`, which reconnects automatically when the server
stops and starts, and reports through Reactive Extensions observables instead of events.

For how it works inside (wire format, send and receive paths, threading, connection lifecycle, reconnecting), see
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md).

## Projects

| Project | What it is |
|---|---|
| `LabLegacy.Neutron` | The library: `NeutronAsyncServer<T>`, `NeutronClientConnection<T>`, `NeutronClient<T>` and their contracts. |
| `LabLegacy.Neutron.Tests` | Server tests with a legacy-style client on the other end, and client tests against the real server. |
| `LabLegacy.Neutron.SampleMessages` | The message type the two samples share. |
| `LabLegacy.Neutron.SampleServer` | Sample server: heartbeats, a price broadcast, and an acknowledgement for each order. |
| `LabLegacy.Neutron.SampleClient` | Sample client built on `NeutronClient<T>`, with a pass/fail check at the end. |

## Using the client

```csharp
using var client = new NeutronClient<SampleMessage>(new NeutronClientConfig
{
    ServerAddress = new IPEndPoint(IPAddress.Loopback, 5080),
    HeartBeatTimeout = TimeSpan.FromSeconds(6),   // a few times the server's heartbeat interval
});

client.Messages.Where(message => message.Kind == SampleMessageKind.Price).Subscribe(HandlePrice);
client.State.Subscribe(state => Console.WriteLine(state));      // Disconnected, Connecting, Connected
client.Errors.Subscribe(error => Console.WriteLine(error.Message));

client.Start();
client.SendMessage(order);   // false while there is no connection
```

- It reconnects with a backoff (100 ms doubling to 5 s) for as long as it is not disposed.
- Nothing is replayed after a reconnect. Subscribe to `State` and send whatever the server needs again each time
  it becomes `Connected`.
- With `HeartBeatTimeout` set, a server that goes silent without closing the socket is also detected.

## Running the samples

Start the server, then the client, in two terminals. Both take an optional port (default 5080) and a number of
seconds to run:

```powershell
dotnet run -c Release --project LabLegacy/LabLegacy.Neutron.SampleServer -- 5080
dotnet run -c Release --project LabLegacy/LabLegacy.Neutron.SampleClient -- 5080 12
```

The server heartbeats every 2 seconds, broadcasts a price twice a second, and acknowledges each order to the client
that sent it. The client sends three orders each time it connects, listens for the given time, and checks what it
received. Its exit code is 0 when every check passes.

This is the client's output from a run in which the server was stopped after 4 seconds and started again 2.5
seconds later:

```
[Disconnected]
[Connecting]
[Connected]
  sent 3 orders
  order #1 accepted
  order #2 accepted
  order #3 accepted
  price #2 EURUSD 1.08547
  price #3 EURUSD 1.08525
  heartbeat
  price #4 EURUSD 1.08487
  heartbeat
[Connecting]
  ! No connection could be made because the target machine actively refused it.
[Connected]
  sent 3 orders
  order #1 accepted
  order #2 accepted
  order #3 accepted
  heartbeat
  heartbeat
  heartbeat
[Disconnected]

PASS  connected (2 times)
PASS  every order acknowledged (3/3)
PASS  prices received (19) and in order
PASS  heartbeats received (5)
```

The restarted server's side of the same run:

```
info: LabLegacy.Neutron.NeutronAsyncServer[0] Starting Neutron Listener on 127.0.0.1:5281
Listening on 127.0.0.1:5281. Press Ctrl+C to stop.
+ client connected: 60053610-80de-43b7-8f31-b06741388981_127.0.0.1:58784
  order #1 buy 1M EURUSD @ 1.08460
  order #2 buy 1M EURUSD @ 1.08470
  order #3 buy 1M EURUSD @ 1.08480
info: LabLegacy.Neutron.NeutronAsyncServer[0] connection_id:60053610-80de-43b7-8f31-b06741388981_127.0.0.1:58784 has stopped
- client disconnected: 60053610-80de-43b7-8f31-b06741388981_127.0.0.1:58784
```

## Wire format (unchanged)

Each message is a Base128 (varint) length followed by the protobuf-net payload. This is exactly what the legacy code
produced and consumed with:

```csharp
Serializer.SerializeWithLengthPrefix(stream, message, PrefixStyle.Base128);
Serializer.DeserializeWithLengthPrefix<T>(stream, PrefixStyle.Base128);
```

The server tests use those two calls as the client, so they prove compatibility in both directions.

## What changed and why

| Legacy | Now | Why |
|---|---|---|
| `SendMessage` wrote to the `NetworkStream` on the caller's thread, with no lock | `SendMessage` queues; one write loop per connection owns the socket | Concurrent senders (including the heartbeat timer) could interleave bytes and corrupt the stream. Now they can't. |
| A blocking write per message | The write loop sends everything queued in one socket call | Under load, many messages share a system call. A lone message still goes out immediately. |
| Broadcast serialized the message once per client, and wrote to each socket in turn | Serialized once into a pooled buffer shared by all clients; the broadcast only queues | Less CPU and garbage, and one slow client no longer delays the caller or the other clients. |
| No limit on a slow client | Bounded send queue (`SendQueueCapacity`); a client that falls that far behind is disconnected and logged | Bounded memory, and a stuck client can't stall the server. |
| Blocking `DeserializeWithLengthPrefix` inside `Task.Run` | Asynchronous `PipeReader`; messages are deserialized straight from the receive buffer | No thread is parked per connection, and one socket read usually yields many messages. |
| No limit on incoming message size | `MaxMessageBytes` (default 16 MB) | A corrupt or hostile length prefix can't make the server allocate without bound. |
| A `System.Timers.Timer` per connection | One `PeriodicTimer` in the server; each heartbeat is serialized once | Fewer timers, and the heartbeat goes through the same safe queue as everything else. |
| Accept loop with no error handling or cancellation | Cancellable accept loop that logs a failed accept, raises `OnServerError` and carries on | `Stop()` ends it cleanly, and one bad accept doesn't kill the listener. |
| `TcpListener` / `TcpClient` | `Socket` with `NoDelay` | Direct access to the efficient `Memory`-based socket calls. |

## Behaviour to be aware of

- **`SendMessage` returns once the message is queued**, not once it is written. `true` means queued. `false` means
  the connection is closed, the id is unknown, or the client's queue was full (it is then disconnected).
- **Serialization errors still throw** from `SendMessage`, as before.
- **Received messages** are passed to `IServerProtoMessageParser<T>.ReceivedMsgAsync` one at a time per connection, in
  order, as before. Different connections call it concurrently.
- **`ListenerAddress`** becomes the address actually bound once the server has started, so port 0 can be used.

## What was inferred

The photos of the original code showed the server and the connection class only. These supporting types were
reconstructed from how they were used, and should be checked against the originals:

- `IBaseMessage`
- `IServerProtoMessageParser<T>`
- `INeutronAsyncServerCallBack<T>`
- `INeutronClientConnection<T>`
- `INeutronAsyncServer<T>`
- `NeutronServerConfig<T>`, which also gains `SendQueueCapacity` and `MaxMessageBytes`

Other differences from the original:

- **Names.** The types live in the `LabLegacy.Neutron` namespace, and every type name uses the `Neutron` prefix in
  place of the original one.
- **Connection constructor.** `NeutronClientConnection<T>` takes a `Socket`, the config and a logger. The heartbeat
  parameters are gone, because the server now sends heartbeats.
- **Logging.** The connection logs through `ILogger` instead of `Trace`.

## Tests

```powershell
dotnet test LabLegacy/LabLegacy.Neutron.Tests -c Release
```

The client tests cover:

- messages in both directions, in order
- 4 concurrent senders with no corruption
- the sequence of states, and completion on dispose
- reconnecting after the server stops and starts again on the same port
- retrying, with each failure reported, until a server appears
- a silent connection being replaced after the heartbeat timeout
- heartbeats keeping a connection alive

The server tests cover:

- ordering of messages from a client
- broadcast and targeted sends
- 8 concurrent senders with no corruption
- 3 MB messages in both directions
- heartbeats
- disconnect reporting
- a slow client being dropped while a healthy one keeps receiving
- `Stop()`
- start failures, thrown or reported as configured
