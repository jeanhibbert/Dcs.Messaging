# LabLegacy.Neutron

A modernised version of a legacy TCP/protobuf server and its per-client connection class, renamed to
`NeutronAsyncServer<T>` and `NeutronClientConnection<T>`. The public members and events are the same as the
originals, and so is the wire format, so existing clients connect unchanged.

For how it works inside (wire format, send and receive paths, threading, connection lifecycle), see
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md).

## Projects

| Project | What it is |
|---|---|
| `LabLegacy.Neutron` | The library: `NeutronAsyncServer<T>`, `NeutronClientConnection<T>` and their contracts. |
| `LabLegacy.Neutron.Tests` | Tests, with a legacy-style client on the other end of every connection. |
| `LabLegacy.Neutron.SampleMessages` | The message type the two samples share. |
| `LabLegacy.Neutron.SampleServer` | Sample server: heartbeats, a price broadcast, and an acknowledgement for each order. |
| `LabLegacy.Neutron.SampleClient` | Sample client written the way a legacy client is, with a pass/fail check at the end. |

## Running the samples

Start the server, then the client, in two terminals. Both take an optional port (default 5080) and a number of
seconds to run:

```powershell
dotnet run -c Release --project LabLegacy/LabLegacy.Neutron.SampleServer -- 5080
dotnet run -c Release --project LabLegacy/LabLegacy.Neutron.SampleClient -- 5080 5
```

The server heartbeats every 2 seconds, broadcasts a price twice a second, and acknowledges each order to the client
that sent it. The client sends three orders, listens for 5 seconds, and checks what it received. Its exit code is 0
when every check passes:

```
Connected to 127.0.0.1:5281
Sent 3 orders; listening for 5 s
  order #1 accepted
  price #1 EURUSD 1.08474
  order #2 accepted
  order #3 accepted
  price #2 EURUSD 1.08530
  price #3 EURUSD 1.08547
  heartbeat
  heartbeat

PASS  every order acknowledged (3/3)
PASS  prices received (10) and in order
PASS  heartbeats received (2)
```

The server's side of the same run:

```
info: LabLegacy.Neutron.NeutronAsyncServer[0] Starting Neutron Listener on 127.0.0.1:5281
+ client connected: 1d6aaa60-be22-4854-86e9-111d1a2d0a70_127.0.0.1:55714
  order #1 buy 1M EURUSD @ 1.08460
  order #2 buy 1M EURUSD @ 1.08470
  order #3 buy 1M EURUSD @ 1.08480
info: LabLegacy.Neutron.NeutronAsyncServer[0] connection_id:1d6aaa60-be22-4854-86e9-111d1a2d0a70_127.0.0.1:55714 has stopped
- client disconnected: 1d6aaa60-be22-4854-86e9-111d1a2d0a70_127.0.0.1:55714
```

## Wire format (unchanged)

Each message is a Base128 (varint) length followed by the protobuf-net payload. This is exactly what the legacy code
produced and consumed with:

```csharp
Serializer.SerializeWithLengthPrefix(stream, message, PrefixStyle.Base128);
Serializer.DeserializeWithLengthPrefix<T>(stream, PrefixStyle.Base128);
```

The tests use those two calls as the client, so they prove compatibility in both directions.

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

They cover:

- ordering of messages from a client
- broadcast and targeted sends
- 8 concurrent senders with no corruption
- 3 MB messages in both directions
- heartbeats
- disconnect reporting
- a slow client being dropped while a healthy one keeps receiving
- `Stop()`
- start failures, thrown or reported as configured
