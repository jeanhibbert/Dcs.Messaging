# LabLegacy.Neutron

A modernised version of a legacy TCP/protobuf server and its per-client connection class, renamed to
`NeutronAsyncServer<T>` and `NeutronClientConnection<T>`. The public members and events are the same as the
originals, and so is the wire format, so existing clients connect unchanged.

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
