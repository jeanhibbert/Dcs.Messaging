# LabEnhanced.Neutron

An enhanced version of `LabOriginal.Neutron`, the small TCP + protobuf-net messaging framework. It keeps the
original's interfaces and wire format, fixes its defects, and is 10–130x faster under load.

This document covers what is in the folder, what stayed the same, what was fixed, what behaves differently, how it
works inside, and the benchmark that compares the two. For the same changes broken down by class and method, see
[METHOD-BY-METHOD.md](METHOD-BY-METHOD.md).

## Projects

| Project | What it is |
|---|---|
| `LabEnhanced.Neutron` | The library: `ProtobuffAsyncServer<T>`, `ProtobuffClientConnection<T>`, `ProtobufAsyncClient<T>`, their configs and interfaces. |
| `LabEnhanced.Neutron.Tests` | 15 tests against the real server and client over loopback, including two that pair each end with its `LabOriginal` counterpart. |
| `LabEnhanced.Benchmarks` | A console benchmark that runs `LabOriginal` and `LabEnhanced` through the same scenarios and prints them side by side. |

```powershell
dotnet test LabEnhanced/LabEnhanced.Neutron.Tests -c Release
dotnet run -c Release --project LabEnhanced/LabEnhanced.Benchmarks
```

## What stayed the same

**Interfaces.** Every interface in `LabOriginal.Neutron` exists in `LabEnhanced.Neutron` with the same name and the
same members:

- `IBaseMessage`
- `IClientConfig<T>`
- `IClientProtoMessageParser<T>`
- `IClientProtoAsyncParser<T>` (in the `.Client` namespace, as before)
- `IServerProtoMessageParser<T>`
- `IProtobufClient<T>`
- `IProtobuffAsyncServer<T>`
- `IProtobuffAsyncServerCallBack<T>`
- `IProtobuffClientConnection<T>`

The one difference is deliberate: events are now Reactive Extensions observables with the same names.

| Member | Original | Enhanced |
|---|---|---|
| `IProtobufClient.Connected` / `Disconnected` | `event EventHandler` | `IObservable<Unit>` |
| `IProtobuffAsyncServer.OnClientConnection` / `OnClientDisconnection` | `event EventHandler<IProtobuffClientConnection<T>>` | `IObservable<IProtobuffClientConnection<T>>` |
| `IProtobuffAsyncServer.OnServerError` | `event EventHandler<Exception>` | `IObservable<Exception>` |
| `IProtobuffClientConnection.OnClientDisconnection` | `event EventHandler<IProtobuffClientConnection<T>>` | `IObservable<IProtobuffClientConnection<T>>` |

So `server.OnClientConnection += Handler;` becomes `server.OnClientConnection.Subscribe(Handler);`.

**Class names and constructors.** `ProtobuffAsyncServer<T>`, `ProtobufAsyncClient<T>`, `ProtobuffServerConfig<T>` and
`ProtobuffClientConfig<T>` keep their names and constructor signatures. Only `ProtobuffClientConnection<T>`'s
constructor changed (see [Behaviour to be aware of](#behaviour-to-be-aware-of)).

**Wire format.** Each message is a Base128 (varint) length followed by the protobuf-net payload, exactly what the
original produces and consumes with:

```csharp
Serializer.SerializeWithLengthPrefix(stream, message, PrefixStyle.Base128);
Serializer.DeserializeWithLengthPrefix<T>(stream, PrefixStyle.Base128);
```

Request/reply correlation through `IBaseMessage.AsyncId` and `AsyncSend` is also unchanged. Two tests prove the
compatibility: an original client against the enhanced server, and an enhanced client against the original server.

## Issues fixed

| Issue in LabOriginal | Now |
|---|---|
| **Stream corruption.** `SendMessage` wrote to the socket on the caller's thread with no lock, so concurrent senders, or a sender and the heartbeat timer, could interleave bytes. | Sending only queues. One flush per connection owns the socket. |
| **One system call per message sent, and several per message read** (the length prefix was read a byte at a time). | Everything queued goes out in one write, and every message a socket read delivers is parsed straight from the receive buffer. |
| **A thread blocked per connection**, parked in a blocking read inside `Task.Run`. | Receiving is asynchronous; no thread waits for data. |
| **Broadcast serialized the message once per client and wrote to each socket in turn**, so one slow client delayed the caller and every other client. | Serialized once; the broadcast only queues and never waits on a socket. |
| **No limit on a slow peer.** A peer that stopped reading blocked the sender indefinitely. | `MaxSendBacklogBytes` (default 16 MB): a peer that falls that far behind is disconnected. |
| **No limit on incoming message size.** A corrupt or hostile length prefix could make the receiver allocate without bound. | `MaxMessageBytes` (default 16 MB): a larger prefix closes the connection. |
| **Disconnection raised twice** on both ends: the client from `Disconnect()` and again from the read loop; the server from the connection's event and again from `RemoveClientConnection`. | Each is published exactly once. |
| **`SendMessageAsync` leaks.** The timeout's `CancellationTokenSource` was never disposed, leaving a timer alive for the full timeout on every request, and the pending entry leaked if the send threw. | Both are cleaned up when the call ends, however it ends. |
| **`SetCanceled` could throw** when a timeout or a disconnect raced with a reply. | `TrySetCanceled` / `TrySetResult` throughout. |
| **`ConnectAsync` never started the heartbeat timer**; only `Connect` did. | Both start it. |
| **A timer per client connection** on the server. | One timer for every client, and each heartbeat is serialized once. |
| **The accept loop died on the first failed accept**, and `Stop()` left it with an unobserved exception. | It reports the failure on `OnServerError` and carries on; `Stop()` ends it cleanly. |
| **`OnClientConnection` was raised before the server subscribed to the connection**, and a failure in `HandleConnection` could leave a dead entry in `ClientConnections`. | A connection is always started and always removed when it ends, even if a subscriber throws. |
| **`ProtobuffClientConnection._disposedValue` was never set.** | Closing is idempotent by construction. |
| **`ListenerAddress` could not report a port chosen with port 0.** | It becomes the address actually bound once the server starts. |
| **`IsConnected` relied on `Socket.Connected`**, which only reflects the last operation. | It reflects whether the connection has been closed, by either side or by a failure. |
| **Log messages built with string interpolation** even when the level was off. | Structured logging templates. |

## Behaviour to be aware of

- **`SendMessage` returns once the message is queued**, not once it is written. `true` means queued. `false` means
  there is no connection, or the peer was so far behind that it is now being disconnected.
- **`Disconnected` is published shortly after `Disconnect()` returns**, not during the call. It is published when the
  connection has actually ended.
- **`ProtobuffClientConnection<T>`'s constructor** takes a `Socket`, the server config and a logger. The heartbeat
  parameters are gone because the server now sends heartbeats from one shared timer.
- **Errors on a client's connection are logged**, through the server's `ILogger`, instead of written to `Trace`.
- **The observables never complete**, including on dispose. The exception is
  `IProtobuffClientConnection.OnClientDisconnection`, which emits the connection once and then completes, so a late
  subscriber still receives it.
- **A subscriber that throws** does not take the framework down: the connection it was told about is still started
  and cleaned up. A message parser that throws closes that connection, as in the original.
- **A restart returns to the same port.** After `Stop()`, `Start()` binds the address bound last time, even when
  the config asked for port 0.
- **Timeouts and disconnects cancel a pending `SendMessageAsync`** (`TaskCanceledException`), as in the original.
  Calling it with no connection throws `IOException`, also as before.
- **A pending `SendMessageAsync` can be cancelled early** if the client disconnects and reconnects immediately,
  because the old connection's cleanup cancels whatever is pending at that moment. It looks the same as a timeout.
- **No counterpart in LabEnhanced** for `TaskCompletionWrapper`, `NetworkTools`, `LoggingConstants` and
  `ProtobuffHandshakeServer`. Nothing in the enhanced library needs them.
- **`IClientConfig` members `CompressData`, `IsEncrypted`, `Password` and `TurnOnEnhancedLogging`** are kept for
  interface compatibility and, as in the original, are not used.
- **`RuntimeTypeModel.Default.IncludeDateTimeKind = true`** is still set, once, the first time a message is encoded
  or decoded. Both ends need the same setting to agree on the bytes of a `DateTime`.

## How it works

All socket work is in two internal types shared by the server and the client, so both ends read and write the same
way.

**`Frame`** is the wire format.

- `Encode` serializes a message into a scratch buffer owned by the calling thread and returns the framed bytes. Room
  for the longest possible length prefix is left in front of the payload, and the prefix is then written
  right-aligned against it, so the frame is one contiguous block. Nothing is allocated per message.
- `TryDecode` reads one frame from the front of a span and deserializes the payload directly from it.

**`Wire`** is one socket.

- *Sending.* `Send` copies the encoded frame into a buffer under a lock and returns. If no flush is running, it
  queues one on the thread pool. The flush swaps the filled buffer for an empty one and writes it in a single socket
  call, repeating until nothing is queued. Messages that arrive while a write is in progress share the next write,
  which is where the throughput comes from; a lone message still goes out at once.
- *Receiving.* One asynchronous loop reads into a 64 KB buffer, decodes every complete message in it, and awaits the
  message parser for each one in order. A trailing partial message is moved to the front, and the buffer doubles
  when a single message does not fit (bounded by `MaxMessageBytes`).
- *Closing.* The first `Close` wins and records the reason. It disposes the socket, which ends the receive loop and
  any write in progress. The receive loop's end is the connection's end: that is when the owner removes it and
  publishes the disconnection.

A broadcast encodes the message once and copies the same bytes into each client's send buffer.

There is no dead-peer watchdog. As in the original, a dead peer is noticed when a write to it fails, which
heartbeats make happen.

## Benchmark

`LabEnhanced.Benchmarks` runs both frameworks through four scenarios over loopback with a message of about 40 bytes.
Each scenario is timed end to end, from the first send until the last message has been received or the last reply
awaited. It runs once to warm up and then five times; the median is reported.

The two runs below were taken on one 20-core Windows machine with .NET 10. Absolute numbers moved a lot between
runs, so both are shown.

| Scenario | Original (messages/s) | Enhanced (messages/s) | Speed-up |
|---|---:|---:|---:|
| Client → server, one-way (500,000 messages) | 103,000 / 104,000 | 2,540,000 / 2,930,000 | 25x / 28x |
| Server → 8 clients, broadcast (800,000 deliveries) | 102,000 / 59,000 | 6,920,000 / 7,610,000 | 68x / 129x |
| Request/reply, one at a time (30,000 requests) | 25,800 / 14,700 | 31,800 / 15,200 | 1.2x / 1.0x |
| Request/reply, 64 in flight (256,000 requests) | 63,500 / 29,300 | 675,000 / 460,000 | 11x / 16x |

What the numbers do and do not show:

- **One-at-a-time request/reply is not improved.** A single round trip is dominated by loopback and thread wake-ups,
  which both frameworks pay. The gains come from batching, which needs more than one message in flight.
- **Allocation per message is about the same**: roughly 105–120 bytes one-way and 840 (enhanced) against 880–930
  (original) for a request and its reply. Most of it is the deserialized message itself.
- **The original needed a lock to run the concurrent scenario.** Without one, concurrent callers corrupt its stream,
  so the benchmark serializes its sends. The enhanced framework needs no lock.
- **The benchmark lifts the enhanced send backlog limit.** Its sends never block, so a sender running flat out would
  otherwise queue more than 16 MB and have its connection closed. Elapsed time still runs until the receiver has
  everything.
- **Loopback only.** Over a real network the per-message system call the original pays matters less relative to
  latency, and the batching matters more relative to packet count. Neither was measured.

The message counts can be scaled: `dotnet run -c Release --project LabEnhanced/LabEnhanced.Benchmarks -- 0.2` runs a
fifth of the load.

## Tests

The 15 tests in `LabEnhanced.Neutron.Tests` cover:

- messages in both directions, in order
- 8 concurrent senders mixing one-way messages and requests, with no corruption and every reply matched
- 3 MB messages in both directions
- a request cancelled on timeout, and failing at once when not connected
- each end reporting a connection and its loss exactly once, and a pending request cancelled by the disconnect
- the server stopping and starting again on the same port, and the client reconnecting
- failover to the first end point that accepts, and a failed connect reported rather than thrown
- heartbeats in both directions
- an oversized message closing the connection
- a client that stops reading being dropped while a healthy one keeps receiving
- start failures thrown or reported, as configured
- interoperability with `LabOriginal` in both directions
