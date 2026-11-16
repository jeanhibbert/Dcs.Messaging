# LabEnhanced.Neutron, method by method

Every improvement made over `LabOriginal.Neutron`, listed against the class and member it belongs to. For the
overview (issues fixed, behaviour to be aware of, benchmark), see [ENHANCEMENTS.md](ENHANCEMENTS.md).

Each row is one improvement:

- **What** is the improvement: the problem in the original and what is better now.
- **How** is the code that achieves it.

A member with several improvements has several rows. Members that exist in only one version are marked *removed* or
*new*.

- [ProtobuffAsyncServer&lt;T&gt;](#protobuffasyncservert)
- [ProtobuffClientConnection&lt;T&gt;](#protobuffclientconnectiont)
- [ProtobufAsyncClient&lt;T&gt;](#protobufasyncclientt)
- [ProtobuffServerConfig&lt;T&gt; and ProtobuffClientConfig&lt;T&gt;](#protobuffserverconfigt-and-protobuffclientconfigt)
- [Frame (new)](#frame-new)
- [Wire (new)](#wire-new)
- [Interfaces](#interfaces)
- [Classes with no counterpart](#classes-with-no-counterpart)

## ProtobuffAsyncServer&lt;T&gt;

| Member | What | How |
|---|---|---|
| Constructor | Constructing a server has no side effects. The original created the `TcpListener` and set a global protobuf-net option every time. | It only assigns fields and exposes the observables. The socket is created in `Start`, and the global option is set once in `Frame`'s static constructor. |
| `OnClientConnection`, `OnClientDisconnection`, `OnServerError` | Notifications are Rx observables instead of events, so they can be filtered, combined and moved to another thread. | Each is a `Subject<T>` exposed through `AsObservable()`, so subscribers cannot publish into it. |
| | Notifications from different connections cannot overlap. | The two that several connections publish to at once, `_disconnections` and `_errors`, are wrapped in `Subject.Synchronize`. |
| `ListenerAddress` | A port chosen with port 0 can be discovered. The original always returned the configured address. | The property has a private setter, and `Start` assigns `(IPEndPoint)listener.LocalEndPoint`. |
| `IsBound` | Safe to read when the server is stopped. | `_listener?.IsBound ?? false`. |
| `Start()` | The server can be started again after `Stop`, on the same port. | A new `Socket` is created on each call and bound to `ListenerAddress`, which still holds the address bound last time. |
| | A failed start does not leak the socket. | The `catch` calls `Stop()` before it rethrows or reports, according to `HandleStartException`. |
| | The accept loop starts without a thread hop. | `_ = AcceptAsync(listener)` instead of `Task.Run`. |
| | One heartbeat timer serves every client, and each heartbeat is serialized once. The original had a timer and a serialization per client. | `Observable.Interval(...)` subscribes a call to the broadcast `SendMessage(heartBeat)`. |
| | A heartbeat that fails is reported instead of lost. | The call is in a `try`/`catch` that passes the exception to `Report`, which publishes it on `OnServerError`. |
| `AcceptConnections` → `AcceptAsync` | One failed accept no longer stops the server accepting. The original loop had no error handling and ended for good. | The `try`/`catch` is inside the loop. A failure goes to `Report`, then `Task.Delay(100)` so a persistent fault cannot spin, then the loop continues. |
| | `Stop()` ends the loop cleanly. The original left an exception nobody observed. | The loop is passed the listener it serves and compares it with `_listener`. `Stop` swaps `_listener` to null, so the comparison fails and the loop returns. |
| | A client accepted at the moment of a stop is not left connected. | The same comparison runs after `Accept`, and calls `connection.Disconnect()` if the server has stopped. |
| `HandleConnection` → `Accept` | A connection is always cleaned up, even if an `OnClientConnection` subscriber throws. | `_connections.OnNext(connection)` is in a `try`, and `connection.Start()` is in its `finally`. A started connection always removes itself when it ends. |
| | No socket is leaked if the connection cannot be created. | The constructor call is in a `try`/`catch` that disposes the socket and rethrows to the accept loop, which reports it. |
| `HandleOnClientDisconnection` | *Removed.* Subscribers are told about a disconnection once, not twice. | The server no longer subscribes to each connection's own notification. `RemoveClientConnection` is the only publisher. |
| `Stop()` | Safe to call twice, or before `Start`. | `Interlocked.Exchange(ref _listener, null)?.Dispose()` takes the listener atomically, so only one caller disposes it. |
| | Heartbeats stop with the server. | `_heartBeat?.Dispose()`. |
| `DisconnectAll` | *Removed.* One less place that edits the dictionary. | `Stop` calls `connection.Disconnect()` on each connection, and each removes itself when it has ended. |
| `Disconnect(connection)` | No spurious "was not present" warning. The original removed the entry itself, so the connection's own cleanup could not find it. | It is one expression, `TryGetValue(...) && known.Disconnect()`. Removal is left to `RemoveClientConnection`. |
| `SendMessage(id, message)` | The caller never waits on the client's socket. | The lookup is unchanged; `connection.SendMessage` now queues. |
| `SendMessage(message)` (broadcast) | The message is serialized once, not once per client. | `Frame.Encode(message)` runs once, and `client.Send(frame)` hands the same bytes to each `ProtobuffClientConnection<T>`. Any other implementation falls back to `connection.SendMessage(message)`. |
| | One slow client delays neither the caller nor the other clients. | `Send` only copies the bytes into that connection's buffer. No socket is touched. |
| | One failing client cannot stop the rest receiving the message. In the original its exception ended the loop. | `Send` returns false instead of throwing, and the results are combined with `&=`. |
| | The connection collection is not copied on every call. | `foreach (var (_, connection) in ClientConnections)` enumerates the dictionary directly. The original's `.Values` copies a `ConcurrentDictionary` under all its locks. |
| `RemoveClientConnection(id)` | A subscriber never sees a disconnected client still listed. | `ClientConnections.Remove(id, out var connection)` runs first, then `_disconnections.OnNext(connection)`. |
| `HandleServerException` → `Report` | An error is always both logged and published. | One method does both: `_log.LogError(...)` then `_errors.OnNext(exception)`. |
| `Dispose()` | Less code, and safe to repeat without a flag. | `Dispose() => Stop()`. |

## ProtobuffClientConnection&lt;T&gt;

| Member | What | How |
|---|---|---|
| Constructor | No timer per connection. | The heartbeat parameters are gone; the server heartbeats every client from one timer. |
| | The socket work is written once and shared with the client. | It creates `new Wire(socket, config.MaxMessageBytes, config.MaxSendBacklogBytes)` and delegates to it. |
| `OnClientDisconnection` | Published exactly once, and a subscriber that arrives late still receives it. | It is an `AsyncSubject<T>`: `OnNext(this)` then `OnCompleted()` at the end of `RunAsync`. An `AsyncSubject` replays its value to late subscribers. |
| `Start()` | No thread is held for the life of the connection. The original parked one in a blocking read. | `_ = RunAsync()`. The method returns at its first socket `await`. |
| `Read` → `RunAsync` | Messages are received without blocking and many at a time. | It awaits `_wire.RunAsync<T>(message => _messageParser.ReceivedMsgAsync(Id, message))`. |
| | The reason a connection failed reaches the application log. The original wrote to `Trace`. | `Wire.RunAsync` returns the exception, and it is passed to `_log.LogError`. |
| | Cleanup happens once and in a fixed order. | After the wire has ended: `_callBack.RemoveClientConnection(Id)`, then the disconnection is published. |
| `SendHeartBeat` | *Removed.* The heartbeat can no longer corrupt the stream by writing at the same time as another sender. | There is no timer callback; heartbeats arrive through `Send` like any other message. |
| `SendMessage(message)` | Safe to call from any number of threads, and never blocks. The original wrote to the `NetworkStream` on the caller's thread with no lock. | `_wire.Send(Frame.Encode(message))`: the message is encoded on the calling thread and the bytes are queued under the wire's lock. |
| `Send(frame)` | *New, internal.* A broadcast can hand over bytes that are already encoded. | `_wire.Send(frame)`. |
| `IsConnected` | Reflects a connection closed by either side or by a failure. `Socket.Connected` only reflects the last operation. | `!_wire.IsClosed`. |
| `Disconnect()` | Safe to call any number of times, from any thread. | `_wire.Close()`, where only the first call has any effect. |
| `Dispose()` | Actually ends the connection. The original set a flag that its blocked read loop could not see, and never closed the socket. | `_wire.Close()` disposes the socket, which ends the receive loop at once. |

## ProtobufAsyncClient&lt;T&gt;

| Member | What | How |
|---|---|---|
| Constructor | No side effects, and no timer that outlives a connection. | It only assigns fields and exposes the observables. The heartbeat is created per connection in `RunAsync`. |
| `Connected`, `Disconnected` | Rx observables instead of events. | Each is a `Subject<Unit>` exposed through `AsObservable()`. |
| `ConnectedEndpoint` | Cannot throw, and is null when not connected. The original asked the socket inside a catch-all. | `_wire is { IsClosed: false } wire ? wire.RemoteEndPoint : null`, where the address was captured when the connection was made. |
| `IsConnected` | Reflects a connection closed by either side or by a failure. | `_wire is { IsClosed: false }`. |
| `Connect()` | One connect implementation instead of two that had drifted apart. | `ConnectAsync().GetAwaiter().GetResult()`. Every `await` inside uses `ConfigureAwait(false)`, so this cannot deadlock on a synchronization context. |
| `ConnectAsync()` | The heartbeat starts however the connection was made. The original's `ConnectAsync` forgot to. | The heartbeat is started in `RunAsync`, which every connection goes through. |
| | A failed attempt leaves nothing behind. | The `catch` disposes the socket, sets `ConnectionException`, logs, and `continue`s to the next end point. |
| | A connection is always cleaned up, even if a `Connected` subscriber throws. | `_connected.OnNext(Unit.Default)` is in a `try`, and `_ = RunAsync(wire)` is in its `finally`. |
| `Read` → `RunAsync` | No thread is blocked reading. | It awaits `wire.RunAsync<T>(Route)`. |
| | `Disconnected` is published once per connection. The original raised it from `Disconnect()` and again from the read loop. | The end of `RunAsync` is the only place `_disconnected.OnNext` is called. |
| | Every pending request is cancelled when the connection ends. | `foreach (var (_, reply) in _pending) reply.TrySetCanceled();` |
| | The heartbeat lives exactly as long as the connection. | `using (StartHeartBeat())` wraps the wait for the wire to end. |
| `Route` | *New.* Replies and ordinary messages are separated without an extra state machine per message. | A message without `AsyncSend` returns the parser's task directly. A reply looks up `_pending` by `AsyncId`, calls `TrySetResult`, and returns `Task.CompletedTask`. |
| `SendHeartBeat` → `StartHeartBeat` | The heartbeat cannot corrupt the stream, and cannot throw on a timer thread. | `Observable.Interval(...)` subscribes a call to `SendMessage(heartBeat)`, which queues like any other send and catches its own exceptions. |
| `Disconnect()` | One teardown path. The original stopped the timer, closed the socket and raised the event itself, alongside the read loop doing the same. | `_wire?.Close()`. The heartbeat, the pending requests and `Disconnected` are all handled where the connection ends, in `RunAsync`. |
| `SendMessage(message)` | Safe to call from any number of threads, and never blocks. | `_wire?.Send(Frame.Encode(message)) == true`. |
| | Same contract as before: false, not an exception, when the message could not be sent. | The call is in a `try`/`catch` that logs and returns false. |
| `NextAsyncSendId` | *Removed.* | `Interlocked.Increment(ref _asyncSendId)` is inlined where it is used. |
| `SendMessageAsync(message)` | One state machine per call instead of two. | It is not `async`: it returns the other overload's task directly. |
| `SendMessageAsync(message, continueOnCapturedContext)` | No timer left running after the call. The original never disposed its `CancellationTokenSource`, so each request kept a timer alive for the full timeout. | `using var timeout = new CancellationTokenSource(Config.AsyncTimeout);` |
| | The pending entry cannot leak. In the original it stayed if the send threw. | The entry is added before the `try`, and `_pending.TryRemove` is in the `finally`. |
| | A timeout that fires just as the reply arrives cannot throw. | The callback calls `TrySetCanceled` instead of `SetCanceled`. |
| | No wrapper object or closure per call. | `timeout.Token.UnsafeRegister(static (state, token) => ..., reply)` passes the `TaskCompletionSource` as state to a static lambda. |
| | Concurrent callers are safe. | The send goes through `_wire.Send`, which queues under a lock. |
| `HandleCancelled` | *Removed.* | Replaced by the static callback above. |
| `ClearAsyncTasks` | *Removed.* All pending requests are cancelled. In the original, one request that had already completed made `SetCanceled` throw, which stopped the loop and left the rest waiting for their timeouts. | The loop in `RunAsync` uses `TrySetCanceled`, which never throws. |
| `Dispose()` | Less code, and safe to repeat without a flag. | `Dispose() => Disconnect()`. |

## ProtobuffServerConfig&lt;T&gt; and ProtobuffClientConfig&lt;T&gt;

| Member | What | How |
|---|---|---|
| `MaxMessageBytes` | *New.* Corrupt or hostile input cannot force a huge allocation. | `Frame.TryDecode` compares each length prefix with it and throws `InvalidDataException` if it is larger, which closes the connection. Default 16 MB. |
| `MaxSendBacklogBytes` | *New.* A peer that stops reading can neither stall the sender nor use unbounded memory. | `Wire.Send` compares the bytes already queued with it and closes the connection if there are more. Default 16 MB. |
| `HeartBeatMsg` | "No heartbeat" is expressible in the type. | The property and constructor parameter are `T?`. |

## Frame (new)

The wire format, in one internal static class. It replaces the direct calls to protobuf-net's length-prefix helpers,
and produces and accepts exactly the same bytes.

| Member | What | How |
|---|---|---|
| Static constructor | The global `IncludeDateTimeKind` option is set once, not on every server and client construction. | `static Frame() => RuntimeTypeModel.Default.IncludeDateTimeKind = true;` runs before the first encode or decode. |
| `Encode(message)` | Serializing no longer touches the socket. `SerializeWithLengthPrefix` wrote to the stream as it went. | It serializes into memory and returns the bytes; `Wire` decides when they are written. |
| | Nothing is allocated per message. | The buffer is a `[ThreadStatic] ArrayBufferWriter<byte>`, reused through `ResetWrittenCount()`. |
| | One very large message does not hold memory for ever. | A scratch buffer that has grown past 1 MB is replaced on the next call. |
| | The length prefix and payload are one contiguous block, without measuring the message first. | `Advance(5)` reserves room for the longest prefix, the payload is serialized after it, and the varint is then written right-aligned against the payload. The returned span starts at the varint's first byte. |
| `TryDecode(data, maxMessageBytes, out message)` | Reading a message makes no system calls. `DeserializeWithLengthPrefix` blocked on the socket and read the prefix a byte at a time. | It parses the varint from a span of bytes already received and calls `Serializer.Deserialize<T>` on a slice of the same span. |
| | Bad input is rejected early. | It throws `InvalidDataException` if the prefix runs past five bytes or the length exceeds `maxMessageBytes`. |
| | A message split across socket reads is handled. | It returns 0 when the frame is incomplete, and the caller keeps the bytes for the next read. |

## Wire (new)

One socket, in one internal class shared by the server's connections and the client. It replaces each class's own
use of `NetworkStream`, and `NetworkTools.CloseConnectionFully`.

| Member | What | How |
|---|---|---|
| Constructor | The remote address is always available, even after the socket has closed. | `RemoteEndPoint` is captured from the socket while it is known to be connected. |
| `Send(frame)` | Concurrent senders cannot interleave bytes. | The frame is appended to `_queued` inside `lock (_gate)`. |
| | The caller never waits on the socket. | Writing is left to a flush on the thread pool. `Send` only copies and returns. |
| | Scheduling the flush allocates nothing. | `Wire` implements `IThreadPoolWorkItem` and queues itself with `ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false)`. |
| | Only one flush runs at a time. | The `_flushing` flag is set under the lock when a flush is queued, and later senders return without queuing another. |
| | A slow peer is disconnected instead of blocking the sender. | If `_queued.WrittenCount` exceeds `_maxSendBacklogBytes`, it calls `Close` with an `IOException` and returns false. |
| `IThreadPoolWorkItem.Execute` (the flush) | Many messages share one system call. The original made one blocking write per message. | Under the lock it swaps the two buffers, `(_queued, _sending) = (_sending, _queued)`, then writes the whole of `_sending` with `SendAsync` outside the lock. Senders keep filling the other buffer meanwhile. |
| | A lone message still goes out at once. | The flush is queued by the first `Send` and does not wait for more. |
| | No message is stranded in the queue. | The loop repeats until `_queued` is empty, and clears `_flushing` under the same lock that senders use to check it. |
| | A burst does not hold memory for ever. | When the flush goes idle, a send buffer that has grown past 1 MB is replaced. |
| | A failed write ends the connection with its reason. | The `catch` calls `Close(exception)`. |
| `RunAsync(onMessage)` | No thread is blocked waiting for data. | `await _socket.ReceiveAsync(buffer.AsMemory(end), SocketFlags.None)`. |
| | Every message a socket read delivers is processed, not one per read. | An inner loop calls `Frame.TryDecode` until it returns 0, awaiting `onMessage` for each message in order. |
| | A message split across reads, or larger than the buffer, is handled. | The unread tail is moved to the front with `CopyTo`, and the buffer is doubled with `Array.Resize` when a single message fills it. `MaxMessageBytes` bounds the growth. |
| | The caller learns why the connection ended. | Exceptions from the socket, the decoder or `onMessage` go to `Close(exception)`, and the method returns `_closeReason` (null for a clean close). |
| `Close(reason)` | Closing is safe from any thread, any number of times. The original closed the socket from several places with no coordination. | Under the lock, the first call sets `_closed` and `_closeReason`; later calls return. |
| | Closing ends everything in progress. | `_socket.Dispose()` makes the pending receive and any write fail, which ends both loops. |
| `IsClosed` | State that cannot throw and is correct on any thread. | A `volatile bool`. |

## Interfaces

The interfaces have the same names and members as the originals. The only change is that the events on
`IProtobufClient<T>`, `IProtobuffAsyncServer<T>` and `IProtobuffClientConnection<T>` are observables. The full list
and the event-to-observable mapping are in [ENHANCEMENTS.md](ENHANCEMENTS.md#what-stayed-the-same).

## Classes with no counterpart

| LabOriginal class | Why LabEnhanced does not have it |
|---|---|
| `TaskCompletionWrapper<T>` | It carried a request id and its `TaskCompletionSource` into the timeout callback. The pending entry is now removed in a `finally`, so the callback only needs the `TaskCompletionSource`. |
| `NetworkTools` | Its one method, `CloseConnectionFully`, is replaced by `Wire.Close`. |
| `LoggingConstants` | It supplied a `Trace` category. All logging now goes through `ILogger`. |
| `ProtobuffHandshakeServer<T>` | A second copy of the server that logged through `Trace`. Only part of its source was available, and nothing it did differs from `ProtobuffAsyncServer<T>`. |
