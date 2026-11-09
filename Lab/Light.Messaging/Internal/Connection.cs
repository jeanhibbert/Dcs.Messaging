using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging.Internal;

/// <summary>
/// Receives frames from the connection's receive loop. Calls for one connection are never concurrent.
/// </summary>
internal interface IConnectionOwner
{
    /// <summary>
    /// The peer's Hello arrived; called before any other frame from it is processed.
    /// </summary>
    void OnConnected(Connection connection);

    void OnFrame(Connection connection, FrameKind kind, int endpointId, long correlationId, ReadOnlySpan<byte> payload);

    /// <summary>
    /// Called once, after both loops have stopped, if <see cref="OnConnected"/> was called.
    /// </summary>
    void OnClosed(Connection connection);
}

/// <summary>
/// Transport-independent half of a connection: an outbound queue drained by a batching send loop, and frame parsing
/// and dispatch for the receive loop. Subclasses only move bytes (<see cref="SendAsync"/>, <see cref="ReceiveLoopAsync"/>).
/// <para>
/// Outbound: anyone may <see cref="Enqueue"/> (never blocks). One send loop copies every queued frame that fits into a
/// single buffer and writes it in one call, so heavy load costs one syscall per batch rather than per message, and
/// light load adds no delay. Inbound: frames are decoded in place from the receive buffer and handed to the owner on
/// the receive loop, so messages from one peer are processed in order, one at a time.
/// </para>
/// </summary>
internal abstract class Connection
{
    private static readonly TimeSpan GracefulCloseTimeout = TimeSpan.FromSeconds(5);

    private readonly FrameQueue _outbound;
    private readonly CancellationTokenSource _closed = new();
    private readonly TaskCompletionSource _handshake = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IConnectionOwner _owner;
    private string _closeReason;
    private string _shutdownReason;
    private long _sendStartedAt;
    private long _droppedFrames;
    private long _reportedDroppedFrames;
    private int _lastDroppedEndpointId;
    private bool _helloReceived;
    private bool _peerSaidGoodbye;
    private volatile bool _closingGracefully;

    protected Connection(ConnectionSettings settings, string remoteAddress)
    {
        Settings = settings;
        RemoteAddress = remoteAddress;
        Peer = new Peer(this);
        _outbound = new FrameQueue(settings.QueueCapacity, settings.SlowConsumerPolicy);
    }

    public ConnectionSettings Settings { get; }

    public Peer Peer { get; }

    public long Id { get; set; }

    public string RemoteAddress { get; }

    public string RemoteName { get; private set; }

    public string CloseReason => Volatile.Read(ref _closeReason);

    public bool IsClosed => Volatile.Read(ref _closeReason) != null;

    public int QueuedFrames => _outbound.Count;

    public long DroppedFrames => Interlocked.Read(ref _droppedFrames);

    /// <summary>
    /// Endpoint of the most recently dropped message, for the slow-consumer warning.
    /// </summary>
    public int LastDroppedEndpointId => Volatile.Read(ref _lastDroppedEndpointId);

    /// <summary>
    /// Frames dropped since the previous call; safe to call from the watchdog and the close path at once.
    /// </summary>
    public long TakeUnreportedDrops()
    {
        var dropped = DroppedFrames;
        return dropped - Interlocked.Exchange(ref _reportedDroppedFrames, dropped);
    }

    /// <summary>
    /// Completes when the peer's Hello has been accepted; faults if the connection closes first.
    /// </summary>
    public Task Handshake => _handshake.Task;

    /// <summary>
    /// Endpoints this peer has subscribed to on the server. Used only from the receive loop and, after it has
    /// stopped, from <see cref="IConnectionOwner.OnClosed"/>, so it needs no lock.
    /// </summary>
    public HashSet<int> RemoteSubscriptions { get; } = new();

    /// <summary>
    /// Queues a frame for sending and takes ownership of one of its references. Never blocks. Returns false if this
    /// frame will not be sent.
    /// </summary>
    public bool Enqueue(Frame frame)
    {
        switch (_outbound.Enqueue(frame, out var dropped))
        {
            case EnqueueResult.Queued:
                if (dropped != null)
                {
                    RecordDrop(dropped);
                }

                return true;

            case EnqueueResult.DroppedNewest:
                RecordDrop(frame);
                return false;

            case EnqueueResult.Full:
                frame.Release();
                Close("send queue full: peer is not keeping up");
                return false;

            default:
                frame.Release();
                return false;
        }
    }

    public void EnqueueControl(FrameKind kind, int endpointId)
    {
        var frame = Frame.Rent(kind, endpointId);
        frame.Seal(Settings.MaxFrameBytes);
        Enqueue(frame);
    }

    /// <summary>
    /// Graceful close: sends what is queued followed by Goodbye, accepts nothing more, and closes once the peer has
    /// replied in kind (see <see cref="Wire"/>).
    /// </summary>
    public void Shutdown(string reason)
    {
        if (Interlocked.CompareExchange(ref _shutdownReason, reason, null) != null)
        {
            return;
        }

        var goodbye = Frame.Rent(FrameKind.Goodbye, 0);
        goodbye.WriteUtf8(reason);
        goodbye.Seal(Settings.MaxFrameBytes);
        Enqueue(goodbye);
        _outbound.Complete(discard: false);
    }

    /// <summary>
    /// Immediate close: queued frames are discarded. The first reason recorded wins.
    /// </summary>
    public void Close(string reason)
    {
        if (Interlocked.CompareExchange(ref _closeReason, reason, null) != null)
        {
            return;
        }

        _outbound.Complete(discard: true);

        // Transport teardown runs on the thread pool, not on the caller, which may be a publisher.
        _ = _closed.CancelAsync();
    }

    public void CloseIfStalled(long now, long stallTicks)
    {
        var startedAt = Volatile.Read(ref _sendStartedAt);
        if (startedAt != 0 && now - startedAt > stallTicks)
        {
            Close("send stalled: peer stopped reading");
        }
    }

    /// <summary>
    /// Runs the connection until it closes. Never throws.
    /// </summary>
    public async Task RunAsync(IConnectionOwner owner)
    {
        _owner = owner;
        Enqueue(CreateHello());

        var token = _closed.Token;
        using var abortOnClose = token.Register(static state => ((Connection)state).AbortQuietly(), this);

        var sending = SendLoopSafeAsync(token);
        var receiving = ReceiveLoopSafeAsync(token);

        // A loop that fails closes the connection. Otherwise a loop only finishes during a graceful close: the send
        // loop once our Goodbye is out, the receive loop once the peer has said Goodbye and ended its stream.
        await Task.WhenAny(sending, receiving).ConfigureAwait(false);
        if (!IsClosed)
        {
            if (!sending.IsCompleted)
            {
                // The peer closed first; its Goodbye started our Shutdown, so finish sending.
                await Task.WhenAny(sending, Task.Delay(GracefulCloseTimeout)).ConfigureAwait(false);
            }

            if (sending.IsCompleted && !IsClosed)
            {
                _closingGracefully = true;
                try
                {
                    await CompleteSendAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                }

                await Task.WhenAny(receiving, Task.Delay(GracefulCloseTimeout)).ConfigureAwait(false);
            }

            Close(_shutdownReason ?? "closed");
        }

        await Task.WhenAll(sending, receiving).ConfigureAwait(false);

        while (_outbound.TryDequeue(out var frame))
        {
            frame.Release();
        }

        _handshake.TrySetException(new MessagingException($"Connection closed during handshake: {CloseReason}"));
        if (_helloReceived)
        {
            owner.OnClosed(this);
        }
    }

    /// <summary>
    /// Sends one or more whole frames.
    /// </summary>
    protected abstract ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken);

    /// <summary>
    /// Reads until the peer ends the stream, passing received bytes to <see cref="ProcessFrames"/>.
    /// </summary>
    protected abstract Task ReceiveLoopAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Signals the end of the outbound stream after a graceful shutdown.
    /// </summary>
    protected virtual ValueTask CompleteSendAsync() => ValueTask.CompletedTask;

    /// <summary>
    /// True once a graceful shutdown has sent everything; <see cref="AbortTransport"/> should then close cleanly
    /// rather than reset.
    /// </summary>
    protected bool IsClosingGracefully => _closingGracefully;

    /// <summary>
    /// Tears down the underlying transport so that pending sends and receives fail.
    /// </summary>
    protected abstract void AbortTransport();

    /// <summary>
    /// Dispatches every complete frame in <paramref name="data"/> and returns the bytes consumed; a trailing partial
    /// frame is left for the caller to complete with more data.
    /// </summary>
    protected int ProcessFrames(ReadOnlySpan<byte> data)
    {
        var consumed = 0;
        while (data.Length - consumed >= Wire.LengthSize)
        {
            var bodyLength = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(consumed));
            if (bodyLength < Wire.MinBodySize || bodyLength > Settings.MaxFrameBytes - Wire.LengthSize)
            {
                throw new InvalidDataException($"Invalid frame length {bodyLength}.");
            }

            if (data.Length - consumed - Wire.LengthSize < bodyLength)
            {
                break;
            }

            DispatchFrame(data.Slice(consumed + Wire.LengthSize, bodyLength));
            consumed += Wire.LengthSize + bodyLength;
        }

        return consumed;
    }

    /// <summary>
    /// Total size of the frame starting at <paramref name="data"/>, or 0 if its length prefix has not fully arrived.
    /// Only valid after <see cref="ProcessFrames"/> has checked that prefix.
    /// </summary>
    protected static int PeekFrameSize(ReadOnlySpan<byte> data)
    {
        return data.Length < Wire.LengthSize ? 0 : Wire.LengthSize + BinaryPrimitives.ReadInt32LittleEndian(data);
    }

    private void DispatchFrame(ReadOnlySpan<byte> body)
    {
        var kind = (FrameKind)body[0];
        var endpointId = BinaryPrimitives.ReadInt32LittleEndian(body.Slice(1));
        var headerSize = Wire.BodyHeaderSize(kind);
        if (body.Length < headerSize)
        {
            throw new InvalidDataException($"Truncated {kind} frame.");
        }

        var correlationId = Wire.HasCorrelation(kind) ? BinaryPrimitives.ReadInt64LittleEndian(body.Slice(Wire.MinBodySize)) : 0;
        var payload = body.Slice(headerSize);

        if (kind == FrameKind.Hello)
        {
            ReceiveHello(payload);
            return;
        }

        if (!_helloReceived)
        {
            throw new InvalidDataException($"Expected Hello, received {kind}.");
        }

        if (kind == FrameKind.Goodbye)
        {
            _peerSaidGoodbye = true;
            Shutdown(Encoding.UTF8.GetString(payload));
            return;
        }

        _owner.OnFrame(this, kind, endpointId, correlationId, payload);
    }

    private void ReceiveHello(ReadOnlySpan<byte> payload)
    {
        if (_helloReceived || payload.Length < sizeof(ushort))
        {
            throw new InvalidDataException("Unexpected or malformed Hello.");
        }

        var version = BinaryPrimitives.ReadUInt16LittleEndian(payload);
        if (version != Wire.ProtocolVersion)
        {
            throw new InvalidDataException($"Peer speaks protocol version {version}; this side speaks {Wire.ProtocolVersion}.");
        }

        RemoteName = Encoding.UTF8.GetString(payload.Slice(sizeof(ushort)));
        _helloReceived = true;
        _owner.OnConnected(this);
        _handshake.TrySetResult();
    }

    private Frame CreateHello()
    {
        var frame = Frame.Rent(FrameKind.Hello, 0);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.GetSpan(sizeof(ushort)), Wire.ProtocolVersion);
        frame.Advance(sizeof(ushort));
        frame.WriteUtf8(Settings.LocalName);
        frame.Seal(Settings.MaxFrameBytes);
        return frame;
    }

    private async Task SendLoopAsync(CancellationToken cancellationToken)
    {
        // On the pinned object heap, so the socket layer does not pin and unpin it on every write.
        var batch = GC.AllocateUninitializedArray<byte>(Settings.SendBatchBytes, pinned: true);
        Frame carried = null;

        try
        {
            // Close completes the queue, which ends the wait; the token covers sends already in progress.
            while (carried != null || await _outbound.WaitToReadAsync().ConfigureAwait(false))
            {
                // Take everything already queued, without waiting for more: batching never adds latency.
                var length = 0;
                while (true)
                {
                    var frame = carried;
                    carried = null;
                    if (frame == null && !_outbound.TryDequeue(out frame))
                    {
                        break;
                    }

                    if (frame.Length <= batch.Length - length)
                    {
                        frame.Span.CopyTo(batch.AsSpan(length));
                        length += frame.Length;
                        frame.Release();
                        continue;
                    }

                    if (length > 0)
                    {
                        // Send what is batched; this frame starts the next batch.
                        carried = frame;
                        break;
                    }

                    // Larger than the whole batch buffer: send it from its own buffer.
                    try
                    {
                        await TimedSendAsync(frame.Memory, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        frame.Release();
                    }
                }

                if (length > 0)
                {
                    await TimedSendAsync(batch.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            carried?.Release();
        }
    }

    private async ValueTask TimedSendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        Volatile.Write(ref _sendStartedAt, Stopwatch.GetTimestamp());
        await SendAsync(data, cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _sendStartedAt, 0);
    }

    private async Task SendLoopSafeAsync(CancellationToken cancellationToken)
    {
        try
        {
            await SendLoopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Close(exception.Message);
        }
    }

    private async Task ReceiveLoopSafeAsync(CancellationToken cancellationToken)
    {
        try
        {
            await ReceiveLoopAsync(cancellationToken).ConfigureAwait(false);

            // After a Goodbye the end of the stream is expected; RunAsync completes the close. Without one, the peer
            // went away abruptly.
            if (!_peerSaidGoodbye)
            {
                Close("peer closed the connection");
            }
        }
        catch (Exception exception)
        {
            Close(exception.Message);
        }
    }

    /// <summary>
    /// Only counts: the session's watchdog turns the count into at most one warning per peer per check, so a flood of
    /// drops never becomes a flood of log lines.
    /// </summary>
    private void RecordDrop(Frame frame)
    {
        Volatile.Write(ref _lastDroppedEndpointId, frame.EndpointId);
        Interlocked.Increment(ref _droppedFrames);
        frame.Release();
    }

    private void AbortQuietly()
    {
        try
        {
            AbortTransport();
        }
        catch (Exception)
        {
        }
    }
}
