using ProtoBuf;
using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;

namespace LabLegacy.Neutron;

/// <summary>
/// One socket carrying length-prefixed protobuf messages in both directions. Shared by the server's per-client
/// connection and by <see cref="NeutronClient{T}"/>, so both ends read and write the same way:
/// <list type="bullet">
/// <item>Reading is asynchronous through a <see cref="PipeReader"/>: one socket read usually delivers many messages,
/// which are deserialized straight from the receive buffer. No thread is blocked waiting for data.</item>
/// <item>Sending only queues. A single write loop owns the socket, so concurrent senders cannot interleave bytes. It
/// writes everything queued in one call.</item>
/// <item>The queue is bounded; <see cref="TrySend"/> fails when it is full and the owner decides what to do.</item>
/// </list>
/// </summary>
internal sealed class WireConnection
{
    private const int SendBatchBytes = 64 * 1024;
    private const int MaxPrefixLength = 5;

    private readonly Socket _socket;
    private readonly int _maxMessageBytes;
    private readonly Channel<EncodedMessage> _outbound;
    private readonly CancellationTokenSource _closed = new();
    private Exception? _closeReason;
    private long _receiveCount;
    private int _isClosed;

    public WireConnection(Socket socket, int sendQueueCapacity, int maxMessageBytes)
    {
        _socket = socket;
        _maxMessageBytes = maxMessageBytes;

        // Messages leave as soon as the write loop sends them; batching is its job, not Nagle's.
        _socket.NoDelay = true;

        _outbound = Channel.CreateBounded<EncodedMessage>(new BoundedChannelOptions(sendQueueCapacity)
        {
            SingleReader = true,

            // The write loop must never run on a sender's thread.
            AllowSynchronousContinuations = false,
        });
    }

    public bool IsClosed => Volatile.Read(ref _isClosed) == 1;

    public bool IsConnected => !IsClosed && _socket.Connected;

    /// <summary>
    /// Number of socket reads that returned data. A value that stops changing means the peer has gone quiet.
    /// </summary>
    public long ReceiveCount => Interlocked.Read(ref _receiveCount);

    /// <summary>
    /// Queues an already encoded message and takes ownership of one of its references. Never blocks. False if the
    /// queue is full or the connection is closed.
    /// </summary>
    public bool TrySend(EncodedMessage message)
    {
        if (_outbound.Writer.TryWrite(message))
        {
            return true;
        }

        message.Release();
        return false;
    }

    /// <summary>
    /// Reads and writes until the connection ends, then disposes the socket. <paramref name="onMessage"/> is awaited
    /// for each received message, in order. Returns why the connection ended: a read, write or handler failure, the
    /// reason given to <see cref="Close"/>, or null when either side simply closed it.
    /// </summary>
    public async Task<Exception?> RunAsync<T>(Func<T, ValueTask> onMessage)
    {
        var reading = GuardAsync(ReadLoopAsync(onMessage, _closed.Token));
        var writing = GuardAsync(WriteLoopAsync(_closed.Token));

        await Task.WhenAny(reading, writing).ConfigureAwait(false);
        Close();
        var error = await reading.ConfigureAwait(false) ?? await writing.ConfigureAwait(false);

        while (_outbound.Reader.TryRead(out var unsent))
        {
            unsent.Release();
        }

        _socket.Dispose();
        return error ?? _closeReason;
    }

    /// <summary>
    /// Closes abruptly: queued messages are discarded. Only the first call has any effect.
    /// </summary>
    public void Close(Exception? reason = null)
    {
        if (Interlocked.Exchange(ref _isClosed, 1) == 1)
        {
            return;
        }

        _closeReason = reason;
        _outbound.Writer.TryComplete();
        _closed.Cancel();
        try
        {
            // Unblocks a send that is stuck on a peer which stopped reading.
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception)
        {
        }
    }

    private async Task ReadLoopAsync<T>(Func<T, ValueTask> onMessage, CancellationToken cancellationToken)
    {
        var reader = PipeReader.Create(new NetworkStream(_socket, ownsSocket: false), new StreamPipeReaderOptions(bufferSize: 64 * 1024));
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = result.Buffer;
                Interlocked.Increment(ref _receiveCount);

                while (TryReadMessage(ref buffer, out var payload))
                {
                    await onMessage(Serializer.Deserialize<T>(payload)).ConfigureAwait(false);
                }

                // Everything before buffer.Start is consumed; a trailing partial message is kept for the next read.
                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted)
                {
                    return;
                }
            }
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads one Base128 length prefix and its payload from the front of <paramref name="buffer"/>, if both have fully
    /// arrived, and moves <paramref name="buffer"/> past them.
    /// </summary>
    private bool TryReadMessage(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload)
    {
        payload = default;
        var reader = new SequenceReader<byte>(buffer);

        uint length = 0;
        for (var shift = 0; ; shift += 7)
        {
            if (!reader.TryRead(out var next))
            {
                return false;
            }

            length |= (uint)(next & 0x7F) << shift;
            if ((next & 0x80) == 0)
            {
                break;
            }

            if (reader.Consumed == MaxPrefixLength)
            {
                throw new InvalidDataException("Malformed length prefix.");
            }
        }

        if (length > (uint)_maxMessageBytes)
        {
            throw new InvalidDataException($"A {length} byte message exceeds MaxMessageBytes ({_maxMessageBytes}).");
        }

        if (reader.Remaining < length)
        {
            return false;
        }

        payload = buffer.Slice(reader.Position, length);
        buffer = buffer.Slice(payload.End);
        return true;
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        // On the pinned object heap, so the socket layer does not pin and unpin it on every write.
        var batch = GC.AllocateUninitializedArray<byte>(SendBatchBytes, pinned: true);
        var reader = _outbound.Reader;

        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            // Send everything already queued in one write, without waiting for more: under load many messages share
            // a system call, and a lone message still goes out at once.
            var length = 0;
            while (reader.TryPeek(out var message))
            {
                var frame = message.Frame;
                if (frame.Length > batch.Length - length)
                {
                    if (length > 0)
                    {
                        break;
                    }

                    // Larger than the whole batch buffer: send it from its own buffer.
                    reader.TryRead(out _);
                    try
                    {
                        await SendAllAsync(frame, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        message.Release();
                    }

                    continue;
                }

                reader.TryRead(out _);
                frame.Span.CopyTo(batch.AsSpan(length));
                length += frame.Length;
                message.Release();
            }

            if (length > 0)
            {
                await SendAllAsync(batch.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask SendAllAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        while (!data.IsEmpty)
        {
            var sent = await _socket.SendAsync(data, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            data = data.Slice(sent);
        }
    }

    private async Task<Exception?> GuardAsync(Task loop)
    {
        try
        {
            await loop.ConfigureAwait(false);
            return null;
        }
        catch (Exception) when (IsClosed)
        {
            // Closing cancels the loops and shuts the socket; the errors that causes are expected.
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }
}
