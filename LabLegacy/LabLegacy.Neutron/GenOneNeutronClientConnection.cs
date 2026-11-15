using Microsoft.Extensions.Logging;
using ProtoBuf;
using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;

namespace LabLegacy.Neutron;

/// <summary>
/// One connected client. Compared with the legacy class:
/// <list type="bullet">
/// <item>Reading is asynchronous through a <see cref="PipeReader"/>: one socket read usually delivers many messages,
/// which are deserialized straight from the receive buffer. No thread is blocked waiting for data.</item>
/// <item>Sending only queues. A single write loop owns the socket, so concurrent senders (and the heartbeat) can no
/// longer interleave bytes and corrupt the stream. It writes everything queued in one call.</item>
/// <item>The queue is bounded. A client that cannot keep up is disconnected instead of blocking the sender.</item>
/// </list>
/// </summary>
public sealed class GenOneNeutronClientConnection<T> : INeutronClientConnection<T>
    where T : IBaseMessage
{
    private const int SendBatchBytes = 64 * 1024;
    private const int MaxPrefixLength = 5;

    private readonly INeutronAsyncServerCallBack<T> _callBack;
    private readonly Socket _socket;
    private readonly IServerProtoMessageParser<T> _messageParser;
    private readonly ILogger _log;
    private readonly int _maxMessageBytes;
    private readonly Channel<EncodedMessage> _outbound;
    private readonly CancellationTokenSource _closed = new();
    private int _isClosed;

    public GenOneNeutronClientConnection(
        INeutronAsyncServerCallBack<T> callBack,
        string id,
        Socket socket,
        IServerProtoMessageParser<T> messageParser,
        NeutronServerConfig<T> config,
        ILogger log)
    {
        _callBack = callBack;
        Id = id;
        _socket = socket;
        _messageParser = messageParser;
        _log = log;
        _maxMessageBytes = config.MaxMessageBytes;

        // Messages leave as soon as the write loop sends them; batching is its job, not Nagle's.
        _socket.NoDelay = true;

        _outbound = Channel.CreateBounded<EncodedMessage>(new BoundedChannelOptions(config.SendQueueCapacity)
        {
            SingleReader = true,

            // The write loop must never run on a sender's thread.
            AllowSynchronousContinuations = false,
        });
    }

    public event EventHandler<INeutronClientConnection<T>>? OnClientDisconnection;

    public string Id { get; }

    public bool IsConnected => Volatile.Read(ref _isClosed) == 0 && _socket.Connected;

    public void Start()
    {
        _ = RunAsync();
    }

    public bool SendMessage(T message)
    {
        return IsConnected && Send(EncodedMessage.Create(message));
    }

    public bool Disconnect()
    {
        Close();
        return true;
    }

    public void Dispose()
    {
        Close();
    }

    /// <summary>
    /// Queues an already encoded message and takes ownership of one of its references. Never blocks.
    /// </summary>
    internal bool Send(EncodedMessage message)
    {
        if (_outbound.Writer.TryWrite(message))
        {
            return true;
        }

        message.Release();
        if (Volatile.Read(ref _isClosed) == 0)
        {
            _log.LogWarning("connection_id:{Id} send queue is full; disconnecting the slow client", Id);
            Close();
        }

        return false;
    }

    private async Task RunAsync()
    {
        var reading = GuardAsync(ReadLoopAsync, "reading from");
        var writing = GuardAsync(WriteLoopAsync, "writing to");

        await Task.WhenAny(reading, writing).ConfigureAwait(false);
        Close();
        await Task.WhenAll(reading, writing).ConfigureAwait(false);

        while (_outbound.Reader.TryRead(out var unsent))
        {
            unsent.Release();
        }

        _socket.Dispose();
        _callBack.RemoveClientConnection(Id);
        _log.LogInformation("connection_id:{Id} has stopped", Id);
        OnClientDisconnection?.Invoke(this, this);
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        var reader = PipeReader.Create(new NetworkStream(_socket, ownsSocket: false), new StreamPipeReaderOptions(bufferSize: 64 * 1024));
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = result.Buffer;

                while (TryReadMessage(ref buffer, out var payload))
                {
                    var message = Serializer.Deserialize<T>(payload);
                    await _messageParser.ReceivedMsgAsync(Id, message).ConfigureAwait(false);
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

    private async Task GuardAsync(Func<CancellationToken, Task> loop, string activity)
    {
        try
        {
            await loop(_closed.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (Volatile.Read(ref _isClosed) == 1 || exception is OperationCanceledException)
        {
            // Closing cancels the loops and shuts the socket; the errors that causes are expected.
        }
        catch (Exception exception)
        {
            _log.LogError(exception, "Error {Activity} connection_id:{Id}", activity, Id);
        }
    }

    private void Close()
    {
        if (Interlocked.Exchange(ref _isClosed, 1) == 1)
        {
            return;
        }

        _outbound.Writer.TryComplete();
        _closed.Cancel();
        try
        {
            // Unblocks a send that is stuck on a client which stopped reading.
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception)
        {
        }
    }
}
