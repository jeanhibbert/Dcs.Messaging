using System.Buffers;
using System.Net;
using System.Net.Sockets;

namespace LabEnhanced.Neutron;

/// <summary>
/// One socket carrying framed messages in both directions. Shared by the server's per-client connection and by the
/// client, so both ends read and write the same way:
/// <list type="bullet">
/// <item>Sending appends the frame to a buffer and returns. A flush on the thread pool then writes everything
/// buffered in one socket call, so concurrent senders cannot interleave bytes, and under load many messages share a
/// system call.</item>
/// <item>Receiving is asynchronous, and every message a socket read delivered is deserialized straight from the
/// receive buffer. No thread is blocked waiting for data.</item>
/// </list>
/// </summary>
internal sealed class Wire : IThreadPoolWorkItem
{
    private const int ReceiveBufferBytes = 64 * 1024;
    private const int MaxRetainedBytes = 1024 * 1024;

    private readonly Socket _socket;
    private readonly int _maxMessageBytes;
    private readonly int _maxSendBacklogBytes;
    private readonly Lock _gate = new();
    private ArrayBufferWriter<byte> _queued = new();
    private ArrayBufferWriter<byte> _sending = new();
    private Exception? _closeReason;
    private bool _flushing;
    private volatile bool _closed;

    public Wire(Socket socket, int maxMessageBytes, int maxSendBacklogBytes)
    {
        _socket = socket;
        _maxMessageBytes = maxMessageBytes;
        _maxSendBacklogBytes = maxSendBacklogBytes;

        // Messages leave as soon as the flush sends them; batching is its job, not Nagle's.
        socket.NoDelay = true;
        RemoteEndPoint = (IPEndPoint)socket.RemoteEndPoint!;
    }

    public IPEndPoint RemoteEndPoint { get; }

    public bool IsClosed => _closed;

    /// <summary>
    /// Queues an encoded message and returns immediately. False if the connection is closed, or if the peer is so far
    /// behind that it is now being disconnected.
    /// </summary>
    public bool Send(ReadOnlySpan<byte> frame)
    {
        lock (_gate)
        {
            if (_closed)
            {
                return false;
            }

            if (_queued.WrittenCount > _maxSendBacklogBytes)
            {
                Close(new IOException($"{RemoteEndPoint} is not reading fast enough."));
                return false;
            }

            _queued.Write(frame);
            if (_flushing)
            {
                return true;
            }

            _flushing = true;
        }

        ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
        return true;
    }

    /// <summary>
    /// Receives until the connection ends, awaiting <paramref name="onMessage"/> for each message in order. Returns
    /// why the connection ended: null when either side simply closed it.
    /// </summary>
    public async Task<Exception?> RunAsync<T>(Func<T, Task> onMessage)
    {
        try
        {
            var buffer = new byte[ReceiveBufferBytes];
            for (int end = 0, read; (read = await _socket.ReceiveAsync(buffer.AsMemory(end), SocketFlags.None).ConfigureAwait(false)) > 0;)
            {
                end += read;

                var start = 0;
                for (int size; (size = Frame.TryDecode(buffer.AsSpan(start, end - start), _maxMessageBytes, out T? message)) > 0; start += size)
                {
                    await onMessage(message!).ConfigureAwait(false);
                }

                // Keep a trailing partial message at the front, and make room if it fills the buffer.
                buffer.AsSpan(start, end - start).CopyTo(buffer);
                end -= start;
                if (end == buffer.Length)
                {
                    Array.Resize(ref buffer, end * 2);
                }
            }
        }
        catch (Exception exception)
        {
            Close(exception);
        }

        Close();
        return _closeReason;
    }

    /// <summary>
    /// Closes abruptly: queued messages are discarded. Only the first call has any effect. Disposing the socket ends
    /// the receive loop and any send in progress.
    /// </summary>
    public void Close(Exception? reason = null)
    {
        lock (_gate)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            _closeReason = reason;
        }

        _socket.Dispose();
    }

    /// <summary>
    /// The flush: writes everything queued, and keeps going until the queue is empty.
    /// </summary>
    async void IThreadPoolWorkItem.Execute()
    {
        try
        {
            while (true)
            {
                lock (_gate)
                {
                    _sending.ResetWrittenCount();
                    if (_queued.WrittenCount == 0)
                    {
                        _flushing = false;
                        if (_sending.Capacity > MaxRetainedBytes)
                        {
                            _sending = new ArrayBufferWriter<byte>();
                        }

                        return;
                    }

                    (_queued, _sending) = (_sending, _queued);
                }

                for (var data = _sending.WrittenMemory; !data.IsEmpty;)
                {
                    data = data[await _socket.SendAsync(data, SocketFlags.None).ConfigureAwait(false)..];
                }
            }
        }
        catch (Exception exception)
        {
            Close(exception);
        }
    }
}
