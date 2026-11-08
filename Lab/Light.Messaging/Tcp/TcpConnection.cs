using Light.Messaging.Internal;
using System;
using System.Buffers;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging.Tcp;

internal sealed class TcpConnection : Connection
{
    /// <summary>
    /// When less than this is free at the end of the receive buffer, the partial frame is moved to the front.
    /// </summary>
    private const int MinReceiveSpace = 16 * 1024;

    private readonly Socket _socket;
    private readonly int _receiveBufferBytes;

    public TcpConnection(Socket socket, ConnectionSettings settings, int receiveBufferBytes)
        : base(settings, socket.RemoteEndPoint?.ToString())
    {
        _socket = socket;
        _receiveBufferBytes = receiveBufferBytes;
    }

    protected override async ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        while (!data.IsEmpty)
        {
            var sent = await _socket.SendAsync(data, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            data = data.Slice(sent);
        }
    }

    protected override ValueTask CompleteSendAsync()
    {
        _socket.Shutdown(SocketShutdown.Send);
        return ValueTask.CompletedTask;
    }

    protected override void AbortTransport()
    {
        _socket.Dispose();
    }

    /// <summary>
    /// One receive typically delivers many frames, all decoded in place. Only a trailing partial frame is ever copied,
    /// and a frame larger than the buffer temporarily gets a pooled buffer of its own.
    /// </summary>
    protected override async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        var standard = GC.AllocateUninitializedArray<byte>(_receiveBufferBytes, pinned: true);
        var buffer = standard;
        var start = 0;
        var end = 0;

        try
        {
            while (true)
            {
                if (buffer.Length - end < MinReceiveSpace)
                {
                    var pending = end - start;
                    var required = Math.Max(PeekFrameSize(buffer.AsSpan(start, pending)), pending) + MinReceiveSpace;
                    var target = required <= buffer.Length ? buffer : ArrayPool<byte>.Shared.Rent(required);

                    buffer.AsSpan(start, pending).CopyTo(target);
                    if (target != buffer && buffer != standard)
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }

                    buffer = target;
                    start = 0;
                    end = pending;
                }

                var read = await _socket.ReceiveAsync(buffer.AsMemory(end), SocketFlags.None, cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    if (end != start)
                    {
                        throw new IOException("Connection closed part-way through a frame.");
                    }

                    return;
                }

                end += read;
                start += ProcessFrames(buffer.AsSpan(start, end - start));

                if (start == end)
                {
                    start = 0;
                    end = 0;
                    if (buffer != standard)
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                        buffer = standard;
                    }
                }
            }
        }
        finally
        {
            if (buffer != standard)
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
