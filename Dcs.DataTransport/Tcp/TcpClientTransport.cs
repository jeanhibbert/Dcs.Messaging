using Dcs.DataTransport.Internal;
using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.DataTransport.Tcp;

internal sealed class TcpClientTransport<T> : ClientTransport<T>
{
    public TcpClientTransport(IMessageCodec<T> codec, DataClientOptions options)
        : base(codec, options)
    {
    }

    public override async Task RunAsync(Action<T> handler, CancellationToken cancellationToken)
    {
        using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        if (Options.SocketReceiveBufferBytes > 0)
        {
            socket.ReceiveBufferSize = Options.SocketReceiveBufferBytes;
        }

        await socket.ConnectAsync(Options.Host, Options.Port, cancellationToken).ConfigureAwait(false);
        await socket.SendSubscriberNameAsync(Options.SubscriberName, cancellationToken).ConfigureAwait(false);

        // One receive typically delivers many frames, all decoded in place. Only a trailing partial frame is ever
        // copied, and only when it sits too close to the end of the buffer for the next receive.
        var buffer = GC.AllocateUninitializedArray<byte>(Options.ReceiveBufferBytes, pinned: true);
        var maxFrameLength = Framing.MaxFrameLength(Codec);
        var start = 0;
        var end = 0;

        while (true)
        {
            if (buffer.Length - end < maxFrameLength)
            {
                buffer.AsSpan(start, end - start).CopyTo(buffer);
                end -= start;
                start = 0;
            }

            var read = await socket.ReceiveAsync(buffer.AsMemory(end), SocketFlags.None, cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                if (end != start)
                {
                    throw new IOException("Server closed the connection part-way through a frame.");
                }

                return;
            }

            end += read;
            start += Framing.DecodeFrames(buffer.AsSpan(start, end - start), Codec, handler, out var count);
            AddReceived(count);

            if (start == end)
            {
                start = 0;
                end = 0;
            }
        }
    }
}
