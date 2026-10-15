using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dcs.DataTransport.Internal;

namespace Dcs.DataTransport.Tcp;

internal static class SocketExtensions
{
    public const int MaxSubscriberNameBytes = 256;

    public static async ValueTask SendAllAsync(this Socket socket, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        while (!data.IsEmpty)
        {
            var sent = await socket.SendAsync(data, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            data = data.Slice(sent);
        }
    }

    public static async ValueTask ReceiveExactlyAsync(this Socket socket, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        while (!buffer.IsEmpty)
        {
            var read = await socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                throw new EndOfStreamException("Connection closed during handshake.");
            }

            buffer = buffer.Slice(read);
        }
    }

    /// <summary>
    /// Handshake, the only data a client sends: one frame holding the subscriber name in UTF-8.
    /// </summary>
    public static ValueTask SendSubscriberNameAsync(this Socket socket, string name, CancellationToken cancellationToken)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name ?? string.Empty);
        if (nameBytes.Length > MaxSubscriberNameBytes)
        {
            throw new ArgumentException($"Subscriber name must be at most {MaxSubscriberNameBytes} UTF-8 bytes.", nameof(name));
        }

        var frame = new byte[Framing.HeaderLength + nameBytes.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, nameBytes.Length);
        nameBytes.CopyTo(frame, Framing.HeaderLength);
        return socket.SendAllAsync(frame, cancellationToken);
    }

    public static async Task<string> ReceiveSubscriberNameAsync(this Socket socket, CancellationToken cancellationToken)
    {
        var header = new byte[Framing.HeaderLength];
        await socket.ReceiveExactlyAsync(header, cancellationToken).ConfigureAwait(false);

        var length = BinaryPrimitives.ReadInt32LittleEndian(header);
        if ((uint)length > MaxSubscriberNameBytes)
        {
            throw new InvalidDataException($"Handshake declared a {length} byte subscriber name.");
        }

        var name = new byte[length];
        await socket.ReceiveExactlyAsync(name, cancellationToken).ConfigureAwait(false);
        return Encoding.UTF8.GetString(name);
    }
}
