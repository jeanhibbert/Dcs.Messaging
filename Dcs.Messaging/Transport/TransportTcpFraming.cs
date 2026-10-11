using ProtoBuf;
using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.Messaging.Transport
{
    internal static class TransportTcpFraming
    {
        public static byte[] SerializeLengthPrefixed(TransportEnvelope envelope)
        {
            using var payloadStream = new MemoryStream();
            Serializer.Serialize(payloadStream, envelope);
            var payload = payloadStream.ToArray();
            var frame = new byte[4 + payload.Length];
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), payload.Length);
            Buffer.BlockCopy(payload, 0, frame, 4, payload.Length);
            return frame;
        }

        public static async Task<TransportEnvelope> ReadEnvelopeAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            var header = await ReadExactAsync(stream, 4, cancellationToken).ConfigureAwait(false);
            if (header == null)
            {
                return null;
            }

            var payloadLength = BinaryPrimitives.ReadInt32BigEndian(header);
            if (payloadLength <= 0)
            {
                return null;
            }

            var payload = await ReadExactAsync(stream, payloadLength, cancellationToken).ConfigureAwait(false);
            if (payload == null)
            {
                return null;
            }

            using var payloadStream = new MemoryStream(payload, writable: false);
            return Serializer.Deserialize<TransportEnvelope>(payloadStream);
        }

        public static async Task<byte[]> ReadExactAsync(NetworkStream stream, int length, CancellationToken cancellationToken)
        {
            var buffer = new byte[length];
            var offset = 0;

            while (offset < length)
            {
                var read = await stream.ReadAsync(buffer, offset, length - offset, cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    return null;
                }

                offset += read;
            }

            return buffer;
        }
    }
}
