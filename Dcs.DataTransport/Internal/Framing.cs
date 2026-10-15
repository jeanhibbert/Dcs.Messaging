using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Channels;

namespace Dcs.DataTransport.Internal;

/// <summary>
/// Wire format shared by both transports: each message is a frame of <c>int32 LE payloadLength | payload</c>.
/// TCP writes frames back to back on the socket; gRPC carries a run of them as one message.
/// </summary>
internal static class Framing
{
    public const int HeaderLength = sizeof(int);

    public static int MaxFrameLength<T>(IMessageCodec<T> codec) => HeaderLength + codec.MaxEncodedLength;

    /// <summary>
    /// Moves queued messages into <paramref name="buffer"/> until the queue is empty or another frame might not fit,
    /// and returns the bytes written. It never waits for more messages, so batching adds no latency: under light load
    /// a batch holds one message, and under heavy load one write carries many.
    /// </summary>
    public static int EncodeBatch<T>(ChannelReader<T> reader, IMessageCodec<T> codec, byte[] buffer, out int count)
    {
        var maxFrameLength = MaxFrameLength(codec);
        var destination = buffer.AsSpan();
        var length = 0;
        count = 0;

        while (destination.Length - length >= maxFrameLength && reader.TryRead(out var message))
        {
            var payloadLength = codec.Encode(in message, destination.Slice(length + HeaderLength));
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(length), payloadLength);
            length += HeaderLength + payloadLength;
            count++;
        }

        return length;
    }

    /// <summary>
    /// Decodes every complete frame in <paramref name="source"/>, passing each message to <paramref name="handler"/>,
    /// and returns the bytes consumed. A trailing partial frame is left for the caller to complete.
    /// </summary>
    public static int DecodeFrames<T>(ReadOnlySpan<byte> source, IMessageCodec<T> codec, Action<T> handler, out int count)
    {
        var maxPayloadLength = codec.MaxEncodedLength;
        var consumed = 0;
        count = 0;

        while (source.Length - consumed >= HeaderLength)
        {
            var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(consumed));
            if ((uint)payloadLength > (uint)maxPayloadLength)
            {
                throw new InvalidDataException($"Frame length {payloadLength} exceeds the codec maximum of {maxPayloadLength} bytes.");
            }

            if (source.Length - consumed - HeaderLength < payloadLength)
            {
                break;
            }

            handler(codec.Decode(source.Slice(consumed + HeaderLength, payloadLength)));
            consumed += HeaderLength + payloadLength;
            count++;
        }

        return consumed;
    }
}
