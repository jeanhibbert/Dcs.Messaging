using ProtoBuf;
using ProtoBuf.Meta;
using System.Buffers;
using System.Runtime.InteropServices;

namespace LabEnhanced.Neutron;

/// <summary>
/// The wire format: a Base128 (varint) length followed by the protobuf payload. It is exactly what LabOriginal reads
/// and writes with <c>SerializeWithLengthPrefix(stream, message, PrefixStyle.Base128)</c>, so the two interoperate.
/// </summary>
internal static class Frame
{
    private const int MaxPrefixBytes = 5;
    private const int MaxRetainedBytes = 1024 * 1024;

    [ThreadStatic]
    private static ArrayBufferWriter<byte>? _scratch;

    // As LabOriginal: the kind of a DateTime travels with it.
    static Frame() => RuntimeTypeModel.Default.IncludeDateTimeKind = true;

    /// <summary>
    /// Serializes <paramref name="message"/> into this thread's scratch buffer and returns the framed bytes, which stay
    /// valid until the thread encodes again. Nothing is allocated once the buffer has grown to fit.
    /// </summary>
    public static ReadOnlySpan<byte> Encode<T>(T message)
    {
        var writer = _scratch is { Capacity: <= MaxRetainedBytes } ? _scratch : _scratch = new ArrayBufferWriter<byte>(256);
        writer.ResetWrittenCount();

        // The length is only known after serializing, so room for the longest prefix is left in front of the payload
        // and the prefix is then written right-aligned against it.
        writer.Advance(MaxPrefixBytes);
        Serializer.Serialize(writer, message);

        var frame = MemoryMarshal.AsMemory(writer.WrittenMemory).Span;
        var length = (uint)(frame.Length - MaxPrefixBytes);
        var start = MaxPrefixBytes - (length < 1 << 7 ? 1 : length < 1 << 14 ? 2 : length < 1 << 21 ? 3 : length < 1 << 28 ? 4 : 5);
        for (var i = start; i < MaxPrefixBytes; i++, length >>= 7)
        {
            frame[i] = (byte)(i < MaxPrefixBytes - 1 ? length | 0x80 : length);
        }

        return frame[start..];
    }

    /// <summary>
    /// Deserializes the frame at the front of <paramref name="data"/>. Returns the frame's size, or 0 if it has not
    /// fully arrived yet.
    /// </summary>
    public static int TryDecode<T>(ReadOnlySpan<byte> data, int maxMessageBytes, out T? message)
    {
        message = default;
        uint length = 0;
        for (var i = 0; i < data.Length; i++)
        {
            if (i == MaxPrefixBytes)
            {
                throw new InvalidDataException("Malformed length prefix.");
            }

            length |= (uint)(data[i] & 0x7F) << (7 * i);
            if (data[i] >= 0x80)
            {
                continue;
            }

            if (length > (uint)maxMessageBytes)
            {
                throw new InvalidDataException($"A {length} byte message exceeds MaxMessageBytes ({maxMessageBytes}).");
            }

            var size = i + 1 + (int)length;
            if (size > data.Length)
            {
                return 0;
            }

            message = Serializer.Deserialize<T>(data.Slice(i + 1, (int)length));
            return size;
        }

        return 0;
    }
}
