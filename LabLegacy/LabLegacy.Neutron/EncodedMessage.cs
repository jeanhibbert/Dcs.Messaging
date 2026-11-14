using ProtoBuf;
using System.Buffers;

namespace LabLegacy.Neutron;

/// <summary>
/// A message serialized once into a pooled buffer, already framed for the wire: a Base128 (varint) length followed by
/// the protobuf payload. This is exactly what the legacy
/// <c>Serializer.SerializeWithLengthPrefix(stream, message, PrefixStyle.Base128)</c> wrote, so existing clients read
/// it unchanged. A broadcast shares one instance between all connections by reference count; the last
/// <see cref="Release"/> returns the buffer to the pool.
/// </summary>
internal sealed class EncodedMessage : IBufferWriter<byte>
{
    /// <summary>
    /// Room kept in front of the payload for the length, which is only known after serializing. A varint of an int
    /// needs at most five bytes.
    /// </summary>
    private const int MaxPrefixLength = 5;

    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(256);
    private int _payloadLength;
    private int _start;
    private int _references = 1;

    private EncodedMessage()
    {
    }

    public ReadOnlyMemory<byte> Frame => _buffer.AsMemory(_start, MaxPrefixLength - _start + _payloadLength);

    /// <summary>
    /// Returns the encoded message holding one reference, owned by the caller.
    /// </summary>
    public static EncodedMessage Create<T>(T message)
    {
        var encoded = new EncodedMessage();
        try
        {
            Serializer.Serialize<T>(encoded, message);
            encoded.WritePrefix();
            return encoded;
        }
        catch
        {
            encoded.Release();
            throw;
        }
    }

    public void AddReference()
    {
        Interlocked.Increment(ref _references);
    }

    public void Release()
    {
        if (Interlocked.Decrement(ref _references) == 0)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null!;
        }
    }

    public void Advance(int count)
    {
        _payloadLength += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(MaxPrefixLength + _payloadLength);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(MaxPrefixLength + _payloadLength);
    }

    /// <summary>
    /// Writes the varint length right-aligned against the payload, so the frame is one contiguous block.
    /// </summary>
    private void WritePrefix()
    {
        Span<byte> prefix = stackalloc byte[MaxPrefixLength];
        var length = 0;
        var value = (uint)_payloadLength;
        do
        {
            var next = (byte)(value & 0x7F);
            value >>= 7;
            prefix[length++] = value == 0 ? next : (byte)(next | 0x80);
        }
        while (value != 0);

        _start = MaxPrefixLength - length;
        prefix.Slice(0, length).CopyTo(_buffer.AsSpan(_start));
    }

    private void EnsureCapacity(int sizeHint)
    {
        var required = MaxPrefixLength + _payloadLength + Math.Max(sizeHint, 1);
        if (required <= _buffer.Length)
        {
            return;
        }

        var larger = ArrayPool<byte>.Shared.Rent(Math.Max(required, _buffer.Length * 2));
        _buffer.AsSpan(0, MaxPrefixLength + _payloadLength).CopyTo(larger);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = larger;
    }
}
