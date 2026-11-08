using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;

namespace Light.Messaging.Internal;

/// <summary>
/// One encoded frame, ready to send. A message is serialized once, straight into the frame (it is the serializer's
/// <see cref="IBufferWriter{T}"/>), and the same frame is then queued to every peer that should receive it. Each queue
/// holds a reference; the last <see cref="Release"/> returns the frame to a pool, so steady-state publishing allocates
/// nothing.
/// </summary>
internal sealed class Frame : IBufferWriter<byte>
{
    private const int InitialBufferSize = 512;

    /// <summary>
    /// Pooled frames keep their buffer only if it is small, so one large message cannot pin a large buffer for good.
    /// </summary>
    private const int MaxRetainedBufferSize = 4096;

    private static readonly ConcurrentQueue<Frame> Pool = new();

    private byte[] _buffer;
    private int _length;
    private int _references;

    private Frame()
    {
    }

    public int Length => _length;

    public ReadOnlySpan<byte> Span => new(_buffer, 0, _length);

    public ReadOnlyMemory<byte> Memory => new(_buffer, 0, _length);

    /// <summary>
    /// Returns a frame with its header written and one reference, owned by the caller. Write the payload through the
    /// <see cref="IBufferWriter{T}"/> members, then call <see cref="Seal"/>.
    /// </summary>
    public static Frame Rent(FrameKind kind, int endpointId, long correlationId = 0)
    {
        if (!Pool.TryDequeue(out var frame))
        {
            frame = new Frame();
        }

        frame._buffer ??= ArrayPool<byte>.Shared.Rent(InitialBufferSize);
        frame._references = 1;

        var header = frame._buffer.AsSpan(Wire.LengthSize);
        header[0] = (byte)kind;
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(1), endpointId);
        if (Wire.HasCorrelation(kind))
        {
            BinaryPrimitives.WriteInt64LittleEndian(header.Slice(Wire.MinBodySize), correlationId);
        }

        frame._length = Wire.LengthSize + Wire.BodyHeaderSize(kind);
        return frame;
    }

    /// <summary>
    /// Writes the length prefix once the payload is complete.
    /// </summary>
    public void Seal(int maxFrameBytes)
    {
        if (_length > maxFrameBytes)
        {
            throw new MessagingException($"A {_length} byte frame exceeds MaxFrameBytes ({maxFrameBytes}).");
        }

        BinaryPrimitives.WriteInt32LittleEndian(_buffer, _length - Wire.LengthSize);
    }

    public void WriteUtf8(string text)
    {
        var span = GetSpan(Encoding.UTF8.GetMaxByteCount(text.Length));
        Advance(Encoding.UTF8.GetBytes(text, span));
    }

    public void AddReference()
    {
        Interlocked.Increment(ref _references);
    }

    public void Release()
    {
        if (Interlocked.Decrement(ref _references) != 0)
        {
            return;
        }

        if (_buffer.Length > MaxRetainedBufferSize)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null;
        }

        Pool.Enqueue(this);
    }

    public void Advance(int count)
    {
        _length += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_length);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_length);
    }

    private void EnsureCapacity(int sizeHint)
    {
        var required = _length + Math.Max(sizeHint, 1);
        if (required <= _buffer.Length)
        {
            return;
        }

        var larger = ArrayPool<byte>.Shared.Rent(Math.Max(required, _buffer.Length * 2));
        _buffer.AsSpan(0, _length).CopyTo(larger);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = larger;
    }
}
