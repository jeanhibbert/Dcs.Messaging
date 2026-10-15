using System;

namespace Dcs.DataTransport;

/// <summary>
/// Encodes and decodes <typeparamref name="T"/> directly to and from spans, so the transports never allocate per message.
/// Implementations must be thread-safe (one instance is shared by every connection).
/// </summary>
public interface IMessageCodec<T>
{
    /// <summary>
    /// Upper bound on the encoded size of any message. Used to size buffers and to reject corrupt frames.
    /// </summary>
    int MaxEncodedLength { get; }

    /// <summary>
    /// Writes <paramref name="message"/> to <paramref name="destination"/>, which is at least
    /// <see cref="MaxEncodedLength"/> bytes long, and returns the number of bytes written.
    /// </summary>
    int Encode(in T message, Span<byte> destination);

    /// <summary>
    /// Reads one message from exactly the bytes <see cref="Encode"/> produced.
    /// </summary>
    T Decode(ReadOnlySpan<byte> source);
}
