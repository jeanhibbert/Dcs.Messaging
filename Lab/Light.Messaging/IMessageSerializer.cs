using ProtoBuf;
using System;
using System.Buffers;

namespace Light.Messaging;

/// <summary>
/// Writes messages straight into the outgoing frame and reads them straight out of the receive buffer, so no
/// intermediate <c>byte[]</c> or stream is created. Implementations must be thread-safe.
/// </summary>
public interface IMessageSerializer
{
    void Serialize<T>(T message, IBufferWriter<byte> destination);

    T Deserialize<T>(ReadOnlySpan<byte> source);

    /// <summary>
    /// Called when a message type is first subscribed to or responded to, so building its serializer does not slow
    /// the first message down.
    /// </summary>
    void Prepare<T>();
}

/// <summary>
/// protobuf-net, the serializer Dcs.Messaging uses, so existing <c>[ProtoContract]</c> DTOs work unchanged.
/// </summary>
public sealed class ProtobufNetSerializer : IMessageSerializer
{
    public static readonly ProtobufNetSerializer Instance = new();

    public void Serialize<T>(T message, IBufferWriter<byte> destination)
    {
        Serializer.Serialize(destination, message);
    }

    public T Deserialize<T>(ReadOnlySpan<byte> source)
    {
        return Serializer.Deserialize<T>(source);
    }

    public void Prepare<T>()
    {
        Serializer.PrepareSerializer<T>();
    }
}
