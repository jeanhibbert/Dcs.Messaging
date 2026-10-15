using Grpc.Core;
using System;
using System.Buffers;
using System.Text;

namespace Dcs.DataTransport.Grpc;

internal sealed class SubscribeRequest
{
    public string SubscriberName { get; init; }
}

/// <summary>
/// A run of frames, laid out exactly as on the TCP stream, carried as one gRPC message. Sending many frames per gRPC
/// message amortises the per-message costs (HTTP/2 DATA frame, gRPC header, flush) that otherwise dominate small messages.
/// </summary>
internal sealed class FrameBatch
{
    public FrameBatch(byte[] buffer, int length)
    {
        Buffer = buffer;
        Length = length;
    }

    public byte[] Buffer { get; }

    public int Length { get; set; }
}

/// <summary>
/// The service is defined directly with <see cref="Method{TRequest,TResponse}"/> and custom marshallers instead of
/// protoc-generated code, so frames are copied into gRPC's buffers as-is with no protobuf layer.
/// </summary>
internal static class GrpcContract
{
    public static readonly Method<SubscribeRequest, FrameBatch> Subscribe = new(
        MethodType.ServerStreaming,
        "dcs.datatransport.DataStream",
        "Subscribe",
        Marshallers.Create(
            (SubscribeRequest request) => Encoding.UTF8.GetBytes(request.SubscriberName ?? string.Empty),
            (byte[] bytes) => new SubscribeRequest { SubscriberName = Encoding.UTF8.GetString(bytes) }),
        Marshallers.Create<FrameBatch>(SerializeBatch, DeserializeBatch));

    private static void SerializeBatch(FrameBatch batch, SerializationContext context)
    {
        context.SetPayloadLength(batch.Length);
        var writer = context.GetBufferWriter();
        batch.Buffer.AsSpan(0, batch.Length).CopyTo(writer.GetSpan(batch.Length));
        writer.Advance(batch.Length);
        context.Complete();
    }

    /// <summary>
    /// The buffer is rented; the client returns it to <see cref="ArrayPool{T}.Shared"/> once the batch is decoded.
    /// </summary>
    private static FrameBatch DeserializeBatch(DeserializationContext context)
    {
        var payload = context.PayloadAsReadOnlySequence();
        var length = checked((int)payload.Length);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        payload.CopyTo(buffer);
        return new FrameBatch(buffer, length);
    }
}
