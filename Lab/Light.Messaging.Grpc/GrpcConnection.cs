using Grpc.Core;
using Grpc.Net.Client;
using Light.Messaging.Internal;
using Microsoft.AspNetCore.Http;
using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging.Grpc;

/// <summary>
/// A run of whole frames, laid out exactly as on the TCP stream, carried as one gRPC message. Sending a batch per
/// message, instead of a message per frame, amortizes the per-message cost of HTTP/2 and gRPC.
/// </summary>
internal sealed class FrameBatch
{
    public ReadOnlyMemory<byte> Data { get; set; }

    /// <summary>
    /// Set on received batches; returned to <see cref="ArrayPool{T}.Shared"/> once the frames are processed.
    /// </summary>
    public byte[] RentedBuffer { get; init; }
}

/// <summary>
/// The service is defined with a hand-written <see cref="Method{TRequest,TResponse}"/> and marshaller instead of
/// protoc-generated code, so frames are copied into gRPC's buffers as-is with no protobuf wrapper.
/// </summary>
internal static class GrpcContract
{
    private static readonly Marshaller<FrameBatch> BatchMarshaller = Marshallers.Create<FrameBatch>(Serialize, Deserialize);

    public static readonly Method<FrameBatch, FrameBatch> Connect = new(
        MethodType.DuplexStreaming,
        "light.messaging.Transport",
        "Connect",
        BatchMarshaller,
        BatchMarshaller);

    /// <summary>
    /// Frames are validated by the connection, so gRPC's own size limit only needs to admit the largest batch.
    /// </summary>
    public static int MaxMessageSize(ConnectionSettings settings) => Math.Max(settings.MaxFrameBytes, settings.SendBatchBytes);

    private static void Serialize(FrameBatch batch, SerializationContext context)
    {
        var length = batch.Data.Length;
        context.SetPayloadLength(length);
        var writer = context.GetBufferWriter();
        batch.Data.Span.CopyTo(writer.GetSpan(length));
        writer.Advance(length);
        context.Complete();
    }

    private static FrameBatch Deserialize(DeserializationContext context)
    {
        var payload = context.PayloadAsReadOnlySequence();
        var length = checked((int)payload.Length);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        payload.CopyTo(buffer);
        return new FrameBatch { Data = buffer.AsMemory(0, length), RentedBuffer = buffer };
    }
}

internal abstract class GrpcConnection : Connection
{
    private readonly IAsyncStreamReader<FrameBatch> _incoming;
    private readonly IAsyncStreamWriter<FrameBatch> _outgoing;
    private readonly FrameBatch _outgoingBatch = new();

    protected GrpcConnection(ConnectionSettings settings, string remoteAddress, IAsyncStreamReader<FrameBatch> incoming, IAsyncStreamWriter<FrameBatch> outgoing)
        : base(settings, remoteAddress)
    {
        _incoming = incoming;
        _outgoing = outgoing;
    }

    protected override async ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        // Reused: gRPC has copied the bytes into its own buffer by the time WriteAsync completes.
        _outgoingBatch.Data = data;
        await _outgoing.WriteAsync(_outgoingBatch, cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        while (await _incoming.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            var batch = _incoming.Current;
            try
            {
                if (ProcessFrames(batch.Data.Span) != batch.Data.Length)
                {
                    throw new InvalidDataException("gRPC message ended part-way through a frame.");
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(batch.RentedBuffer);
            }
        }
    }
}

internal sealed class GrpcServerConnection : GrpcConnection
{
    private readonly ServerCallContext _context;

    public GrpcServerConnection(ConnectionSettings settings, IAsyncStreamReader<FrameBatch> requests, IServerStreamWriter<FrameBatch> responses, ServerCallContext context)
        : base(settings, context.Peer, requests, responses)
    {
        _context = context;
    }

    /// <summary>
    /// Resets the HTTP/2 stream, which fails any read or write still pending on it. Not after a graceful shutdown:
    /// a reset would discard responses Kestrel has not yet flushed, so the call is ended by returning instead.
    /// </summary>
    protected override void AbortTransport()
    {
        if (!IsClosingGracefully)
        {
            _context.GetHttpContext().Abort();
        }
    }
}

internal sealed class GrpcClientConnection : GrpcConnection
{
    private readonly GrpcChannel _channel;
    private readonly AsyncDuplexStreamingCall<FrameBatch, FrameBatch> _call;

    public GrpcClientConnection(ConnectionSettings settings, string remoteAddress, GrpcChannel channel, AsyncDuplexStreamingCall<FrameBatch, FrameBatch> call)
        : base(settings, remoteAddress, call.ResponseStream, call.RequestStream)
    {
        _channel = channel;
        _call = call;
    }

    protected override ValueTask CompleteSendAsync() => new(_call.RequestStream.CompleteAsync());

    protected override void AbortTransport()
    {
        _call.Dispose();
        _channel.Dispose();
    }
}
