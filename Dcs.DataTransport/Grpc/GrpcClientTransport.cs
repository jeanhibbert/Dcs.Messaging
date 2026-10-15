using Dcs.DataTransport.Internal;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Buffers;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.DataTransport.Grpc;

internal sealed class GrpcClientTransport<T> : ClientTransport<T>
{
    public GrpcClientTransport(IMessageCodec<T> codec, DataClientOptions options)
        : base(codec, options)
    {
    }

    public override async Task RunAsync(Action<T> handler, CancellationToken cancellationToken)
    {
        using var channel = GrpcChannel.ForAddress(
            $"http://{Options.Host}:{Options.Port}",
            new GrpcChannelOptions
            {
                HttpHandler = new SocketsHttpHandler
                {
                    EnableMultipleHttp2Connections = true,
                    PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                },
                DisposeHttpClient = true,

                // Batch size is bounded by the server's MaxBatchBytes.
                MaxReceiveMessageSize = null,
                ThrowOperationCanceledOnCancellation = true,
            });

        var request = new SubscribeRequest { SubscriberName = Options.SubscriberName };
        using var call = channel.CreateCallInvoker().AsyncServerStreamingCall(
            GrpcContract.Subscribe,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        var responses = call.ResponseStream;
        while (await responses.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            var batch = responses.Current;
            try
            {
                var consumed = Framing.DecodeFrames(batch.Buffer.AsSpan(0, batch.Length), Codec, handler, out var count);
                AddReceived(count);

                if (consumed != batch.Length)
                {
                    throw new InvalidDataException("gRPC batch ended part-way through a frame.");
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(batch.Buffer);
            }
        }
    }
}
