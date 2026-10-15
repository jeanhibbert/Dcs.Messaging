using Dcs.DataTransport.Internal;
using Grpc.AspNetCore.Server.Model;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.DataTransport.Grpc;

internal sealed class GrpcServerTransport<T> : IServerTransport
{
    private readonly DataServer<T> _server;
    private readonly IMessageCodec<T> _codec;
    private readonly DataServerOptions _options;
    private WebApplication _app;

    public GrpcServerTransport(DataServer<T> server, IMessageCodec<T> codec, DataServerOptions options)
    {
        _server = server;
        _codec = codec;
        _options = options;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var address = IPAddress.Parse(_options.Host);
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { Args = Array.Empty<string>() });

        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IHostLifetime, EmbeddedHostLifetime>();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.AddServerHeader = false;

            // Cleartext HTTP/2 (h2c): TLS is a deployment decision, and the demo measures transport cost without it.
            kestrel.Listen(address, _options.Port, listen => listen.Protocols = HttpProtocols.Http2);
        });

        builder.Services.AddGrpc();
        builder.Services.AddSingleton(new DataStreamService<T>(this));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IServiceMethodProvider<DataStreamService<T>>, DataStreamMethodProvider<T>>());

        _app = builder.Build();
        _app.MapGrpcService<DataStreamService<T>>();
        await _app.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_app == null)
        {
            return;
        }

        // DataServer has completed every subscriber queue. Kestrel waits for the streaming calls to drain and
        // finish, and aborts whatever is still running when the drain timeout expires.
        using (var drainTimeout = new CancellationTokenSource(_options.ShutdownDrainTimeout))
        {
            try
            {
                await _app.StopAsync(drainTimeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        await _app.DisposeAsync().ConfigureAwait(false);
    }

    internal async Task RunSubscriberAsync(SubscribeRequest request, IServerStreamWriter<FrameBatch> responseStream, ServerCallContext context)
    {
        var subscriber = _server.AddSubscriber(request.SubscriberName, TransportKind.Grpc, context.Peer);
        if (subscriber == null)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, "Server is shutting down."));
        }

        var closeReason = "server shutdown";
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(subscriber.DisconnectToken, context.CancellationToken);
        try
        {
            // Reused for every write: grpc-dotnet has serialized the batch by the time WriteAsync completes.
            var batch = new FrameBatch(GC.AllocateUninitializedArray<byte>(_options.MaxBatchBytes), 0);
            var reader = subscriber.Reader;

            while (await reader.WaitToReadAsync(stop.Token).ConfigureAwait(false))
            {
                batch.Length = Framing.EncodeBatch(reader, _codec, batch.Buffer, out var count);
                if (count == 0)
                {
                    continue;
                }

                // Completes when HTTP/2 flow control lets the batch out. Each subscriber has its own connection and
                // stream, so a client that reads slowly holds up only this loop.
                subscriber.BeginSend();
                await responseStream.WriteAsync(batch, stop.Token).ConfigureAwait(false);
                subscriber.EndSend(count);
            }
        }
        catch (Exception) when (subscriber.DisconnectToken.IsCancellationRequested)
        {
            // The server dropped this subscriber (queue full or send stalled): tell the client why.
            closeReason = subscriber.DisconnectReason;
            throw new RpcException(new Status(StatusCode.ResourceExhausted, closeReason));
        }
        catch (Exception exception) when (context.CancellationToken.IsCancellationRequested)
        {
            closeReason = exception is OperationCanceledException ? "connection closed" : exception.Message;
        }
        finally
        {
            _server.RemoveSubscriber(subscriber, closeReason);
        }
    }
}
