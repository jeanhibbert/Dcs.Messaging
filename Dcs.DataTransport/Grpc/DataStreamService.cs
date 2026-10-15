using Grpc.AspNetCore.Server.Model;
using Grpc.Core;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Dcs.DataTransport.Grpc;

internal sealed class DataStreamService<T>
{
    private readonly GrpcServerTransport<T> _transport;

    public DataStreamService(GrpcServerTransport<T> transport)
    {
        _transport = transport;
    }

    public Task Subscribe(SubscribeRequest request, IServerStreamWriter<FrameBatch> responseStream, ServerCallContext context)
    {
        return _transport.RunSubscriberAsync(request, responseStream, context);
    }
}

/// <summary>
/// Binds <see cref="GrpcContract.Subscribe"/> to <see cref="DataStreamService{T}"/> without generated code.
/// </summary>
internal sealed class DataStreamMethodProvider<T> : IServiceMethodProvider<DataStreamService<T>>
{
    public void OnServiceMethodDiscovery(ServiceMethodProviderContext<DataStreamService<T>> context)
    {
        context.AddServerStreamingMethod(
            GrpcContract.Subscribe,
            new List<object>(),
            (service, request, responseStream, callContext) => service.Subscribe(request, responseStream, callContext));
    }
}

/// <summary>
/// The embedded host must not take over the application's Ctrl+C handling or lifetime; DataServer controls it.
/// </summary>
internal sealed class EmbeddedHostLifetime : IHostLifetime
{
    public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
