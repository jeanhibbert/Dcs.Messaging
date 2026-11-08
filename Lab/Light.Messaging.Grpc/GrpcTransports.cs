using Grpc.AspNetCore.Server.Model;
using Grpc.Core;
using Grpc.Net.Client;
using Light.Messaging.Internal;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging.Grpc;

/// <summary>
/// Hosts the messaging endpoint on an embedded Kestrel server: one bidirectional gRPC stream per client over HTTP/2.
/// Cleartext (h2c) by default; put TLS in front if the network requires it.
/// </summary>
public sealed class GrpcServerTransport : ServerTransport
{
    private WebApplication _app;

    public GrpcServerTransport(string host, int port)
    {
        Host = host;
        Port = port;
    }

    public string Host { get; }

    public int Port { get; }

    /// <summary>
    /// HTTP/2 PING interval that detects a client that vanished without closing. <see cref="TimeSpan.Zero"/> disables it.
    /// </summary>
    public TimeSpan KeepAlivePingDelay { get; init; } = TimeSpan.FromSeconds(10);

    internal override async Task StartAsync(IConnectionHost host, CancellationToken cancellationToken)
    {
        var address = IPAddress.Parse(Host);
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { Args = Array.Empty<string>() });

        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IHostLifetime, EmbeddedHostLifetime>();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.AddServerHeader = false;
            if (KeepAlivePingDelay > TimeSpan.Zero)
            {
                kestrel.Limits.Http2.KeepAlivePingDelay = KeepAlivePingDelay;
                kestrel.Limits.Http2.KeepAlivePingTimeout = KeepAlivePingDelay;
            }

            kestrel.Listen(address, Port, listen => listen.Protocols = HttpProtocols.Http2);
        });

        builder.Services.AddGrpc(grpc =>
        {
            grpc.MaxReceiveMessageSize = GrpcContract.MaxMessageSize(host.ConnectionSettings);
            grpc.MaxSendMessageSize = null;
        });
        builder.Services.AddSingleton(new TransportService(host));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IServiceMethodProvider<TransportService>, TransportMethodProvider>());

        _app = builder.Build();
        _app.MapGrpcService<TransportService>();
        await _app.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The server closes its connections before calling this, so Kestrel has no calls left to wait for.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_app == null)
        {
            return;
        }

        using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            try
            {
                await _app.StopAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        await _app.DisposeAsync().ConfigureAwait(false);
        _app = null;
    }

    internal sealed class TransportService
    {
        private readonly IConnectionHost _host;

        public TransportService(IConnectionHost host)
        {
            _host = host;
        }

        public Task Connect(IAsyncStreamReader<FrameBatch> requests, IServerStreamWriter<FrameBatch> responses, ServerCallContext context)
        {
            return _host.RunConnectionAsync(new GrpcServerConnection(_host.ConnectionSettings, requests, responses, context));
        }
    }

    /// <summary>
    /// Binds <see cref="GrpcContract.Connect"/> to <see cref="TransportService"/> without generated code.
    /// </summary>
    internal sealed class TransportMethodProvider : IServiceMethodProvider<TransportService>
    {
        public void OnServiceMethodDiscovery(ServiceMethodProviderContext<TransportService> context)
        {
            context.AddDuplexStreamingMethod(
                GrpcContract.Connect,
                new List<object>(),
                (service, requests, responses, callContext) => service.Connect(requests, responses, callContext));
        }
    }

    /// <summary>
    /// The embedded host must not take over the application's Ctrl+C handling or lifetime.
    /// </summary>
    internal sealed class EmbeddedHostLifetime : IHostLifetime
    {
        public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}

public sealed class GrpcClientTransport : ClientTransport
{
    public GrpcClientTransport(string host, int port)
        : this(new Uri($"http://{host}:{port}"))
    {
    }

    /// <param name="address">For example <c>http://10.0.0.5:5071</c> (h2c) or an <c>https://</c> address.</param>
    public GrpcClientTransport(Uri address)
    {
        Address = address;
    }

    public Uri Address { get; }

    /// <summary>
    /// HTTP/2 PING interval that detects a server that vanished without closing. <see cref="TimeSpan.Zero"/> disables it.
    /// </summary>
    public TimeSpan KeepAlivePingDelay { get; init; } = TimeSpan.FromSeconds(10);

    internal override Task<Connection> ConnectAsync(ConnectionSettings settings, CancellationToken cancellationToken)
    {
        var handler = new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
        };

        if (KeepAlivePingDelay > TimeSpan.Zero)
        {
            handler.KeepAlivePingDelay = KeepAlivePingDelay;
            handler.KeepAlivePingTimeout = KeepAlivePingDelay;
            handler.KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always;
        }

        var channel = GrpcChannel.ForAddress(Address, new GrpcChannelOptions
        {
            HttpHandler = handler,
            DisposeHttpClient = true,
            MaxReceiveMessageSize = GrpcContract.MaxMessageSize(settings),
            MaxSendMessageSize = null,
        });

        // The call's lifetime is the connection's: it ends when the connection closes, not on a token.
        var call = channel.CreateCallInvoker().AsyncDuplexStreamingCall(GrpcContract.Connect, null, new CallOptions());
        return Task.FromResult<Connection>(new GrpcClientConnection(settings, Address.ToString(), channel, call));
    }
}
