using Light.Messaging.Internal;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging;

/// <summary>
/// Accepts connections for a <see cref="MessagingServer"/>: <see cref="Tcp.TcpServerTransport"/>, or
/// <c>GrpcServerTransport</c> from Light.Messaging.Grpc.
/// </summary>
public abstract class ServerTransport : IAsyncDisposable
{
    internal ServerTransport()
    {
    }

    internal abstract Task StartAsync(IConnectionHost host, CancellationToken cancellationToken);

    /// <summary>
    /// Stops accepting connections. Called by the server after its connections have closed.
    /// </summary>
    public abstract ValueTask DisposeAsync();
}

/// <summary>
/// Opens the connection for a <see cref="MessagingClient"/>: <see cref="Tcp.TcpClientTransport"/>, or
/// <c>GrpcClientTransport</c> from Light.Messaging.Grpc.
/// </summary>
public abstract class ClientTransport
{
    internal ClientTransport()
    {
    }

    internal abstract Task<Connection> ConnectAsync(ConnectionSettings settings, CancellationToken cancellationToken);
}

internal interface IConnectionHost
{
    ConnectionSettings ConnectionSettings { get; }

    /// <summary>
    /// Runs an accepted connection until it closes. Never throws.
    /// </summary>
    Task RunConnectionAsync(Connection connection);
}
