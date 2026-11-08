using Light.Messaging.Internal;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging;

/// <summary>
/// Accepts client connections; publishes to clients, receives what they publish, and answers their requests.
/// <code>
/// await using var server = await MessagingServer.StartAsync(new TcpServerTransport("0.0.0.0", 5070));
/// using var pricing = server.Respond&lt;PricingRequest, ForwardPrice&gt;(Endpoints.Pricing, (request, context) => Price(request));
/// server.Publish(Endpoints.Prices, tick);
/// </code>
/// </summary>
public sealed class MessagingServer : MessagingSession, IConnectionOwner, IConnectionHost
{
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(5);

    private readonly object _gate = new();
    private readonly ServerTransport _transport;
    private readonly ConcurrentDictionary<Connection, Task> _running = new();

    // Copy-on-write maps, read without locks on every publish and request.
    private Dictionary<int, Connection[]> _interest = new();
    private Dictionary<int, Responder> _responders = new();
    private Peer[] _peers = Array.Empty<Peer>();
    private long _nextPeerId;
    private bool _disposed;

    private MessagingServer(ServerTransport transport, MessagingServerOptions options)
        : base(options, "server")
    {
        _transport = transport;
    }

    public event Action<Peer> PeerConnected;

    public event Action<Peer> PeerDisconnected;

    public IReadOnlyList<Peer> Peers => Volatile.Read(ref _peers);

    ConnectionSettings IConnectionHost.ConnectionSettings => ConnectionSettings;

    public static async Task<MessagingServer> StartAsync(ServerTransport transport, MessagingServerOptions options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transport);
        var server = new MessagingServer(transport, options ?? new MessagingServerOptions());
        try
        {
            await transport.StartAsync(server, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transport.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        server.StartWatchdog();
        return server;
    }

    /// <summary>
    /// Sends <paramref name="message"/> to every client subscribed to <paramref name="endpoint"/>. It is serialized
    /// once and shared by all of them, and not at all if nobody is subscribed. Never blocks: each client has its own
    /// queue, and a full queue is handled by <see cref="MessagingOptions.SlowConsumerPolicy"/>. Thread-safe.
    /// </summary>
    public void Publish<T>(Endpoint endpoint, T message)
    {
        if (!Volatile.Read(ref _interest).TryGetValue(endpoint.Id, out var subscribers))
        {
            return;
        }

        var frame = CreateFrame(FrameKind.Publish, endpoint.Id, 0, message);
        foreach (var connection in subscribers)
        {
            frame.AddReference();
            connection.Enqueue(frame);
        }

        frame.Release();
    }

    /// <summary>
    /// Sends <paramref name="message"/> to one client, whether or not it has subscribed to <paramref name="endpoint"/>.
    /// Returns false if the client has disconnected or its queue is full.
    /// </summary>
    public bool Publish<T>(Endpoint endpoint, T message, Peer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        return !peer.Connection.IsClosed && peer.Connection.Enqueue(CreateFrame(FrameKind.Publish, endpoint.Id, 0, message));
    }

    /// <summary>
    /// Answers requests sent to <paramref name="endpoint"/>. The handler runs on the requesting client's receive loop;
    /// if it throws, the client's request fails with <see cref="RemoteException"/>. One responder per endpoint.
    /// </summary>
    public IDisposable Respond<TRequest, TResponse>(Endpoint endpoint, Func<TRequest, MessageContext, TResponse> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return AddResponder<TRequest, TResponse>(new Responder<TRequest, TResponse>(this, endpoint, handler, null));
    }

    /// <summary>
    /// Registers an asynchronous responder, for a handler that must wait on I/O. While it waits, the client's later
    /// messages keep being processed. Otherwise as <see cref="Respond{TRequest, TResponse}"/>.
    /// </summary>
    public IDisposable RespondAsync<TRequest, TResponse>(Endpoint endpoint, Func<TRequest, MessageContext, ValueTask<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return AddResponder<TRequest, TResponse>(new Responder<TRequest, TResponse>(this, endpoint, null, handler));
    }

    /// <summary>
    /// Stops accepting clients, lets each connection send what it has queued (up to
    /// <see cref="MessagingOptions.ShutdownDrainTimeout"/>), then closes them all.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        await StopWatchdogAsync().ConfigureAwait(false);

        foreach (var connection in _running.Keys)
        {
            connection.Shutdown("server shutting down");
        }

        var running = Task.WhenAll(_running.Values);
        if (await Task.WhenAny(running, Task.Delay(ShutdownDrainTimeout)).ConfigureAwait(false) != running)
        {
            foreach (var connection in _running.Keys)
            {
                connection.Close("server shutdown drain timed out");
            }
        }

        await running.ConfigureAwait(false);
        await _transport.DisposeAsync().ConfigureAwait(false);
    }

    Task IConnectionHost.RunConnectionAsync(Connection connection)
    {
        connection.Id = Interlocked.Increment(ref _nextPeerId);
        var run = connection.RunAsync(this);
        _running[connection] = run;
        _ = run.ContinueWith(_ => _running.TryRemove(connection, out _), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        _ = CloseIfNoHandshakeAsync(connection);
        return run;
    }

    void IConnectionOwner.OnConnected(Connection connection)
    {
        lock (_gate)
        {
            if (_disposed)
            {
                connection.Close("server shutting down");
                return;
            }

            _peers = [.. _peers, connection.Peer];
        }

        PeerConnected?.Invoke(connection.Peer);
    }

    void IConnectionOwner.OnFrame(Connection connection, FrameKind kind, int endpointId, long correlationId, ReadOnlySpan<byte> payload)
    {
        switch (kind)
        {
            case FrameKind.Publish:
                DispatchPublish(connection, endpointId, payload);
                break;

            case FrameKind.Request:
                if (Volatile.Read(ref _responders).TryGetValue(endpointId, out var responder))
                {
                    responder.Handle(connection, correlationId, payload);
                }
                else
                {
                    SendError(connection, endpointId, correlationId, $"No responder for endpoint id {endpointId}.");
                }

                break;

            case FrameKind.Subscribe:
                if (connection.RemoteSubscriptions.Add(endpointId))
                {
                    UpdateInterest(endpointId, connection, subscribe: true);
                }

                break;

            case FrameKind.Unsubscribe:
                if (connection.RemoteSubscriptions.Remove(endpointId))
                {
                    UpdateInterest(endpointId, connection, subscribe: false);
                }

                break;

            default:
                throw new InvalidDataException($"Unexpected {kind} frame from a client.");
        }
    }

    void IConnectionOwner.OnClosed(Connection connection)
    {
        lock (_gate)
        {
            _peers = Array.FindAll(_peers, peer => peer != connection.Peer);

            if (connection.RemoteSubscriptions.Count > 0)
            {
                var interest = new Dictionary<int, Connection[]>(_interest);
                foreach (var endpointId in connection.RemoteSubscriptions)
                {
                    RemoveInterest(interest, endpointId, connection);
                }

                Volatile.Write(ref _interest, interest);
            }
        }

        PeerDisconnected?.Invoke(connection.Peer);
    }

    internal static void SendError(Connection connection, int endpointId, long correlationId, string message)
    {
        var frame = Frame.Rent(FrameKind.ErrorResponse, endpointId, correlationId);
        frame.WriteUtf8(message.Length > 4096 ? message.Substring(0, 4096) : message);
        frame.Seal(connection.Settings.MaxFrameBytes);
        connection.Enqueue(frame);
    }

    private protected override IEnumerable<Connection> ActiveConnections() => Volatile.Read(ref _peers).Select(peer => peer.Connection);

    /// <summary>
    /// A socket that connects but never says Hello would otherwise hold its resources forever.
    /// </summary>
    private static async Task CloseIfNoHandshakeAsync(Connection connection)
    {
        await Task.WhenAny(connection.Handshake, Task.Delay(HandshakeTimeout)).ConfigureAwait(false);
        if (!connection.Handshake.IsCompleted)
        {
            connection.Close("handshake timed out");
        }
    }

    private IDisposable AddResponder<TRequest, TResponse>(Responder responder)
    {
        Serializer.Prepare<TRequest>();
        Serializer.Prepare<TResponse>();

        lock (_gate)
        {
            if (_responders.ContainsKey(responder.Endpoint.Id))
            {
                throw new InvalidOperationException($"Endpoint '{responder.Endpoint}' already has a responder.");
            }

            Volatile.Write(ref _responders, new Dictionary<int, Responder>(_responders) { [responder.Endpoint.Id] = responder });
        }

        return new DisposableAction(() =>
        {
            lock (_gate)
            {
                var responders = new Dictionary<int, Responder>(_responders);
                responders.Remove(responder.Endpoint.Id);
                Volatile.Write(ref _responders, responders);
            }
        });
    }

    private void UpdateInterest(int endpointId, Connection connection, bool subscribe)
    {
        lock (_gate)
        {
            if (connection.IsClosed)
            {
                return;
            }

            var interest = new Dictionary<int, Connection[]>(_interest);
            if (subscribe)
            {
                interest.TryGetValue(endpointId, out var existing);
                interest[endpointId] = [.. existing ?? Array.Empty<Connection>(), connection];
            }
            else
            {
                RemoveInterest(interest, endpointId, connection);
            }

            Volatile.Write(ref _interest, interest);
        }
    }

    private static void RemoveInterest(Dictionary<int, Connection[]> interest, int endpointId, Connection connection)
    {
        if (!interest.TryGetValue(endpointId, out var existing))
        {
            return;
        }

        var remaining = Array.FindAll(existing, candidate => candidate != connection);
        if (remaining.Length == 0)
        {
            interest.Remove(endpointId);
        }
        else
        {
            interest[endpointId] = remaining;
        }
    }
}
