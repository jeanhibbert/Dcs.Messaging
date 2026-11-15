using Microsoft.Extensions.Logging;
using ProtoBuf.Meta;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace LabLegacy.Neutron;

/// <summary>
/// Accepts clients and exchanges protobuf messages with them, each prefixed with a Base128 length. The public shape
/// and the wire format match the legacy server, so existing clients work unchanged. What changed underneath:
/// <list type="bullet">
/// <item>A broadcast serializes the message once and queues the same bytes to every client. It never touches a
/// socket, so one slow client no longer delays the others or the caller.</item>
/// <item>One heartbeat timer serves every connection, instead of a timer per connection.</item>
/// <item>The accept loop is cancellable and survives a failed accept.</item>
/// </list>
/// </summary>
public sealed class NeutronAsyncServer<T> : INeutronAsyncServer<T>
    where T : IBaseMessage
{
    private readonly IServerProtoMessageParser<T> _messageParser;
    private readonly NeutronServerConfig<T> _config;
    private readonly ILogger<NeutronAsyncServer<T>> _log;
    private readonly ConcurrentDictionary<string, INeutronClientConnection<T>> _connections = new();
    private CancellationTokenSource? _cts;
    private Socket? _listener;
    private bool _disposed;

    public NeutronAsyncServer(IServerProtoMessageParser<T> messageParser, NeutronServerConfig<T> config, ILogger<NeutronAsyncServer<T>> log)
    {
        RuntimeTypeModel.Default.IncludeDateTimeKind = true;
        _messageParser = messageParser;
        _config = config;
        _log = log;
        ListenerAddress = config.ListenerAddress;
    }

    public event EventHandler<INeutronClientConnection<T>>? OnClientConnection;

    public event EventHandler<INeutronClientConnection<T>>? OnClientDisconnection;

    public event EventHandler<Exception>? OnServerError;

    /// <summary>
    /// The configured address; once started, the address actually bound (useful with port 0).
    /// </summary>
    public IPEndPoint ListenerAddress { get; private set; }

    public IDictionary<string, INeutronClientConnection<T>> ClientConnections => _connections;

    public int ConnectedClientCount => _connections.Count;

    public bool IsBound => _listener?.IsBound ?? false;

    public void Start()
    {
        try
        {
            _log.LogInformation("Starting Neutron Listener on {ListenerAddress}", ListenerAddress);

            _listener = new Socket(ListenerAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _listener.Bind(ListenerAddress);
            _listener.Listen(512);
            ListenerAddress = (IPEndPoint)_listener.LocalEndPoint!;

            _cts = new CancellationTokenSource();
            _ = AcceptConnectionsAsync(_listener, _cts.Token);
            if (_config.HeartBeatMsg != null && _config.HeartBeatIntervalInSec > 0)
            {
                _ = SendHeartBeatsAsync(_config.HeartBeatMsg, TimeSpan.FromSeconds(_config.HeartBeatIntervalInSec), _cts.Token);
            }
        }
        catch (Exception exception) when (_config.HandleStartException)
        {
            _log.LogError(exception, "NeutronAsyncServer.Start");
            HandleServerException(exception);
        }
    }

    public void Stop()
    {
        if (_listener == null)
        {
            return;
        }

        _log.LogInformation("Stopping Neutron Listener");
        _cts?.Cancel();
        _listener?.Dispose();
        _listener = null;
        DisconnectAll();
    }

    public bool Disconnect(INeutronClientConnection<T> connection)
    {
        _log.LogInformation("Disconnecting client connection {Id}", connection.Id);
        return _connections.TryGetValue(connection.Id, out var known) && known.Disconnect();
    }

    public bool SendMessage(string id, T message)
    {
        return _connections.TryGetValue(id, out var connection) && connection.SendMessage(message);
    }

    public bool SendMessage(T message)
    {
        if (_connections.IsEmpty)
        {
            return true;
        }

        // Serialized once; every connection queues the same bytes and holds a reference until it has sent them.
        var encoded = EncodedMessage.Create(message);
        try
        {
            var result = true;
            foreach (var connection in _connections.Values)
            {
                if (connection is NeutronClientConnection<T> client)
                {
                    encoded.AddReference();
                    result &= client.Send(encoded);
                }
                else
                {
                    result &= connection.SendMessage(message);
                }
            }

            return result;
        }
        finally
        {
            encoded.Release();
        }
    }

    public void RemoveClientConnection(string id)
    {
        if (!_connections.TryRemove(id, out _))
        {
            _log.LogWarning("{Id} Client Connection was not present", id);
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        Stop();
        _cts?.Dispose();
    }

    private async Task AcceptConnectionsAsync(Socket listener, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                HandleConnection(await listener.AcceptAsync(cancellationToken).ConfigureAwait(false));
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                // One failed accept (a client that reset straight away, say) must not stop the listener.
                _log.LogError(exception, "NeutronAsyncServer failed to accept a connection");
                HandleServerException(exception);
            }
        }
    }

    private void HandleConnection(Socket socket)
    {
        var remote = (IPEndPoint)socket.RemoteEndPoint!;
        var id = $"{Guid.NewGuid()}_{remote.Address}:{remote.Port}";

        var connection = new NeutronClientConnection<T>(this, id, socket, _messageParser, _config, _log);
        connection.OnClientDisconnection += (_, closed) => OnClientDisconnection?.Invoke(this, closed);
        _connections[id] = connection;

        OnClientConnection?.Invoke(this, connection);
        connection.Start();
    }

    /// <summary>
    /// One timer for all connections, and each heartbeat is serialized once however many clients there are.
    /// </summary>
    private async Task SendHeartBeatsAsync(T heartBeat, TimeSpan interval, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                SendMessage(heartBeat);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            _log.LogError(exception, "NeutronAsyncServer failed to send a heartbeat");
            HandleServerException(exception);
        }
    }

    private void DisconnectAll()
    {
        foreach (var connection in _connections.Values)
        {
            connection.Disconnect();
        }
    }

    private void HandleServerException(Exception exception)
    {
        OnServerError?.Invoke(this, exception);
    }
}
