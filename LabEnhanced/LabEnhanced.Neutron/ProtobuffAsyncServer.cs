using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace LabEnhanced.Neutron;

/// <summary>
/// Accepts clients and exchanges framed protobuf messages with them. Received messages go to the
/// <see cref="IServerProtoMessageParser{T}"/>: one at a time and in order for each client, concurrently across clients.
/// </summary>
public sealed class ProtobuffAsyncServer<T> : IProtobuffAsyncServer<T> where T : IBaseMessage
{
    private readonly IServerProtoMessageParser<T> _messageParser;
    private readonly ProtobuffServerConfig<T> _config;
    private readonly ILogger<ProtobuffAsyncServer<T>> _log;
    private readonly Subject<IProtobuffClientConnection<T>> _connections = new();
    private readonly ISubject<IProtobuffClientConnection<T>> _disconnections = Subject.Synchronize(new Subject<IProtobuffClientConnection<T>>());
    private readonly ISubject<Exception> _errors = Subject.Synchronize(new Subject<Exception>());
    private Socket? _listener;
    private IDisposable? _heartBeat;

    public ProtobuffAsyncServer(IServerProtoMessageParser<T> messageParser, ProtobuffServerConfig<T> config, ILogger<ProtobuffAsyncServer<T>> log)
    {
        _messageParser = messageParser;
        _config = config;
        _log = log;
        ListenerAddress = config.ListenerAddress;
        OnClientConnection = _connections.AsObservable();
        OnClientDisconnection = _disconnections.AsObservable();
        OnServerError = _errors.AsObservable();
    }

    /// <summary>
    /// Each client as it connects, before any of its messages are read.
    /// </summary>
    public IObservable<IProtobuffClientConnection<T>> OnClientConnection { get; }

    /// <summary>
    /// Each client, once, when its connection has ended and it is no longer in <see cref="ClientConnections"/>.
    /// </summary>
    public IObservable<IProtobuffClientConnection<T>> OnClientDisconnection { get; }

    /// <summary>
    /// A failed start (when <see cref="ProtobuffServerConfig{T}.HandleStartException"/> is set), accept or heartbeat.
    /// The server carries on after every one of these.
    /// </summary>
    public IObservable<Exception> OnServerError { get; }

    /// <summary>
    /// The configured address; once started, the address actually bound.
    /// </summary>
    public IPEndPoint ListenerAddress { get; private set; }

    public IDictionary<string, IProtobuffClientConnection<T>> ClientConnections { get; set; } = new ConcurrentDictionary<string, IProtobuffClientConnection<T>>();

    public int ConnectedClientCount => ClientConnections.Count;

    public bool IsBound => _listener?.IsBound ?? false;

    public void Start()
    {
        _log.LogInformation("Starting ProtoBuff listener on {ListenerAddress}", ListenerAddress);
        var listener = _listener = new Socket(ListenerAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            // After a Stop this is the address bound last time, so a restart returns to the same port.
            listener.Bind(ListenerAddress);
            listener.Listen(512);
            ListenerAddress = (IPEndPoint)listener.LocalEndPoint!;
        }
        catch (Exception exception)
        {
            Stop();
            if (!_config.HandleStartException)
            {
                throw;
            }

            Report(exception, "ProtobuffAsyncServer Start");
            return;
        }

        _ = AcceptAsync(listener);

        // One timer for every client, and each heartbeat is serialized once however many clients there are.
        if (_config.HeartBeatMsg is { } heartBeat && _config.HeartBeatIntervalInSec > 0)
        {
            _heartBeat = Observable.Interval(TimeSpan.FromSeconds(_config.HeartBeatIntervalInSec)).Subscribe(_ =>
            {
                try
                {
                    SendMessage(heartBeat);
                }
                catch (Exception exception)
                {
                    Report(exception, "ProtobuffAsyncServer failed to send a heartbeat");
                }
            });
        }
    }

    public void Stop()
    {
        _log.LogInformation("Stopping ProtoBuff listener");
        _heartBeat?.Dispose();
        Interlocked.Exchange(ref _listener, null)?.Dispose();
        foreach (var connection in ClientConnections.Values)
        {
            connection.Disconnect();
        }
    }

    public bool Disconnect(IProtobuffClientConnection<T> connection)
    {
        _log.LogInformation("Disconnecting client connection {Id}", connection.Id);
        return ClientConnections.TryGetValue(connection.Id, out var known) && known.Disconnect();
    }

    public bool SendMessage(string id, T message)
    {
        return ClientConnections.TryGetValue(id, out var connection) && connection.SendMessage(message);
    }

    /// <summary>
    /// Sends to every client. The message is serialized once, and the call never waits on a socket, so one slow client
    /// delays neither the caller nor the other clients. False if any client did not take it.
    /// </summary>
    public bool SendMessage(T message)
    {
        var frame = Frame.Encode(message);
        var result = true;
        foreach (var (_, connection) in ClientConnections)
        {
            result &= connection is ProtobuffClientConnection<T> client ? client.Send(frame) : connection.SendMessage(message);
        }

        return result;
    }

    public void RemoveClientConnection(string id)
    {
        if (ClientConnections.Remove(id, out var connection))
        {
            _disconnections.OnNext(connection);
        }
        else
        {
            _log.LogWarning("{Id} Client Connection was not present", id);
        }
    }

    public void Dispose() => Stop();

    private async Task AcceptAsync(Socket listener)
    {
        while (true)
        {
            try
            {
                var connection = Accept(await listener.AcceptAsync().ConfigureAwait(false));
                if (_listener != listener)
                {
                    connection.Disconnect();
                }
            }
            catch (Exception exception)
            {
                if (_listener != listener)
                {
                    return;
                }

                // One failed accept (a client that reset straight away, say) must not stop the listener.
                Report(exception, "ProtobuffAsyncServer failed to accept a connection");
                await Task.Delay(100).ConfigureAwait(false);
            }
        }
    }

    private ProtobuffClientConnection<T> Accept(Socket socket)
    {
        ProtobuffClientConnection<T> connection;
        try
        {
            connection = new ProtobuffClientConnection<T>(this, $"{Guid.NewGuid()}_{socket.RemoteEndPoint}", socket, _messageParser, _config, _log);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        ClientConnections[connection.Id] = connection;
        try
        {
            _connections.OnNext(connection);
        }
        finally
        {
            // Started even if a subscriber threw, so the connection is always cleaned up when it ends.
            connection.Start();
        }

        return connection;
    }

    private void Report(Exception exception, string message)
    {
        _log.LogError(exception, "{Message}", message);
        _errors.OnNext(exception);
    }
}
