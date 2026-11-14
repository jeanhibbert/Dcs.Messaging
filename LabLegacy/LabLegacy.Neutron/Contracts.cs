using System.Net;

namespace LabLegacy.Neutron;

/// <summary>
/// Marker for the root message type exchanged on a connection.
/// </summary>
public interface IBaseMessage
{
}

/// <summary>
/// Receives each message a client sends. Calls for one connection are sequential and in arrival order; different
/// connections call concurrently.
/// </summary>
public interface IServerProtoMessageParser<in T>
    where T : IBaseMessage
{
    Task ReceivedMsgAsync(string connectionId, T message);
}

/// <summary>
/// How a connection tells its server that it has closed.
/// </summary>
public interface INeutronAsyncServerCallBack<T>
    where T : IBaseMessage
{
    void RemoveClientConnection(string id);
}

public interface INeutronClientConnection<T> : IDisposable
    where T : IBaseMessage
{
    event EventHandler<INeutronClientConnection<T>>? OnClientDisconnection;

    string Id { get; }

    bool IsConnected { get; }

    void Start();

    /// <summary>
    /// Queues <paramref name="message"/> for sending and returns immediately. False if the connection is closed or its
    /// send queue is full (the client is then disconnected as too slow). Throws if the message cannot be serialized.
    /// </summary>
    bool SendMessage(T message);

    bool Disconnect();
}

public interface INeutronAsyncServer<T> : INeutronAsyncServerCallBack<T>, IDisposable
    where T : IBaseMessage
{
    event EventHandler<INeutronClientConnection<T>>? OnClientConnection;

    event EventHandler<INeutronClientConnection<T>>? OnClientDisconnection;

    event EventHandler<Exception>? OnServerError;

    IPEndPoint ListenerAddress { get; }

    IDictionary<string, INeutronClientConnection<T>> ClientConnections { get; }

    int ConnectedClientCount { get; }

    bool IsBound { get; }

    void Start();

    void Stop();

    bool Disconnect(INeutronClientConnection<T> connection);

    /// <summary>
    /// Sends to one client. False if the id is unknown or the message was not queued.
    /// </summary>
    bool SendMessage(string id, T message);

    /// <summary>
    /// Sends to every client. The message is serialized once. False if any client did not take it.
    /// </summary>
    bool SendMessage(T message);
}

public sealed class NeutronServerConfig<T>
    where T : IBaseMessage
{
    public required IPEndPoint ListenerAddress { get; init; }

    /// <summary>
    /// Sent to every client each <see cref="HeartBeatIntervalInSec"/>; null or a zero interval disables heartbeats.
    /// </summary>
    public T? HeartBeatMsg { get; init; }

    public int HeartBeatIntervalInSec { get; init; }

    /// <summary>
    /// When true, a failure in <see cref="INeutronAsyncServer{T}.Start"/> is logged and raised on OnServerError
    /// instead of thrown.
    /// </summary>
    public bool HandleStartException { get; init; }

    /// <summary>
    /// Messages a client may have waiting to be sent. A client that falls this far behind is disconnected, so it can
    /// never hold up the sender or the other clients.
    /// </summary>
    public int SendQueueCapacity { get; init; } = 10_000;

    /// <summary>
    /// Largest message accepted from a client. A larger length prefix closes the connection, which protects the
    /// server from corrupt or hostile input.
    /// </summary>
    public int MaxMessageBytes { get; init; } = 16 * 1024 * 1024;
}
