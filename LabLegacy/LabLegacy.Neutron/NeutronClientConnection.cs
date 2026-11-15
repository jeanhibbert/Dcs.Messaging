using Microsoft.Extensions.Logging;
using System.Net.Sockets;

namespace LabLegacy.Neutron;

/// <summary>
/// One connected client, as the server sees it. Compared with the legacy class:
/// <list type="bullet">
/// <item>Reading is asynchronous and no thread is blocked waiting for data.</item>
/// <item>Sending only queues. A single write loop owns the socket, so concurrent senders (and the heartbeat) can no
/// longer interleave bytes and corrupt the stream.</item>
/// <item>The queue is bounded. A client that cannot keep up is disconnected instead of blocking the sender.</item>
/// </list>
/// The socket work itself is in <see cref="WireConnection"/>.
/// </summary>
public sealed class NeutronClientConnection<T> : INeutronClientConnection<T>
    where T : IBaseMessage
{
    private readonly INeutronAsyncServerCallBack<T> _callBack;
    private readonly IServerProtoMessageParser<T> _messageParser;
    private readonly ILogger _log;
    private readonly WireConnection _wire;

    public NeutronClientConnection(
        INeutronAsyncServerCallBack<T> callBack,
        string id,
        Socket socket,
        IServerProtoMessageParser<T> messageParser,
        NeutronServerConfig<T> config,
        ILogger log)
    {
        _callBack = callBack;
        Id = id;
        _messageParser = messageParser;
        _log = log;
        _wire = new WireConnection(socket, config.SendQueueCapacity, config.MaxMessageBytes);
    }

    public event EventHandler<INeutronClientConnection<T>>? OnClientDisconnection;

    public string Id { get; }

    public bool IsConnected => _wire.IsConnected;

    public void Start()
    {
        _ = RunAsync();
    }

    public bool SendMessage(T message)
    {
        return IsConnected && Send(EncodedMessage.Create(message));
    }

    public bool Disconnect()
    {
        _wire.Close();
        return true;
    }

    public void Dispose()
    {
        _wire.Close();
    }

    /// <summary>
    /// Queues an already encoded message and takes ownership of one of its references. Never blocks.
    /// </summary>
    internal bool Send(EncodedMessage message)
    {
        if (_wire.TrySend(message))
        {
            return true;
        }

        if (!_wire.IsClosed)
        {
            _log.LogWarning("connection_id:{Id} send queue is full; disconnecting the slow client", Id);
            _wire.Close();
        }

        return false;
    }

    private async Task RunAsync()
    {
        var error = await _wire
            .RunAsync<T>(message => new ValueTask(_messageParser.ReceivedMsgAsync(Id, message)))
            .ConfigureAwait(false);
        if (error != null)
        {
            _log.LogError(error, "Error on connection_id:{Id}", Id);
        }

        _callBack.RemoveClientConnection(Id);
        _log.LogInformation("connection_id:{Id} has stopped", Id);
        OnClientDisconnection?.Invoke(this, this);
    }
}
