using Microsoft.Extensions.Logging;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace LabEnhanced.Neutron;

/// <summary>
/// One connected client, as the server sees it. The socket work is in <see cref="Wire"/>.
/// </summary>
public sealed class ProtobuffClientConnection<T> : IProtobuffClientConnection<T> where T : IBaseMessage
{
    private readonly IProtobuffAsyncServerCallBack<T> _callBack;
    private readonly IServerProtoMessageParser<T> _messageParser;
    private readonly ILogger _log;
    private readonly Wire _wire;
    private readonly AsyncSubject<IProtobuffClientConnection<T>> _disconnection = new();

    public ProtobuffClientConnection(
        IProtobuffAsyncServerCallBack<T> callBack,
        string id,
        Socket socket,
        IServerProtoMessageParser<T> messageParser,
        ProtobuffServerConfig<T> config,
        ILogger log)
    {
        _callBack = callBack;
        Id = id;
        _messageParser = messageParser;
        _log = log;
        _wire = new Wire(socket, config.MaxMessageBytes, config.MaxSendBacklogBytes);
        OnClientDisconnection = _disconnection.AsObservable();
    }

    /// <summary>
    /// This connection, once, when it has ended; then completes. A late subscriber still receives it.
    /// </summary>
    public IObservable<IProtobuffClientConnection<T>> OnClientDisconnection { get; }

    public string Id { get; }

    public bool IsConnected => !_wire.IsClosed;

    public void Start() => _ = RunAsync();

    /// <summary>
    /// Queues <paramref name="message"/> and returns immediately. False if the connection is closed, or if the client
    /// is so far behind that it is now being disconnected. Throws if the message cannot be serialized.
    /// </summary>
    public bool SendMessage(T message) => _wire.Send(Frame.Encode(message));

    public bool Disconnect()
    {
        _wire.Close();
        return true;
    }

    public void Dispose() => _wire.Close();

    internal bool Send(ReadOnlySpan<byte> frame) => _wire.Send(frame);

    private async Task RunAsync()
    {
        var error = await _wire.RunAsync<T>(message => _messageParser.ReceivedMsgAsync(Id, message)).ConfigureAwait(false);
        if (error != null)
        {
            _log.LogError(error, "Error on connection_id={Id}", Id);
        }

        _log.LogInformation("connection_id={Id} has stopped", Id);
        _callBack.RemoveClientConnection(Id);
        _disconnection.OnNext(this);
        _disconnection.OnCompleted();
    }
}
