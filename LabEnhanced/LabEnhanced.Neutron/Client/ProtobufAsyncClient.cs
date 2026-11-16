using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace LabEnhanced.Neutron.Client;

/// <summary>
/// Connects to the first of <see cref="ProtobuffClientConfig{T}.IPEndPoints"/> that accepts. Received messages go to
/// the <see cref="IClientProtoAsyncParser{T}"/>, one at a time and in order, except replies to
/// <see cref="SendMessageAsync(T)"/>, which complete that call. The socket work is in <see cref="Wire"/>.
/// </summary>
public sealed class ProtobufAsyncClient<T> : IProtobufClient<T> where T : IBaseMessage
{
    private readonly IClientProtoAsyncParser<T> _messageParser;
    private readonly ILogger _log;
    private readonly string _connectionName;
    private readonly Subject<Unit> _connected = new();
    private readonly Subject<Unit> _disconnected = new();
    private readonly ConcurrentDictionary<int, TaskCompletionSource<T>> _pending = new();
    private volatile Wire? _wire;
    private int _asyncSendId;

    public ProtobufAsyncClient(IClientProtoAsyncParser<T> messageParser, ProtobuffClientConfig<T> config, ILogger log)
    {
        _messageParser = messageParser;
        Config = config;
        _log = log;
        _connectionName = $"ProtobuffSingleConnection_{config.Name}";
        Connected = _connected.AsObservable();
        Disconnected = _disconnected.AsObservable();
    }

    /// <summary>
    /// Each time a connection is made, before any of its messages are read.
    /// </summary>
    public IObservable<Unit> Connected { get; }

    /// <summary>
    /// Once for each connection, when it has ended: by <see cref="Disconnect"/>, by the server, or by a failure.
    /// </summary>
    public IObservable<Unit> Disconnected { get; }

    /// <summary>
    /// Why the last <see cref="Connect"/> or <see cref="ConnectAsync"/> failed; null once it has succeeded.
    /// </summary>
    public Exception? ConnectionException { get; set; }

    public IPEndPoint? ConnectedEndpoint => _wire is { IsClosed: false } wire ? wire.RemoteEndPoint : null;

    public bool IsConnected => _wire is { IsClosed: false };

    public ProtobuffClientConfig<T> Config { get; }

    public void Connect() => ConnectAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Does not throw when no server accepts: check <see cref="IsConnected"/> and <see cref="ConnectionException"/>.
    /// </summary>
    public async Task ConnectAsync()
    {
        ConnectionException = null;
        if (IsConnected)
        {
            return;
        }

        foreach (var endPoint in Config.IPEndPoints)
        {
            var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await socket.ConnectAsync(endPoint).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                socket.Dispose();
                ConnectionException = exception;
                _log.LogError(exception, "{Connection} failed to connect to {EndPoint}", _connectionName, endPoint);
                continue;
            }

            // We only want the first good connection.
            ConnectionException = null;
            var wire = _wire = new Wire(socket, Config.MaxMessageBytes, Config.MaxSendBacklogBytes);
            try
            {
                _connected.OnNext(Unit.Default);
            }
            finally
            {
                // Started even if a subscriber threw, so the connection is always cleaned up when it ends.
                _ = RunAsync(wire);
            }

            return;
        }
    }

    public void Disconnect() => _wire?.Close();

    /// <summary>
    /// Queues <paramref name="message"/> and returns immediately. False if it could not be queued: there is no
    /// connection, the server is too far behind, or the message could not be serialized.
    /// </summary>
    public bool SendMessage(T message)
    {
        try
        {
            if (_wire?.Send(Frame.Encode(message)) == true)
            {
                return true;
            }

            _log.LogWarning("{Connection} connection is not connected", _connectionName);
        }
        catch (Exception exception)
        {
            _log.LogError(exception, "{Connection} unable to send data {Message}", _connectionName, message);
        }

        return false;
    }

    public Task<T> SendMessageAsync(T message) => SendMessageAsync(message, continueOnCapturedContext: true);

    /// <summary>
    /// Sends <paramref name="message"/>, stamped with a new <see cref="IBaseMessage.AsyncId"/>, and returns the message
    /// the server sends back with that id. Throws <see cref="IOException"/> if there is no connection. The task is
    /// cancelled if no reply arrives within <see cref="ProtobuffClientConfig{T}.AsyncTimeout"/> milliseconds, or the
    /// connection ends first.
    /// </summary>
    public async Task<T> SendMessageAsync(T message, bool continueOnCapturedContext)
    {
        // The continuation must not run on the receive loop, which completes this.
        var reply = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        message.AsyncSend = true;
        message.AsyncId = Interlocked.Increment(ref _asyncSendId);
        _pending[message.AsyncId] = reply;
        try
        {
            if (_wire?.Send(Frame.Encode(message)) != true)
            {
                throw new IOException($"Failed to send message because protobuff client {_connectionName} is not connected");
            }

            using var timeout = new CancellationTokenSource(Config.AsyncTimeout);
            using var expiry = timeout.Token.UnsafeRegister(static (state, token) => ((TaskCompletionSource<T>)state!).TrySetCanceled(token), reply);
            return await reply.Task.ConfigureAwait(continueOnCapturedContext);
        }
        finally
        {
            _pending.TryRemove(message.AsyncId, out _);
        }
    }

    public void Dispose() => Disconnect();

    private async Task RunAsync(Wire wire)
    {
        _log.LogInformation("{Connection} connected to {EndPoint}", _connectionName, wire.RemoteEndPoint);
        using (StartHeartBeat())
        {
            if (await wire.RunAsync<T>(Route).ConfigureAwait(false) is { } error)
            {
                _log.LogError(error, "{Connection} lost its connection", _connectionName);
            }
        }

        foreach (var (_, reply) in _pending)
        {
            reply.TrySetCanceled();
        }

        _log.LogInformation("{Connection} disconnected", _connectionName);
        _disconnected.OnNext(Unit.Default);
    }

    private Task Route(T message)
    {
        if (!message.AsyncSend)
        {
            return _messageParser.ReceivedMsgAsync(message);
        }

        if (_pending.TryGetValue(message.AsyncId, out var reply))
        {
            reply.TrySetResult(message);
        }

        return Task.CompletedTask;
    }

    private IDisposable StartHeartBeat()
    {
        return Config.HeartBeatMsg is { } heartBeat && Config.HeartBeatInterval > 0
            ? Observable.Interval(TimeSpan.FromSeconds(Config.HeartBeatInterval)).Subscribe(_ => SendMessage(heartBeat))
            : Disposable.Empty;
    }
}
