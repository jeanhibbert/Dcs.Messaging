using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace LabLegacy.Neutron;

public enum NeutronClientState
{
    /// <summary>Not started, or disposed.</summary>
    Disconnected,

    /// <summary>Started and trying to reach the server: the first attempt, or a retry after a lost connection.</summary>
    Connecting,

    Connected,
}

public sealed class NeutronClientConfig
{
    /// <summary>
    /// The server to connect to: an <see cref="IPEndPoint"/>, or a <see cref="DnsEndPoint"/> for a host name.
    /// </summary>
    public required EndPoint ServerAddress { get; init; }

    /// <summary>
    /// Wait before the first reconnect attempt. It doubles after each failed attempt, up to
    /// <see cref="MaxReconnectDelay"/>, and resets once a connection is made.
    /// </summary>
    public TimeSpan ReconnectDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);

    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// When above zero, a connection on which nothing has arrived for this long is treated as dead and replaced. Set
    /// it to a few times the server's heartbeat interval. Detection takes between one and two times this value.
    /// Zero (the default) turns the check off.
    /// </summary>
    public TimeSpan HeartBeatTimeout { get; init; }

    /// <summary>
    /// Messages that may be waiting to be sent. If the server falls this far behind, the connection is replaced.
    /// </summary>
    public int SendQueueCapacity { get; init; } = 10_000;

    /// <summary>
    /// Largest message accepted from the server.
    /// </summary>
    public int MaxMessageBytes { get; init; } = 16 * 1024 * 1024;
}

/// <summary>
/// Client for <see cref="NeutronAsyncServer{T}"/>. It keeps one connection to the server and replaces it whenever it
/// is lost, so it survives the server stopping and starting. Everything it reports is an observable:
/// <see cref="Messages"/>, <see cref="State"/> and <see cref="Errors"/>.
/// <para>
/// Nothing is replayed after a reconnect: messages either side sent while the connection was down are lost. Subscribe
/// to <see cref="State"/> and act on <see cref="NeutronClientState.Connected"/> to re-establish whatever the server
/// needs to know.
/// </para>
/// </summary>
public sealed class NeutronClient<T> : IDisposable
    where T : IBaseMessage
{
    private readonly NeutronClientConfig _config;
    private readonly ILogger _log;
    private readonly Subject<T> _messages = new();
    private readonly Subject<Exception> _errors = new();
    private readonly BehaviorSubject<NeutronClientState> _state = new(NeutronClientState.Disconnected);
    private readonly CancellationTokenSource _stop = new();
    private readonly Func<T, ValueTask> _publish;
    private WireConnection? _connection;
    private int _started;
    private int _disposed;

    public NeutronClient(NeutronClientConfig config, ILogger<NeutronClient<T>>? log = null)
    {
        _config = config;
        _log = log ?? NullLogger<NeutronClient<T>>.Instance;
        _publish = message =>
        {
            _messages.OnNext(message);
            return ValueTask.CompletedTask;
        };

        Messages = _messages.AsObservable();
        Errors = _errors.AsObservable();
        State = _state.DistinctUntilChanged();
    }

    /// <summary>
    /// Every message from the server, in the order it was sent. Hot: a subscriber sees messages from the moment it
    /// subscribes. Delivered on the connection's read loop, so a slow subscriber delays reading from the server; use
    /// <c>ObserveOn</c> to move slow work elsewhere. Completes when the client is disposed.
    /// </summary>
    public IObservable<T> Messages { get; }

    /// <summary>
    /// The current state on subscription, then each change. <see cref="NeutronClientState.Connected"/> is published
    /// before any message from that connection, and <see cref="SendMessage"/> already works inside the handler.
    /// Completes when the client is disposed.
    /// </summary>
    public IObservable<NeutronClientState> State { get; }

    /// <summary>
    /// Why a connection attempt failed or a connection was lost: a socket error, a timeout, a full send queue. A
    /// server that closes the connection cleanly shows on <see cref="State"/> only. The client carries on after
    /// every one of these. Completes when the client is disposed.
    /// </summary>
    public IObservable<Exception> Errors { get; }

    public bool IsConnected => _state.Value == NeutronClientState.Connected;

    /// <summary>
    /// Starts connecting, and keeps the connection alive until <see cref="Dispose"/>. Returns immediately.
    /// </summary>
    public void Start()
    {
        if (Interlocked.Exchange(ref _started, 1) == 0)
        {
            _ = Task.Run(() => RunAsync(_stop.Token));
        }
    }

    /// <summary>
    /// Queues <paramref name="message"/> for sending and returns immediately. False if there is no connection, or if
    /// the send queue is full (the connection is then replaced). Throws if the message cannot be serialized.
    /// </summary>
    public bool SendMessage(T message)
    {
        var connection = Volatile.Read(ref _connection);
        if (connection == null || connection.IsClosed)
        {
            return false;
        }

        if (connection.TrySend(EncodedMessage.Create(message)))
        {
            return true;
        }

        connection.Close(new IOException("The send queue is full: the server is not reading fast enough."));
        return false;
    }

    /// <summary>
    /// Closes the connection and stops reconnecting. The observables complete shortly afterwards.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _stop.Cancel();
        if (Interlocked.Exchange(ref _started, 1) == 0)
        {
            Complete();
        }
    }

    private async Task RunAsync(CancellationToken stop)
    {
        try
        {
            var delay = _config.ReconnectDelay;
            while (!stop.IsCancellationRequested)
            {
                _state.OnNext(NeutronClientState.Connecting);

                Exception? error;
                try
                {
                    var socket = await ConnectAsync(stop).ConfigureAwait(false);
                    delay = _config.ReconnectDelay;
                    error = await RunConnectionAsync(socket, stop).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    error = exception;
                }

                if (stop.IsCancellationRequested)
                {
                    break;
                }

                _state.OnNext(NeutronClientState.Connecting);
                if (error == null)
                {
                    _log.LogInformation("{ServerAddress} closed the connection; reconnecting", _config.ServerAddress);
                }
                else
                {
                    _log.LogWarning(error, "No connection to {ServerAddress}; retrying", _config.ServerAddress);
                    _errors.OnNext(error);
                }

                // Jitter, so that the clients of a restarted server do not all return at the same instant.
                await Task.Delay(delay * (0.75 + (Random.Shared.NextDouble() * 0.5)), stop).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, _config.MaxReconnectDelay.Ticks));
            }
        }
        catch (Exception exception)
        {
            // Only a subscriber to State or Errors that throws can get here.
            _log.LogCritical(exception, "NeutronClient for {ServerAddress} stopped by a failing subscriber", _config.ServerAddress);
        }
        finally
        {
            Complete();
        }
    }

    private async Task<Socket> ConnectAsync(CancellationToken stop)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop);
        timeout.CancelAfter(_config.ConnectTimeout);
        try
        {
            await socket.ConnectAsync(_config.ServerAddress, timeout.Token).ConfigureAwait(false);
            return socket;
        }
        catch (Exception exception)
        {
            socket.Dispose();
            if (exception is OperationCanceledException && !stop.IsCancellationRequested)
            {
                throw new TimeoutException($"Connecting to {_config.ServerAddress} took longer than {_config.ConnectTimeout}.");
            }

            throw;
        }
    }

    /// <summary>
    /// Runs one connection until it ends, and returns why it ended (null for a clean close).
    /// </summary>
    private async Task<Exception?> RunConnectionAsync(Socket socket, CancellationToken stop)
    {
        var connection = new WireConnection(socket, _config.SendQueueCapacity, _config.MaxMessageBytes);
        using var stopping = stop.Register(() => connection.Close());
        using var watchdog = WatchForSilence(connection);

        // Published before the read loop starts, so Connected always precedes the connection's first message.
        Volatile.Write(ref _connection, connection);
        _log.LogInformation("Connected to {ServerAddress}", _config.ServerAddress);
        _state.OnNext(NeutronClientState.Connected);
        try
        {
            return await connection.RunAsync(_publish).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _connection, null);
        }
    }

    /// <summary>
    /// Closes the connection when a whole <see cref="NeutronClientConfig.HeartBeatTimeout"/> passes with nothing
    /// received. It compares a counter once per interval, so the receive path pays nothing for it.
    /// </summary>
    private IDisposable WatchForSilence(WireConnection connection)
    {
        var timeout = _config.HeartBeatTimeout;
        if (timeout <= TimeSpan.Zero)
        {
            return Disposable.Empty;
        }

        return Observable.Interval(timeout)
            .Select(_ => connection.ReceiveCount)
            .Scan((Previous: -1L, Current: 0L), (seen, count) => (seen.Current, count))
            .Where(seen => seen.Previous == seen.Current)
            .Subscribe(_ => connection.Close(new TimeoutException($"Nothing received from {_config.ServerAddress} for {timeout}.")));
    }

    private void Complete()
    {
        _state.OnNext(NeutronClientState.Disconnected);
        _state.OnCompleted();
        _messages.OnCompleted();
        _errors.OnCompleted();
    }
}
