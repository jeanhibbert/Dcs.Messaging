using Light.Messaging.Internal;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging;

/// <summary>
/// A connection to a <see cref="MessagingServer"/>: subscribes to what it publishes, publishes to it, and sends it
/// requests.
/// <code>
/// await using var client = await MessagingClient.ConnectAsync(new TcpClientTransport("10.0.0.5", 5070));
/// using var prices = client.Observe&lt;PriceTick&gt;(Endpoints.Prices).Where(t => t.CurrencyPair == "EURUSD").Subscribe(Show);
/// var price = await client.RequestAsync&lt;PricingRequest, ForwardPrice&gt;(Endpoints.Pricing, request);
/// using var refresh = client.Reconnected.Subscribe(_ => RequestSnapshot());
/// </code>
/// <para>
/// If the connection drops, the client reconnects with backoff (<see cref="MessagingClientOptions.AutoReconnect"/>)
/// and restores every subscription. While it is disconnected, Publish and RequestAsync fail immediately rather than
/// queueing: replaying an order minutes later is worse than rejecting it. Messages the server published during the
/// outage are not replayed, so use <see cref="Reconnected"/> to refresh state, for example by requesting a snapshot.
/// </para>
/// </summary>
public sealed class MessagingClient : MessagingSession, IConnectionOwner
{
    private static readonly TimeSpan TimeoutCheckPeriod = TimeSpan.FromMilliseconds(10);

    private readonly ConcurrentDictionary<long, PendingRequest> _pending = new();
    private readonly ClientTransport _transport;
    private readonly MessagingClientOptions _options;
    private readonly CancellationTokenSource _disposing = new();
    private readonly object _statusGate = new();

    // Nobody can subscribe before ConnectAsync returns, so the placeholder value is never observed.
    private readonly BehaviorSubject<ConnectionStatus> _status = new(default);
    private Connection _connection;
    private Task _supervisor = Task.CompletedTask;
    private long _lastCorrelationId;

    private MessagingClient(ClientTransport transport, MessagingClientOptions options)
        : base(options, "client")
    {
        _transport = transport;
        _options = options;
    }

    /// <summary>
    /// The current connection state, then every change: Connected, Reconnecting (with the reason), Connected again,
    /// and finally Closed, after which the stream completes.
    /// </summary>
    public IObservable<ConnectionStatus> Status => _status.AsObservable();

    /// <summary>
    /// The new server peer each time the client reconnects, after its subscriptions have been restored. A good place
    /// to refresh state the outage may have made stale.
    /// </summary>
    public IObservable<Peer> Reconnected => Status.Skip(1).Where(status => status.State == ConnectionState.Connected).Select(status => status.Server);

    /// <summary>
    /// The server as seen by the current connection (a new <see cref="Peer"/> after each reconnect).
    /// </summary>
    public Peer Server => Volatile.Read(ref _connection).Peer;

    /// <summary>
    /// False while the connection is down, including while reconnecting.
    /// </summary>
    public bool IsConnected => !Volatile.Read(ref _connection).IsClosed;

    /// <summary>
    /// Completes when the client stops for good: it was disposed, reconnecting is disabled, or it gave up after
    /// <see cref="MessagingClientOptions.MaxReconnectAttempts"/>. Never faults.
    /// </summary>
    public Task Completion => _supervisor;

    /// <summary>
    /// Connects once, without retrying. Throws if the server cannot be reached or the handshake fails.
    /// </summary>
    public static async Task<MessagingClient> ConnectAsync(ClientTransport transport, MessagingClientOptions options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transport);
        options ??= new MessagingClientOptions();

        var client = new MessagingClient(transport, options);
        var (connection, run) = await client.ConnectOnceAsync(cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref client._connection, connection);
        client.SetConnected(connection);
        client._supervisor = client.SuperviseAsync(run);
        client.DisposeWithSession(Observable.Interval(TimeoutCheckPeriod, client.Scheduler).Subscribe(_ => client.FailExpiredRequests()));
        client.StartWatchdog();
        return client;
    }

    /// <summary>
    /// Sends <paramref name="message"/> to the server's subscribers for <paramref name="endpoint"/>. Never blocks.
    /// </summary>
    /// <exception cref="MessagingException">Not connected (closed, or reconnecting).</exception>
    public void Publish<T>(Endpoint endpoint, T message)
    {
        var connection = ConnectedOrThrow();
        connection.Enqueue(CreateFrame(FrameKind.Publish, endpoint.Id, 0, message));
    }

    /// <summary>
    /// Sends a request and completes with the server's response. Fails with <see cref="RemoteException"/> if the
    /// responder threw, <see cref="TimeoutException"/> after <paramref name="timeout"/> (default
    /// <see cref="MessagingClientOptions.RequestTimeout"/>, checked every 10 ms on the scheduler), or
    /// <see cref="MessagingException"/> if the connection is down or drops before the response arrives. Returns a pooled
    /// ValueTask: await it exactly once.
    /// </summary>
    public ValueTask<TResponse> RequestAsync<TRequest, TResponse>(Endpoint endpoint, TRequest request, TimeSpan? timeout = null)
    {
        var connection = ConnectedOrThrow();

        var correlationId = Interlocked.Increment(ref _lastCorrelationId);
        var deadline = Scheduler.Now.UtcTicks + (timeout ?? _options.RequestTimeout).Ticks;
        var pending = PendingRequest<TResponse>.Rent(Serializer, deadline);
        var result = pending.Task;

        // Registered before sending, so the response cannot arrive first.
        _pending[correlationId] = pending;

        Frame frame;
        try
        {
            frame = CreateFrame(FrameKind.Request, endpoint.Id, correlationId, request);
        }
        catch (Exception exception)
        {
            FailPending(correlationId, exception);
            return result;
        }

        if (!connection.Enqueue(frame))
        {
            FailPending(correlationId, new MessagingException($"Request not sent ({connection.CloseReason ?? "connection closing"})"));
        }

        return result;
    }

    /// <summary>
    /// <see cref="RequestAsync{TRequest, TResponse}"/> as a cold observable: each subscription sends the request once,
    /// so it composes with Rx (<c>Retry</c>, <c>Timeout</c>, <c>SelectMany</c> from a stream of prices...). It
    /// allocates a Task per request; prefer RequestAsync on hot paths.
    /// </summary>
    public IObservable<TResponse> Request<TRequest, TResponse>(Endpoint endpoint, TRequest request, TimeSpan? timeout = null)
    {
        return Observable.FromAsync(() => RequestAsync<TRequest, TResponse>(endpoint, request, timeout).AsTask());
    }

    /// <summary>
    /// Stops reconnecting, sends what is queued (up to <see cref="MessagingOptions.ShutdownDrainTimeout"/>), then
    /// closes and completes every stream.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_disposing.IsCancellationRequested)
        {
            return;
        }

        _disposing.Cancel();
        Volatile.Read(ref _connection).Shutdown("client disposed");
        if (await Task.WhenAny(_supervisor, Task.Delay(ShutdownDrainTimeout)).ConfigureAwait(false) != _supervisor)
        {
            Volatile.Read(ref _connection).Close("client disposed (drain timed out)");
        }

        await _supervisor.ConfigureAwait(false);
        StopSession();
    }

    void IConnectionOwner.OnConnected(Connection connection)
    {
    }

    void IConnectionOwner.OnFrame(Connection connection, FrameKind kind, int endpointId, long correlationId, ReadOnlySpan<byte> payload)
    {
        switch (kind)
        {
            case FrameKind.Publish:
                DispatchPublish(connection, endpointId, payload);
                break;

            case FrameKind.Response:
                if (_pending.TryRemove(correlationId, out var pending))
                {
                    pending.Complete(payload);
                }

                break;

            case FrameKind.ErrorResponse:
                if (_pending.TryRemove(correlationId, out var failed))
                {
                    failed.Fail(new RemoteException(Encoding.UTF8.GetString(payload)));
                }

                break;

            default:
                throw new InvalidDataException($"Unexpected {kind} frame from the server.");
        }
    }

    void IConnectionOwner.OnClosed(Connection connection)
    {
        // Requests in flight cannot be resent safely: the server may already have acted on them.
        foreach (var correlationId in _pending.Keys)
        {
            FailPending(correlationId, new MessagingException($"Connection lost before the response arrived ({connection.CloseReason})"));
        }

        ReportDrops(connection);

        // A connection that closed before it became current (a race in connect or reconnect) changes no state: the
        // supervisor sees its end and simply tries again.
        if (connection != Volatile.Read(ref _connection))
        {
            return;
        }

        var reconnecting = _options.AutoReconnect && !_disposing.IsCancellationRequested;
        if (!_disposing.IsCancellationRequested)
        {
            Logger.LogWarning("Connection to {Server} lost ({Reason}){Reconnecting}", connection.Peer, connection.CloseReason, reconnecting ? "; reconnecting" : string.Empty);
        }

        SetStatus(new ConnectionStatus(reconnecting ? ConnectionState.Reconnecting : ConnectionState.Closed, connection.Peer, connection.CloseReason));
    }

    private protected override void OnFirstSubscription(Endpoint endpoint)
    {
        // While reconnecting this goes to the closed connection and is discarded; the reconnect resubscribes.
        Volatile.Read(ref _connection).EnqueueControl(FrameKind.Subscribe, endpoint.Id);
    }

    private protected override void OnLastUnsubscription(Endpoint endpoint)
    {
        Volatile.Read(ref _connection).EnqueueControl(FrameKind.Unsubscribe, endpoint.Id);
    }

    private protected override IEnumerable<Connection> ActiveConnections()
    {
        yield return Volatile.Read(ref _connection);
    }

    private async Task<(Connection Connection, Task Run)> ConnectOnceAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_options.ConnectTimeout);

        var connection = await _transport.ConnectAsync(ConnectionSettings, timeout.Token).ConfigureAwait(false);
        var run = connection.RunAsync(this);
        try
        {
            await connection.Handshake.WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch
        {
            connection.Close("handshake failed");
            await run.ConfigureAwait(false);
            throw;
        }

        return (connection, run);
    }

    /// <summary>
    /// Runs each connection to its end, then reconnects, until disposed or reconnecting gives up.
    /// </summary>
    private async Task SuperviseAsync(Task run)
    {
        while (run != null)
        {
            await run.ConfigureAwait(false);
            run = _options.AutoReconnect && !_disposing.IsCancellationRequested ? await ReconnectAsync().ConfigureAwait(false) : null;
        }

        var last = _status.Value;
        if (last.State != ConnectionState.Closed)
        {
            SetStatus(last with { State = ConnectionState.Closed, Reason = _disposing.IsCancellationRequested ? "client disposed" : "gave up reconnecting" });
        }

        lock (_statusGate)
        {
            _status.OnCompleted();
        }
    }

    /// <summary>
    /// Returns the new connection's run task, or null if disposed or out of attempts.
    /// </summary>
    private async Task<Task> ReconnectAsync()
    {
        var delay = _options.ReconnectMinDelay;
        for (var attempt = 1; _options.MaxReconnectAttempts == 0 || attempt <= _options.MaxReconnectAttempts; attempt++)
        {
            try
            {
                await Observable.Timer(WithJitter(delay), Scheduler).ToTask(_disposing.Token).ConfigureAwait(false);
                var (connection, run) = await ConnectOnceAsync(_disposing.Token).ConfigureAwait(false);

                var restored = SwapUnderSubscriptionLock(
                    () => Volatile.Write(ref _connection, connection),
                    endpointId => connection.EnqueueControl(FrameKind.Subscribe, endpointId));

                if (_disposing.IsCancellationRequested)
                {
                    connection.Shutdown("client disposed");
                    return run;
                }

                Logger.LogInformation("Reconnected to {Server} after {Attempts} attempt(s); restored {Subscriptions} subscription(s)", connection.Peer, attempt, restored);
                SetConnected(connection);
                return run;
            }
            catch (OperationCanceledException) when (_disposing.IsCancellationRequested)
            {
                return null;
            }
            catch (Exception exception)
            {
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, _options.ReconnectMaxDelay.Ticks));
                Logger.LogWarning("Reconnect attempt {Attempt} failed ({Reason}); retrying in about {Delay} ms", attempt, exception.Message, (int)delay.TotalMilliseconds);
            }
        }

        Logger.LogError("Gave up reconnecting after {Attempts} attempt(s)", _options.MaxReconnectAttempts);
        return null;
    }

    /// <summary>
    /// ±20% so that many clients do not reconnect in lockstep after a server restart.
    /// </summary>
    private static TimeSpan WithJitter(TimeSpan delay)
    {
        return TimeSpan.FromTicks((long)(delay.Ticks * (0.8 + Random.Shared.NextDouble() * 0.4)));
    }

    /// <summary>
    /// The close path and the reconnect task can both report, so changes are serialized as Rx requires.
    /// </summary>
    private void SetStatus(ConnectionStatus status)
    {
        lock (_statusGate)
        {
            Notify(_status, status);
        }
    }

    /// <summary>
    /// Checked under the status lock: if the connection has already closed, its close has reported (or will report)
    /// the newer state, and Connected must not overwrite it.
    /// </summary>
    private void SetConnected(Connection connection)
    {
        lock (_statusGate)
        {
            if (!connection.IsClosed)
            {
                Notify(_status, new ConnectionStatus(ConnectionState.Connected, connection.Peer, null));
            }
        }
    }

    private Connection ConnectedOrThrow()
    {
        var connection = Volatile.Read(ref _connection);
        if (connection.IsClosed)
        {
            var state = _disposing.IsCancellationRequested || !_options.AutoReconnect ? "closed" : "reconnecting";
            throw new MessagingException($"Not connected, {state} ({connection.CloseReason})");
        }

        return connection;
    }

    private void FailPending(long correlationId, Exception exception)
    {
        if (_pending.TryRemove(correlationId, out var pending))
        {
            pending.Fail(exception);
        }
    }

    private void FailExpiredRequests()
    {
        if (_pending.IsEmpty)
        {
            return;
        }

        var now = Scheduler.Now.UtcTicks;
        foreach (var entry in _pending)
        {
            if (now > entry.Value.Deadline)
            {
                FailPending(entry.Key, new TimeoutException($"No response within the request timeout (correlation id {entry.Key})."));
            }
        }
    }
}
