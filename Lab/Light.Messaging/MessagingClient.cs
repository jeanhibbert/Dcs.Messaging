using Light.Messaging.Internal;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging;

/// <summary>
/// One connection to a <see cref="MessagingServer"/>: subscribes to what it publishes, publishes to it, and sends it
/// requests.
/// <code>
/// await using var client = await MessagingClient.ConnectAsync(new TcpClientTransport("10.0.0.5", 5070));
/// using var prices = client.Subscribe&lt;PriceTick&gt;(Endpoints.Prices, tick => ...);
/// var price = await client.RequestAsync&lt;PricingRequest, ForwardPrice&gt;(Endpoints.Pricing, request);
/// </code>
/// The client does not reconnect by itself: watch <see cref="Disconnected"/> or <see cref="Completion"/>, then connect
/// a new client and subscribe again.
/// </summary>
public sealed class MessagingClient : MessagingSession, IConnectionOwner
{
    private static readonly TimeSpan TimeoutCheckPeriod = TimeSpan.FromMilliseconds(10);

    private readonly ConcurrentDictionary<long, PendingRequest> _pending = new();
    private readonly TimeSpan _requestTimeout;
    private readonly Timer _timeoutTimer;
    private Connection _connection;
    private Task _run = Task.CompletedTask;
    private long _lastCorrelationId;

    private MessagingClient(MessagingClientOptions options)
        : base(options, "client")
    {
        _requestTimeout = options.RequestTimeout;
        _timeoutTimer = new Timer(static state => ((MessagingClient)state).FailExpiredRequests(), this, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>
    /// Raised once when the connection closes, with the reason.
    /// </summary>
    public event Action<string> Disconnected;

    public Peer Server => _connection.Peer;

    public bool IsConnected => !_connection.IsClosed;

    /// <summary>
    /// Completes when the connection has closed. Never faults.
    /// </summary>
    public Task Completion => _run;

    public static async Task<MessagingClient> ConnectAsync(ClientTransport transport, MessagingClientOptions options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transport);
        options ??= new MessagingClientOptions();

        var client = new MessagingClient(options);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.ConnectTimeout);

        client._connection = await transport.ConnectAsync(client.ConnectionSettings, timeout.Token).ConfigureAwait(false);
        client._run = client._connection.RunAsync(client);
        try
        {
            await client._connection.Handshake.WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch
        {
            client._connection.Close("handshake failed");
            await client._run.ConfigureAwait(false);
            throw;
        }

        client._timeoutTimer.Change(TimeoutCheckPeriod, TimeoutCheckPeriod);
        client.StartWatchdog();
        return client;
    }

    /// <summary>
    /// Sends <paramref name="message"/> to the server's subscribers for <paramref name="endpoint"/>. Never blocks.
    /// </summary>
    /// <exception cref="MessagingException">The connection has closed.</exception>
    public void Publish<T>(Endpoint endpoint, T message)
    {
        ThrowIfClosed();
        _connection.Enqueue(CreateFrame(FrameKind.Publish, endpoint.Id, 0, message));
    }

    /// <summary>
    /// Sends a request and completes with the server's response. Fails with <see cref="RemoteException"/> if the
    /// responder threw, <see cref="TimeoutException"/> after <paramref name="timeout"/> (default
    /// <see cref="MessagingClientOptions.RequestTimeout"/>, checked every 10 ms), or <see cref="MessagingException"/>
    /// if the connection closes. Await the result exactly once.
    /// </summary>
    public ValueTask<TResponse> RequestAsync<TRequest, TResponse>(Endpoint endpoint, TRequest request, TimeSpan? timeout = null)
    {
        ThrowIfClosed();

        var correlationId = Interlocked.Increment(ref _lastCorrelationId);
        var deadline = Stopwatch.GetTimestamp() + (long)((timeout ?? _requestTimeout).TotalSeconds * Stopwatch.Frequency);
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

        if (!_connection.Enqueue(frame))
        {
            FailPending(correlationId, new MessagingException($"Request not sent: {_connection.CloseReason ?? "send queue full"}."));
        }

        return result;
    }

    /// <summary>
    /// Sends what is queued (up to <see cref="MessagingOptions.ShutdownDrainTimeout"/>), then closes the connection.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        await _timeoutTimer.DisposeAsync().ConfigureAwait(false);
        await StopWatchdogAsync().ConfigureAwait(false);

        _connection.Shutdown("client disposed");
        if (await Task.WhenAny(_run, Task.Delay(ShutdownDrainTimeout)).ConfigureAwait(false) != _run)
        {
            _connection.Close("client disposed (drain timed out)");
        }

        await _run.ConfigureAwait(false);
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
        foreach (var correlationId in _pending.Keys)
        {
            FailPending(correlationId, new MessagingException($"Connection closed: {connection.CloseReason}."));
        }

        Disconnected?.Invoke(connection.CloseReason);
    }

    private protected override void OnFirstSubscription(Endpoint endpoint)
    {
        _connection.EnqueueControl(FrameKind.Subscribe, endpoint.Id);
    }

    private protected override void OnLastUnsubscription(Endpoint endpoint)
    {
        _connection.EnqueueControl(FrameKind.Unsubscribe, endpoint.Id);
    }

    private protected override IEnumerable<Connection> ActiveConnections()
    {
        yield return _connection;
    }

    private void ThrowIfClosed()
    {
        if (_connection.IsClosed)
        {
            throw new MessagingException($"Not connected: {_connection.CloseReason}.");
        }
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

        var now = Stopwatch.GetTimestamp();
        foreach (var entry in _pending)
        {
            if (now > entry.Value.Deadline)
            {
                FailPending(entry.Key, new TimeoutException($"No response within the request timeout (correlation id {entry.Key})."));
            }
        }
    }
}
