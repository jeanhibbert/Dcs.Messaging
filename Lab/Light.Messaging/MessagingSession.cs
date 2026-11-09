using Light.Messaging.Internal;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging;

/// <summary>
/// What <see cref="MessagingServer"/> and <see cref="MessagingClient"/> share: subscriptions, dispatch of received
/// messages, and the watchdog that closes connections whose writes have stalled and reports slow consumers.
/// </summary>
public abstract class MessagingSession : IAsyncDisposable
{
    private readonly object _subscriptionGate = new();
    private readonly Timer _watchdog;
    private readonly TimeSpan _sendStallTimeout;
    private readonly TimeSpan _watchdogPeriod;

    // Copy-on-write: received messages are dispatched with one lock-free dictionary lookup.
    private Dictionary<int, Subscription[]> _subscriptions = new();

    private protected MessagingSession(MessagingOptions options, string defaultName)
    {
        ConnectionSettings = options.ToConnectionSettings(defaultName);
        Serializer = options.Serializer;
        Logger = options.Logger;
        ShutdownDrainTimeout = options.ShutdownDrainTimeout;
        _sendStallTimeout = options.SendStallTimeout;
        _watchdogPeriod = TimeSpan.FromTicks(Math.Clamp(_sendStallTimeout.Ticks / 4, TimeSpan.TicksPerMillisecond * 100, TimeSpan.TicksPerSecond));
        _watchdog = new Timer(static state => ((MessagingSession)state).CheckConnections(), this, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>
    /// Raised when a subscriber or responder throws (and, for a responder, the client has been sent the error).
    /// The connection stays open.
    /// </summary>
    public event Action<Exception> HandlerError;

    public string Name => ConnectionSettings.LocalName;

    public IMessageSerializer Serializer { get; }

    internal ConnectionSettings ConnectionSettings { get; }

    private protected ILogger Logger { get; }

    private protected TimeSpan ShutdownDrainTimeout { get; }

    public IDisposable Subscribe<T>(Endpoint endpoint, Action<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Subscribe<T>(endpoint, (message, _) => handler(message));
    }

    /// <summary>
    /// Calls <paramref name="handler"/> for each message received on <paramref name="endpoint"/>. Handlers run on the
    /// connection's receive loop: messages from one peer arrive in order, one at a time, but a server may run
    /// handlers for different clients concurrently. Keep handlers short, since a slow handler delays every later
    /// message from that peer. Dispose the result to unsubscribe.
    /// </summary>
    public IDisposable Subscribe<T>(Endpoint endpoint, Action<T, MessageContext> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Serializer.Prepare<T>();

        var subscription = new Subscription<T>(endpoint, handler, Serializer);
        lock (_subscriptionGate)
        {
            _subscriptions.TryGetValue(endpoint.Id, out var existing);
            existing ??= Array.Empty<Subscription>();
            Volatile.Write(ref _subscriptions, new Dictionary<int, Subscription[]>(_subscriptions) { [endpoint.Id] = [.. existing, subscription] });

            if (existing.Length == 0)
            {
                OnFirstSubscription(endpoint);
            }
        }

        return new DisposableAction(() => Unsubscribe(subscription));
    }

    public abstract ValueTask DisposeAsync();

    /// <summary>
    /// Serializes <paramref name="message"/> once into a pooled frame holding one reference for the caller.
    /// </summary>
    internal Frame CreateFrame<T>(FrameKind kind, int endpointId, long correlationId, T message)
    {
        var frame = Frame.Rent(kind, endpointId, correlationId);
        try
        {
            Serializer.Serialize(message, frame);
            frame.Seal(ConnectionSettings.MaxFrameBytes);
            return frame;
        }
        catch
        {
            frame.Release();
            throw;
        }
    }

    internal void ReportHandlerError(Exception exception)
    {
        Logger.LogError(exception, "A message handler threw; the connection stays open");
        try
        {
            HandlerError?.Invoke(exception);
        }
        catch (Exception)
        {
        }
    }

    /// <summary>
    /// Logs messages dropped for <paramref name="connection"/> since the last report. Called by the watchdog and when
    /// a connection closes, so drops just before a disconnect are not missed.
    /// </summary>
    private protected void ReportDrops(Connection connection)
    {
        var dropped = connection.TakeUnreportedDrops();
        if (dropped == 0)
        {
            return;
        }

        Logger.LogWarning(
            "Slow consumer {Peer}: dropped {Dropped} message(s) in the last {Seconds:0.#} s ({Total} in total), most recently on endpoint {Endpoint}. "
            + "Its send queue of {Capacity} messages is full: the consumer or the network is not keeping up",
            connection.Peer,
            dropped,
            _watchdogPeriod.TotalSeconds,
            connection.DroppedFrames,
            Endpoint.Describe(connection.LastDroppedEndpointId),
            ConnectionSettings.QueueCapacity);
    }

    /// <summary>
    /// Runs <paramref name="swap"/> and then <paramref name="resubscribe"/> for every subscribed endpoint, all under
    /// the subscription lock, so no Subscribe or Unsubscribe can slip in between. Returns the number of endpoints.
    /// </summary>
    private protected int SwapUnderSubscriptionLock(Action swap, Action<int> resubscribe)
    {
        lock (_subscriptionGate)
        {
            swap();
            foreach (var endpointId in _subscriptions.Keys)
            {
                resubscribe(endpointId);
            }

            return _subscriptions.Count;
        }
    }

    /// <summary>
    /// Called under the subscription lock, so subscribe and unsubscribe notifications stay in order.
    /// </summary>
    private protected virtual void OnFirstSubscription(Endpoint endpoint)
    {
    }

    private protected virtual void OnLastUnsubscription(Endpoint endpoint)
    {
    }

    private protected void DispatchPublish(Connection connection, int endpointId, ReadOnlySpan<byte> payload)
    {
        if (!Volatile.Read(ref _subscriptions).TryGetValue(endpointId, out var subscriptions))
        {
            return;
        }

        var context = new MessageContext(connection.Peer);
        foreach (var subscription in subscriptions)
        {
            try
            {
                subscription.Dispatch(payload, context);
            }
            catch (Exception exception)
            {
                ReportHandlerError(exception);
            }
        }
    }

    private protected abstract IEnumerable<Connection> ActiveConnections();

    private protected void StartWatchdog()
    {
        _watchdog.Change(_watchdogPeriod, _watchdogPeriod);
    }

    private protected ValueTask StopWatchdogAsync() => _watchdog.DisposeAsync();

    private void Unsubscribe(Subscription subscription)
    {
        lock (_subscriptionGate)
        {
            if (!_subscriptions.TryGetValue(subscription.Endpoint.Id, out var existing))
            {
                return;
            }

            var remaining = Array.FindAll(existing, candidate => candidate != subscription);
            var updated = new Dictionary<int, Subscription[]>(_subscriptions);
            if (remaining.Length == 0)
            {
                updated.Remove(subscription.Endpoint.Id);
            }
            else
            {
                updated[subscription.Endpoint.Id] = remaining;
            }

            Volatile.Write(ref _subscriptions, updated);
            if (remaining.Length == 0)
            {
                OnLastUnsubscription(subscription.Endpoint);
            }
        }
    }

    private void CheckConnections()
    {
        var now = Stopwatch.GetTimestamp();
        var stallTicks = (long)(_sendStallTimeout.TotalSeconds * Stopwatch.Frequency);
        foreach (var connection in ActiveConnections())
        {
            connection.CloseIfStalled(now, stallTicks);
            ReportDrops(connection);
        }
    }
}
