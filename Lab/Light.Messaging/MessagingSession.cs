using Light.Messaging.Internal;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging;

/// <summary>
/// What <see cref="MessagingServer"/> and <see cref="MessagingClient"/> share: subscriptions, dispatch of received
/// messages, the observable streams the session reports through, and the watchdog.
/// <para>
/// There are two ways to receive. <see cref="Subscribe{T}(Endpoint, Action{T, MessageContext})"/> takes a delegate and
/// is the hot path: no allocation and no lock per message. <see cref="Observe{T}"/> returns an
/// <see cref="IObservable{T}"/> for composing with Rx operators (Where, Sample, Buffer, Throttle...). Both deliver the
/// same messages in the same order; an Rx subscription is a delegate subscription underneath.
/// </para>
/// </summary>
public abstract class MessagingSession : IAsyncDisposable
{
    private readonly object _subscriptionGate = new();
    private readonly ISubject<Exception> _handlerErrors = Subject.Synchronize(new Subject<Exception>());
    private readonly ISubject<SlowConsumerReport> _slowConsumers = Subject.Synchronize(new Subject<SlowConsumerReport>());
    private readonly AsyncSubject<Unit> _stopped = new();
    private readonly CompositeDisposable _lifetime = new();
    private readonly TimeSpan _sendStallTimeout;
    private readonly TimeSpan _watchdogPeriod;

    // Copy-on-write: received messages are dispatched with one lock-free dictionary lookup.
    private Dictionary<int, Subscription[]> _subscriptions = new();

    private protected MessagingSession(MessagingOptions options, string defaultName)
    {
        ConnectionSettings = options.ToConnectionSettings(defaultName);
        Serializer = options.Serializer;
        Logger = options.Logger;
        Scheduler = options.Scheduler;
        ShutdownDrainTimeout = options.ShutdownDrainTimeout;
        _sendStallTimeout = options.SendStallTimeout;
        _watchdogPeriod = TimeSpan.FromTicks(Math.Clamp(_sendStallTimeout.Ticks / 4, TimeSpan.TicksPerMillisecond * 100, TimeSpan.TicksPerSecond));

        // The library's own logging is just another subscriber to the streams it exposes.
        DisposeWithSession(_handlerErrors.Subscribe(exception => Logger.LogError(exception, "A message handler threw; the connection stays open")));
        DisposeWithSession(_slowConsumers.Subscribe(LogSlowConsumer));
    }

    public string Name => ConnectionSettings.LocalName;

    public IMessageSerializer Serializer { get; }

    /// <summary>
    /// Exceptions thrown by subscribers and responders (for a responder, the client has also been sent the error).
    /// The connection stays open.
    /// </summary>
    public IObservable<Exception> HandlerErrors => _handlerErrors.AsObservable();

    /// <summary>
    /// Messages dropped because a peer could not keep up (<see cref="SlowConsumerPolicy.DropOldest"/> and
    /// <see cref="SlowConsumerPolicy.DropNewest"/>), aggregated per peer per watchdog check. Each report is also
    /// logged as a warning.
    /// </summary>
    public IObservable<SlowConsumerReport> SlowConsumers => _slowConsumers.AsObservable();

    internal ConnectionSettings ConnectionSettings { get; }

    private protected ILogger Logger { get; }

    private protected IScheduler Scheduler { get; }

    private protected TimeSpan ShutdownDrainTimeout { get; }

    /// <summary>
    /// True when handlers can be called from several receive loops at once (a server with many clients).
    /// </summary>
    private protected virtual bool DispatchesConcurrently => false;

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

        return Disposable.Create(() => Unsubscribe(subscription));
    }

    /// <summary>
    /// Messages received on <paramref name="endpoint"/> as an observable. Each Rx subscription subscribes, and
    /// disposing it unsubscribes, so <c>Take</c>, <c>FirstAsync</c> or <c>TakeUntil</c> also stop delivery from the
    /// server once nobody is listening. OnNext calls never overlap, as Rx requires, even on a server receiving from
    /// many clients. The stream completes when the session is disposed. A message that fails to deserialize goes to
    /// <see cref="HandlerErrors"/> rather than ending the stream.
    /// </summary>
    public IObservable<T> Observe<T>(Endpoint endpoint)
    {
        return Observe<T, T>(endpoint, static (message, _) => message);
    }

    /// <summary>
    /// As <see cref="Observe{T}"/>, with the peer each message came from.
    /// </summary>
    public IObservable<(T Message, Peer Sender)> ObserveWithSender<T>(Endpoint endpoint)
    {
        return Observe<T, (T, Peer)>(endpoint, static (message, context) => (message, context.Sender));
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
        Notify(_handlerErrors, exception);
    }

    /// <summary>
    /// Raises <paramref name="value"/> on <paramref name="subject"/>. An exception from a user's subscriber is reported
    /// to <see cref="HandlerErrors"/> instead of escaping into a receive loop and closing the connection.
    /// </summary>
    private protected void Notify<T>(IObserver<T> subject, T value)
    {
        try
        {
            subject.OnNext(value);
        }
        catch (Exception exception)
        {
            // A failing HandlerErrors subscriber has nowhere left to report to.
            if (!ReferenceEquals(subject, _handlerErrors))
            {
                ReportHandlerError(exception);
            }
        }
    }

    private protected void DisposeWithSession(IDisposable disposable)
    {
        _lifetime.Add(disposable);
    }

    /// <summary>
    /// Reports messages dropped for <paramref name="connection"/> since the last report. Called by the watchdog and
    /// when a connection closes, so drops just before a disconnect are not missed.
    /// </summary>
    private protected void ReportDrops(Connection connection)
    {
        var dropped = connection.TakeUnreportedDrops();
        if (dropped > 0)
        {
            Notify(_slowConsumers, new SlowConsumerReport(connection.Peer, dropped, connection.DroppedFrames, Endpoint.Describe(connection.LastDroppedEndpointId)));
        }
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
        DisposeWithSession(Observable.Interval(_watchdogPeriod, Scheduler).Subscribe(_ => CheckConnections()));
    }

    /// <summary>
    /// Completes every <see cref="Observe{T}"/> stream and the session's own streams, and stops the timers.
    /// </summary>
    private protected void StopSession()
    {
        _stopped.OnNext(Unit.Default);
        _stopped.OnCompleted();
        _handlerErrors.OnCompleted();
        _slowConsumers.OnCompleted();
        _lifetime.Dispose();
    }

    private IObservable<TResult> Observe<T, TResult>(Endpoint endpoint, Func<T, MessageContext, TResult> project)
    {
        return Observable.Create<TResult>(observer =>
        {
            var target = DispatchesConcurrently ? Observer.Synchronize(observer) : observer;
            return Subscribe<T>(endpoint, (message, context) => target.OnNext(project(message, context)));
        })
        .TakeUntil(_stopped);
    }

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

    private void LogSlowConsumer(SlowConsumerReport report)
    {
        Logger.LogWarning(
            "Slow consumer {Peer}: dropped {Dropped} message(s) since the last report ({Total} in total), most recently on endpoint {Endpoint}. "
            + "Its send queue of {Capacity} messages is full: the consumer or the network is not keeping up",
            report.Peer,
            report.Dropped,
            report.TotalDropped,
            report.LastEndpoint,
            ConnectionSettings.QueueCapacity);
    }
}
