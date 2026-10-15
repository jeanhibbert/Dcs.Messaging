using Dcs.DataTransport.Grpc;
using Dcs.DataTransport.Internal;
using Dcs.DataTransport.Tcp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.DataTransport;

/// <summary>
/// Publishes <typeparamref name="T"/> messages to every connected <see cref="DataClient{T}"/> over TCP or gRPC.
/// <para>
/// Each subscriber has its own bounded queue and its own asynchronous writer loop. <see cref="Publish"/> only enqueues,
/// so it never waits on the network, and a slow subscriber fills only its own queue, after which
/// <see cref="DataServerOptions.SlowSubscriberPolicy"/> decides what to discard. The writer loops send whatever has
/// queued up in one socket write (or one gRPC message), which gives high throughput under load without adding latency
/// when lightly loaded.
/// </para>
/// </summary>
public sealed class DataServer<T> : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly List<Subscriber<T>> _everySubscriber = new();
    private readonly DataServerOptions _options;
    private readonly IServerTransport _transport;
    private readonly Timer _stallWatchdog;
    private Subscriber<T>[] _subscribers = Array.Empty<Subscriber<T>>();
    private long _published;
    private long _nextSubscriberId;
    private bool _disposed;

    public DataServer(IMessageCodec<T> codec, DataServerOptions options = null)
    {
        ArgumentNullException.ThrowIfNull(codec);
        _options = options ?? new DataServerOptions();

        if (_options.SubscriberQueueCapacity < 1)
        {
            throw new ArgumentException("SubscriberQueueCapacity must be at least 1.", nameof(options));
        }

        if (_options.MaxBatchBytes < Framing.MaxFrameLength(codec))
        {
            throw new ArgumentException($"MaxBatchBytes must be at least {Framing.MaxFrameLength(codec)} (one frame).", nameof(options));
        }

        _transport = _options.Transport switch
        {
            TransportKind.Tcp => new TcpServerTransport<T>(this, codec, _options),
            TransportKind.Grpc => new GrpcServerTransport<T>(this, codec, _options),
            _ => throw new ArgumentOutOfRangeException(nameof(options), _options.Transport, "Unknown transport.")
        };

        _stallWatchdog = new Timer(_ => DisconnectStalledSubscribers(), null, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>
    /// Raised on a transport thread when a subscriber connects.
    /// </summary>
    public event Action<SubscriberStats> SubscriberConnected;

    /// <summary>
    /// Raised on a transport thread when a subscriber's connection closes, with its final counters.
    /// </summary>
    public event Action<SubscriberStats> SubscriberDisconnected;

    public long Published => Interlocked.Read(ref _published);

    public int SubscriberCount => Volatile.Read(ref _subscribers).Length;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);

        var period = TimeSpan.FromTicks(Math.Clamp(_options.SendStallTimeout.Ticks / 4, TimeSpan.TicksPerMillisecond * 100, TimeSpan.TicksPerSecond));
        _stallWatchdog.Change(period, period);
    }

    /// <summary>
    /// Queues <paramref name="message"/> for every connected subscriber. Never blocks and does no I/O. Thread-safe, but
    /// messages published concurrently from different threads have no defined order relative to each other.
    /// </summary>
    public void Publish(in T message)
    {
        var subscribers = Volatile.Read(ref _subscribers);
        for (var i = 0; i < subscribers.Length; i++)
        {
            subscribers[i].Enqueue(in message);
        }

        Interlocked.Increment(ref _published);
    }

    /// <summary>
    /// Counters for every subscriber that has connected, including those since disconnected.
    /// </summary>
    public IReadOnlyList<SubscriberStats> GetSubscriberStats()
    {
        lock (_gate)
        {
            return _everySubscriber.Select(subscriber => subscriber.GetStats()).ToArray();
        }
    }

    /// <summary>
    /// Stops accepting subscribers, lets each one drain its queue for up to
    /// <see cref="DataServerOptions.ShutdownDrainTimeout"/>, then closes every connection.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        Subscriber<T>[] subscribers;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            subscribers = _subscribers;
        }

        await _stallWatchdog.DisposeAsync().ConfigureAwait(false);

        foreach (var subscriber in subscribers)
        {
            subscriber.Complete();
        }

        await _transport.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns <c>null</c> when the server is shutting down.
    /// </summary>
    internal Subscriber<T> AddSubscriber(string name, TransportKind transport, string remoteEndPoint)
    {
        Subscriber<T> subscriber;
        lock (_gate)
        {
            if (_disposed)
            {
                return null;
            }

            subscriber = new Subscriber<T>(++_nextSubscriberId, name, transport, remoteEndPoint, _options);
            _everySubscriber.Add(subscriber);

            // Copy-on-write so Publish can iterate a plain array with no lock and no allocation.
            _subscribers = [.. _subscribers, subscriber];
        }

        SubscriberConnected?.Invoke(subscriber.GetStats());
        return subscriber;
    }

    internal void RemoveSubscriber(Subscriber<T> subscriber, string reason)
    {
        subscriber.MarkClosed(reason);
        lock (_gate)
        {
            _subscribers = Array.FindAll(_subscribers, existing => existing != subscriber);
        }

        SubscriberDisconnected?.Invoke(subscriber.GetStats());
    }

    private void DisconnectStalledSubscribers()
    {
        var now = Stopwatch.GetTimestamp();
        var stallTicks = (long)(_options.SendStallTimeout.TotalSeconds * Stopwatch.Frequency);
        foreach (var subscriber in Volatile.Read(ref _subscribers))
        {
            subscriber.DisconnectIfStalled(now, stallTicks);
        }
    }
}
