using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;

namespace Dcs.DataTransport.Internal;

/// <summary>
/// Server-side state for one subscriber. The publisher only ever does a non-blocking <see cref="Enqueue"/>; a
/// transport-specific writer loop drains <see cref="Reader"/> to the network. Because every subscriber has its own
/// bounded queue and its own writer, a slow subscriber can fill only its own queue.
/// </summary>
internal sealed class Subscriber<T>
{
    private readonly Channel<T> _queue;
    private readonly bool _disconnectWhenFull;
    private readonly CancellationTokenSource _disconnected = new();
    private string _disconnectReason;
    private long _sent;
    private long _dropped;
    private long _batches;
    private long _sendStartedAt;
    private volatile bool _isConnected = true;

    public Subscriber(long id, string name, TransportKind transport, string remoteEndPoint, DataServerOptions options)
    {
        Id = id;
        Name = name;
        Transport = transport;
        RemoteEndPoint = remoteEndPoint;
        _disconnectWhenFull = options.SlowSubscriberPolicy == SlowSubscriberPolicy.Disconnect;

        var channelOptions = new BoundedChannelOptions(options.SubscriberQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            // The writer loop must never run inline on the publisher's thread, or one subscriber's socket write
            // would delay delivery to the next.
            AllowSynchronousContinuations = false,
            FullMode = options.SlowSubscriberPolicy switch
            {
                SlowSubscriberPolicy.DropOldest => BoundedChannelFullMode.DropOldest,
                SlowSubscriberPolicy.DropNewest => BoundedChannelFullMode.DropWrite,
                _ => BoundedChannelFullMode.Wait
            }
        };

        _queue = Channel.CreateBounded<T>(channelOptions, _ => Interlocked.Increment(ref _dropped));
    }

    public long Id { get; }

    public string Name { get; }

    public TransportKind Transport { get; }

    public string RemoteEndPoint { get; }

    public ChannelReader<T> Reader => _queue.Reader;

    public CancellationToken DisconnectToken => _disconnected.Token;

    public string DisconnectReason => Volatile.Read(ref _disconnectReason);

    /// <summary>
    /// Called on the publishing thread. Never blocks: a full queue is handled by the configured policy.
    /// </summary>
    public void Enqueue(in T message)
    {
        if (!_queue.Writer.TryWrite(message) && _disconnectWhenFull)
        {
            Disconnect("queue full: subscriber too slow");
        }
    }

    public void BeginSend()
    {
        Volatile.Write(ref _sendStartedAt, Stopwatch.GetTimestamp());
    }

    public void EndSend(int messageCount)
    {
        Volatile.Write(ref _sendStartedAt, 0);
        Volatile.Write(ref _sent, _sent + messageCount);
        Volatile.Write(ref _batches, _batches + 1);
    }

    public void DisconnectIfStalled(long now, long stallTicks)
    {
        var startedAt = Volatile.Read(ref _sendStartedAt);
        if (startedAt != 0 && now - startedAt > stallTicks)
        {
            Disconnect("send stalled: peer stopped reading");
        }
    }

    /// <summary>
    /// Graceful end: no more messages are accepted, and the writer loop sends what is queued and then ends the stream.
    /// </summary>
    public void Complete()
    {
        _queue.Writer.TryComplete();
    }

    /// <summary>
    /// Abandons the subscriber: anything still queued is discarded and the transport closes the connection.
    /// </summary>
    public void Disconnect(string reason)
    {
        if (Interlocked.CompareExchange(ref _disconnectReason, reason, null) != null)
        {
            return;
        }

        _queue.Writer.TryComplete();

        // Callbacks (socket teardown) run on the thread pool, not on the caller, which may be the publisher.
        _ = _disconnected.CancelAsync();
    }

    /// <summary>
    /// Records that the transport has closed the connection. The first reason recorded wins.
    /// </summary>
    public void MarkClosed(string reason)
    {
        Interlocked.CompareExchange(ref _disconnectReason, reason, null);
        _isConnected = false;
    }

    public SubscriberStats GetStats()
    {
        var isConnected = _isConnected;
        return new SubscriberStats(
            Id,
            Name,
            Transport,
            RemoteEndPoint,
            isConnected,
            Volatile.Read(ref _sent),
            Volatile.Read(ref _dropped),
            Volatile.Read(ref _batches),
            isConnected ? _queue.Reader.Count : 0,
            DisconnectReason);
    }
}
