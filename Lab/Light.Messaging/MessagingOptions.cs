using Light.Messaging.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Reactive.Concurrency;

namespace Light.Messaging;

public abstract class MessagingOptions
{
    /// <summary>
    /// Announced to the other side in the handshake; shows up as <see cref="Peer.Name"/> there.
    /// </summary>
    public string Name { get; set; }

    public IMessageSerializer Serializer { get; set; } = ProtobufNetSerializer.Instance;

    /// <summary>
    /// Receives connection events (Information), dropped messages from slow consumers and failed reconnects (Warning),
    /// and handler exceptions (Error). Nothing is logged per message, so logging never touches the hot path.
    /// </summary>
    public ILogger Logger { get; set; } = NullLogger.Instance;

    /// <summary>
    /// Clock and timers for the watchdog, request timeouts and reconnect backoff. Tests can pass Rx's
    /// <c>TestScheduler</c> to drive these in virtual time instead of waiting.
    /// </summary>
    public IScheduler Scheduler { get; set; } = System.Reactive.Concurrency.Scheduler.Default;

    /// <summary>
    /// Published messages each connection may have waiting to be sent before <see cref="SlowConsumerPolicy"/>
    /// applies. Responses and control frames are never dropped and do not count towards it.
    /// </summary>
    public int SendQueueCapacity { get; set; } = 65_536;

    public SlowConsumerPolicy SlowConsumerPolicy { get; set; } = SlowConsumerPolicy.DropOldest;

    /// <summary>
    /// Largest frame (header plus serialized message) that may be sent or received. Larger incoming frames close the
    /// connection, which guards against corrupt or hostile length prefixes.
    /// </summary>
    public int MaxFrameBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// Queued frames are coalesced into writes of up to this size: one socket write (TCP) or one gRPC message.
    /// </summary>
    public int SendBatchBytes { get; set; } = 64 * 1024;

    /// <summary>
    /// A connection whose current write has not completed in this time is treated as dead and closed.
    /// </summary>
    public TimeSpan SendStallTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// On dispose, how long queued frames may take to drain before connections are cut.
    /// </summary>
    public TimeSpan ShutdownDrainTimeout { get; set; } = TimeSpan.FromSeconds(2);

    internal ConnectionSettings ToConnectionSettings(string defaultName)
    {
        if (SendQueueCapacity < 1)
        {
            throw new ArgumentException("SendQueueCapacity must be at least 1.");
        }

        if (SendBatchBytes < 1024)
        {
            throw new ArgumentException("SendBatchBytes must be at least 1024.");
        }

        if (MaxFrameBytes < 1024)
        {
            throw new ArgumentException("MaxFrameBytes must be at least 1024.");
        }

        ArgumentNullException.ThrowIfNull(Serializer);
        ArgumentNullException.ThrowIfNull(Logger);
        ArgumentNullException.ThrowIfNull(Scheduler);
        return new ConnectionSettings(string.IsNullOrEmpty(Name) ? defaultName : Name, SendQueueCapacity, SlowConsumerPolicy, MaxFrameBytes, SendBatchBytes);
    }
}

public sealed class MessagingServerOptions : MessagingOptions
{
}

public sealed class MessagingClientOptions : MessagingOptions
{
    /// <summary>
    /// Used by <see cref="MessagingClient.RequestAsync{TRequest, TResponse}"/> when no timeout is passed.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Covers connecting and the protocol handshake, for the first connection and every reconnect attempt.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// After losing the connection, keep trying to reconnect and restore every subscription. The first
    /// <see cref="MessagingClient.ConnectAsync"/> never retries: a wrong address should fail loudly.
    /// </summary>
    public bool AutoReconnect { get; set; } = true;

    /// <summary>
    /// Wait before the first reconnect attempt; doubles after each failure up to <see cref="ReconnectMaxDelay"/>,
    /// with ±20% jitter so many clients do not reconnect in lockstep after a server restart.
    /// </summary>
    public TimeSpan ReconnectMinDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Attempts before giving up, after which <see cref="MessagingClient.Completion"/> completes. 0 means no limit.
    /// </summary>
    public int MaxReconnectAttempts { get; set; }
}
