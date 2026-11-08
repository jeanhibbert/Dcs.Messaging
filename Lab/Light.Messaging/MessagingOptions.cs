using Light.Messaging.Internal;
using System;

namespace Light.Messaging;

public abstract class MessagingOptions
{
    /// <summary>
    /// Announced to the other side in the handshake; shows up as <see cref="Peer.Name"/> there.
    /// </summary>
    public string Name { get; set; }

    public IMessageSerializer Serializer { get; set; } = ProtobufNetSerializer.Instance;

    /// <summary>
    /// Frames each connection may have waiting to be sent before <see cref="SlowConsumerPolicy"/> applies.
    /// </summary>
    public int SendQueueCapacity { get; set; } = 65_536;

    public SlowConsumerPolicy SlowConsumerPolicy { get; set; } = SlowConsumerPolicy.Disconnect;

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
    /// Covers connecting and the protocol handshake.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
