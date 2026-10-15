using System;

namespace Dcs.DataTransport;

public sealed class DataServerOptions
{
    public TransportKind Transport { get; set; } = TransportKind.Tcp;

    /// <summary>
    /// IP address to listen on.
    /// </summary>
    public string Host { get; set; } = "127.0.0.1";

    public int Port { get; set; } = 5050;

    /// <summary>
    /// Messages each subscriber may have queued before <see cref="SlowSubscriberPolicy"/> applies. This bounds memory per
    /// subscriber and also how stale a slow subscriber's data can get (capacity / publish rate).
    /// </summary>
    public int SubscriberQueueCapacity { get; set; } = 65_536;

    public SlowSubscriberPolicy SlowSubscriberPolicy { get; set; } = SlowSubscriberPolicy.DropOldest;

    /// <summary>
    /// Largest socket write (TCP) or gRPC message (gRPC). Messages already queued are coalesced up to this size.
    /// </summary>
    public int MaxBatchBytes { get; set; } = 64 * 1024;

    /// <summary>
    /// A subscriber whose current send has not completed within this time is treated as dead and disconnected.
    /// </summary>
    public TimeSpan SendStallTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long <see cref="DataServer{T}.DisposeAsync"/> lets subscribers drain their queues before cutting them off.
    /// </summary>
    public TimeSpan ShutdownDrainTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// TCP only: socket send buffer size in bytes; 0 keeps the operating system default.
    /// </summary>
    public int SocketSendBufferBytes { get; set; }
}
