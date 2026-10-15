namespace Dcs.DataTransport;

/// <summary>
/// Point-in-time counters for one subscriber, as seen by the server.
/// </summary>
/// <param name="Sent">Messages handed to the network (socket or HTTP/2 stream).</param>
/// <param name="Dropped">Messages discarded by <see cref="SlowSubscriberPolicy"/> because the queue was full.</param>
/// <param name="Batches">Socket writes (TCP) or gRPC messages (gRPC) used to send <paramref name="Sent"/>.</param>
/// <param name="QueueDepth">Messages currently waiting to be sent.</param>
public readonly record struct SubscriberStats(
    long Id,
    string Name,
    TransportKind Transport,
    string RemoteEndPoint,
    bool IsConnected,
    long Sent,
    long Dropped,
    long Batches,
    int QueueDepth,
    string DisconnectReason);
