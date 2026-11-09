namespace Light.Messaging;

/// <summary>
/// What happens when a peer's send queue is full of published messages because the peer (or the network) is not
/// keeping up. Publishing never blocks, and other peers are unaffected, whichever policy is chosen. Only published
/// messages are ever dropped: responses and control frames always get through. Drops are counted in
/// <see cref="Peer.DroppedFrames"/> and logged as a warning at most once per check (about once a second) per peer.
/// </summary>
public enum SlowConsumerPolicy
{
    /// <summary>
    /// Discard the oldest queued message to make room. The consumer sees gaps but stays current. The default; suits
    /// prices and other state that newer messages supersede.
    /// </summary>
    DropOldest,

    /// <summary>
    /// Close the connection. Nothing is silently lost: the peer sees a disconnect and must recover. Use it for orders
    /// and other streams where a gap is unacceptable.
    /// </summary>
    Disconnect,

    /// <summary>
    /// Discard the message being published.
    /// </summary>
    DropNewest
}
