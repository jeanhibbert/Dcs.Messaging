namespace Dcs.DataTransport;

/// <summary>
/// What the server does when a subscriber's queue is full because it is not keeping up. Whichever policy is chosen,
/// <see cref="DataServer{T}.Publish"/> never blocks and the other subscribers are unaffected.
/// </summary>
public enum SlowSubscriberPolicy
{
    /// <summary>
    /// Discard the oldest queued message. The subscriber sees gaps but always converges on the latest data,
    /// which suits prices and other state that newer messages supersede.
    /// </summary>
    DropOldest,

    /// <summary>
    /// Discard the message being published. The subscriber's backlog stays contiguous but grows staler.
    /// </summary>
    DropNewest,

    /// <summary>
    /// Disconnect the subscriber, which must reconnect and recover. Suits streams where a gap is unacceptable.
    /// </summary>
    Disconnect
}
