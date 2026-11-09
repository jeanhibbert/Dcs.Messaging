using Light.Messaging.Internal;
using System;

namespace Light.Messaging;

/// <summary>
/// The other end of a connection: a connected client as seen by the server, or the server as seen by a client.
/// </summary>
public sealed class Peer
{
    internal Peer(Connection connection)
    {
        Connection = connection;
    }

    /// <summary>
    /// Assigned by the server, unique within the server process. Always 0 on the client side.
    /// </summary>
    public long Id => Connection.Id;

    /// <summary>
    /// The name the peer announced in its handshake (<see cref="MessagingOptions.Name"/>).
    /// </summary>
    public string Name => Connection.RemoteName;

    public string RemoteAddress => Connection.RemoteAddress;

    public bool IsConnected => !Connection.IsClosed;

    public string CloseReason => Connection.CloseReason;

    /// <summary>
    /// Frames waiting to be sent to this peer. A publisher can read this to slow down before the queue fills.
    /// </summary>
    public int QueuedFrames => Connection.QueuedFrames;

    /// <summary>
    /// Published messages discarded for this peer because its queue was full (DropOldest and DropNewest policies).
    /// </summary>
    public long DroppedFrames => Connection.DroppedFrames;

    internal Connection Connection { get; }

    public override string ToString() => $"{Name} ({RemoteAddress})";
}

/// <summary>
/// Information about a received message, passed to subscribers and responders.
/// </summary>
public readonly struct MessageContext
{
    internal MessageContext(Peer sender)
    {
        Sender = sender;
    }

    /// <summary>
    /// The peer the message came from. On the server, pass it to <see cref="MessagingServer.Publish{T}(Endpoint, T, Peer)"/>
    /// to reply to that client only.
    /// </summary>
    public Peer Sender { get; }
}

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

/// <summary>
/// The connection closed, or a request could not be delivered.
/// </summary>
public class MessagingException : Exception
{
    public MessagingException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The responder threw while handling the request; <see cref="Exception.Message"/> is the remote exception's message.
/// </summary>
public sealed class RemoteException : MessagingException
{
    public RemoteException(string message)
        : base(message)
    {
    }
}
