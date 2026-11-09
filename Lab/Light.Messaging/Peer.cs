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
