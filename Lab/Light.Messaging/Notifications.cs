namespace Light.Messaging;

public enum ConnectionState
{
    Connected,

    /// <summary>
    /// The connection was lost and the client is retrying (<see cref="MessagingClientOptions.AutoReconnect"/>).
    /// </summary>
    Reconnecting,

    /// <summary>
    /// Final: the client was disposed, reconnecting is off, or it gave up.
    /// </summary>
    Closed
}

/// <summary>
/// An entry in <see cref="MessagingClient.Status"/>.
/// </summary>
/// <param name="Server">The server as seen by the connection this status is about.</param>
/// <param name="Reason">Why the connection closed; null while connected.</param>
public readonly record struct ConnectionStatus(ConnectionState State, Peer Server, string Reason);

/// <summary>
/// Published messages dropped for a slow consumer since the previous report; see
/// <see cref="MessagingSession.SlowConsumers"/>. Reports come at most once per watchdog check (about once a second)
/// per peer, never once per message.
/// </summary>
/// <param name="Dropped">Messages dropped since the previous report for this peer.</param>
/// <param name="TotalDropped">Messages dropped for this peer since it connected.</param>
/// <param name="LastEndpoint">Address of the endpoint the most recent drop was on.</param>
public readonly record struct SlowConsumerReport(Peer Peer, long Dropped, long TotalDropped, string LastEndpoint);
