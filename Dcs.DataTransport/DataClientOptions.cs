namespace Dcs.DataTransport;

public sealed class DataClientOptions
{
    public TransportKind Transport { get; set; } = TransportKind.Tcp;

    public string Host { get; set; } = "127.0.0.1";

    public int Port { get; set; } = 5050;

    /// <summary>
    /// Name the server reports in its subscriber statistics.
    /// </summary>
    public string SubscriberName { get; set; } = "subscriber";

    /// <summary>
    /// TCP only: size of the buffer each socket receive fills. Frames are decoded straight out of it.
    /// </summary>
    public int ReceiveBufferBytes { get; set; } = 256 * 1024;

    /// <summary>
    /// TCP only: socket receive buffer size in bytes; 0 keeps the operating system default.
    /// </summary>
    public int SocketReceiveBufferBytes { get; set; }
}
