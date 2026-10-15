namespace Dcs.DataTransport;

public enum TransportKind
{
    /// <summary>
    /// Length-prefixed frames written straight to a TCP socket.
    /// </summary>
    Tcp,

    /// <summary>
    /// The same frames, batched into the messages of one gRPC server-streaming call over HTTP/2.
    /// </summary>
    Grpc
}
