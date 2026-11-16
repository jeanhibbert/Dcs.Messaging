using System.Net;

namespace LabEnhanced.Neutron;

public class ProtobuffServerConfig<T> where T : IBaseMessage
{
    /// <summary>
    /// Port 0 picks a free port; the server's ListenerAddress then reports the one in use.
    /// </summary>
    public IPEndPoint ListenerAddress { get; set; }

    public T? HeartBeatMsg { get; set; }

    public int HeartBeatIntervalInSec { get; set; }

    public bool HandleStartException { get; set; }

    /// <summary>
    /// Largest message accepted from a client. A larger length prefix closes the connection.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// Bytes that may be waiting to be sent to one client. A client that falls further behind is disconnected, so it
    /// can never hold up the sender or the other clients.
    /// </summary>
    public int MaxSendBacklogBytes { get; set; } = 16 * 1024 * 1024;

    public ProtobuffServerConfig(IPEndPoint listenerAddress, T? heartBeatMsg, int heartBeatIntervalInSec, bool handleStartException = true)
    {
        ListenerAddress = listenerAddress;
        HeartBeatMsg = heartBeatMsg;
        HeartBeatIntervalInSec = heartBeatIntervalInSec;
        HandleStartException = handleStartException;
    }
}
