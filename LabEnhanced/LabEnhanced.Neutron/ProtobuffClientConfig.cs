using System.Net;

namespace LabEnhanced.Neutron;

public class ProtobuffClientConfig<T> : IClientConfig<T>
{
    public string Name { get; }
    public IPEndPoint[] IPEndPoints { get; set; }
    public T? HeartBeatMsg { get; set; }
    public int HeartBeatInterval { get; set; }
    public int AsyncTimeout { get; set; }
    public bool TurnOnEnhancedLogging { get; set; }
    public bool CompressData { get; set; }
    public bool IsEncrypted { get; set; }
    public string? Password { get; set; }

    /// <summary>
    /// Largest message accepted from the server. A larger length prefix closes the connection.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// Bytes that may be waiting to be sent. If the server falls further behind, the connection is closed.
    /// </summary>
    public int MaxSendBacklogBytes { get; set; } = 16 * 1024 * 1024;

    public ProtobuffClientConfig(string connectionName, IPEndPoint[] ipEndPoints, T? heartBeatMsg, int heartBeatIntervalInSecs, int asyncTimeout = 30000)
    {
        Name = connectionName;
        IPEndPoints = ipEndPoints;
        HeartBeatMsg = heartBeatMsg;
        HeartBeatInterval = heartBeatIntervalInSecs;
        AsyncTimeout = asyncTimeout;
    }
}
