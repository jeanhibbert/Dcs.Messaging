using System.Net;

namespace LabOriginal.Neutron
{
    public class ProtobuffClientConfig<T> : IClientConfig<T>
    {
        public string Name { get; }
        public IPEndPoint[] IPEndPoints { get; set; }
        public T HeartBeatMsg { get; set; }
        public int HeartBeatInterval { get; set; }
        public int AsyncTimeout { get; set; }
        public bool TurnOnEnhancedLogging { get; set; }
        public bool CompressData { get; set; }
        public bool IsEncrypted { get; set; }
        public string Password { get; set; }

        public ProtobuffClientConfig(string connectionName, IPEndPoint[] ipEndPoints, T heartBeatMsg, int heartBeatIntervalInSecs, int asyncTimeout = 30000)
        {
            Name = connectionName;
            IPEndPoints = ipEndPoints;
            HeartBeatMsg = heartBeatMsg;
            HeartBeatInterval = heartBeatIntervalInSecs;
            AsyncTimeout = asyncTimeout;
        }
    }
}
