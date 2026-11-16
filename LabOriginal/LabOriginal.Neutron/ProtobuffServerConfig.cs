using System.Net;

namespace LabOriginal.Neutron
{
    public class ProtobuffServerConfig<T> where T : IBaseMessage
    {
        public IPEndPoint ListenerAddress { get; set; }


        public T HeartBeatMsg { get; set; }

        public int HeartBeatIntervalInSec { get; set; }

        public bool HandleStartException { get; set; }

        public ProtobuffServerConfig(IPEndPoint listenerAddress, T heartBeatMsg, int heartBeatIntervalInSec, bool handleStartException = true)
        {
            ListenerAddress = listenerAddress;
            HeartBeatMsg = heartBeatMsg;
            HeartBeatIntervalInSec = heartBeatIntervalInSec;
            HandleStartException = handleStartException;
        }
    }
}
