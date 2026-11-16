using System.Net;

namespace LabOriginal.Neutron
{
    public interface IClientConfig<out T>
    {
        int AsyncTimeout { get; }
        bool CompressData { get; set; }
        int HeartBeatInterval { get; }
        T HeartBeatMsg { get; }
        IPEndPoint[] IPEndPoints { get; }
        bool IsEncrypted { get; set; }
        string Name { get; }
        string Password { get; set; }
        bool TurnOnEnhancedLogging { get; set; }
    }
}
