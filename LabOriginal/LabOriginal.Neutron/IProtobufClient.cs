using System;
using System.Net;
using System.Threading.Tasks;

namespace LabOriginal.Neutron
{
    public interface IProtobufClient<T> : IDisposable where T : IBaseMessage
    {
        event EventHandler Connected;
        event EventHandler Disconnected;
        Exception ConnectionException { get; set; }
        IPEndPoint ConnectedEndpoint { get; }
        bool IsConnected { get; }
        ProtobuffClientConfig<T> Config { get; }
        void Connect();
        Task ConnectAsync();
        void Disconnect();
        bool SendMessage(T message);
        Task<T> SendMessageAsync(T message);
        Task<T> SendMessageAsync(T message, bool continueOnCapturedContext);
    }
}
