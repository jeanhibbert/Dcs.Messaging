using System.Net;
using System.Reactive;

namespace LabEnhanced.Neutron;

public interface IProtobufClient<T> : IDisposable where T : IBaseMessage
{
    IObservable<Unit> Connected { get; }
    IObservable<Unit> Disconnected { get; }
    Exception? ConnectionException { get; set; }
    IPEndPoint? ConnectedEndpoint { get; }
    bool IsConnected { get; }
    ProtobuffClientConfig<T> Config { get; }
    void Connect();
    Task ConnectAsync();
    void Disconnect();
    bool SendMessage(T message);
    Task<T> SendMessageAsync(T message);
    Task<T> SendMessageAsync(T message, bool continueOnCapturedContext);
}
