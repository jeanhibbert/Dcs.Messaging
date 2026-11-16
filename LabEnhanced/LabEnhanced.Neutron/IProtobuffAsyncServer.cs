using System.Net;

namespace LabEnhanced.Neutron;

public interface IProtobuffAsyncServer<T> : IProtobuffAsyncServerCallBack<T> where T : IBaseMessage
{
    void Start();
    void Stop();
    IPEndPoint ListenerAddress { get; }
    int ConnectedClientCount { get; }
    bool Disconnect(IProtobuffClientConnection<T> connection);
    bool SendMessage(string id, T message);
    bool SendMessage(T message);
    IObservable<IProtobuffClientConnection<T>> OnClientConnection { get; }
    IObservable<IProtobuffClientConnection<T>> OnClientDisconnection { get; }
    IObservable<Exception> OnServerError { get; }
    IDictionary<string, IProtobuffClientConnection<T>> ClientConnections { get; set; }
}
