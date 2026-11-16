namespace LabEnhanced.Neutron;

public interface IProtobuffClientConnection<T> : IDisposable
{
    string Id { get; }

    bool SendMessage(T message);

    IObservable<IProtobuffClientConnection<T>> OnClientDisconnection { get; }

    bool Disconnect();
}
