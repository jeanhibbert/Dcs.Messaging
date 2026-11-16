using System;

namespace LabOriginal.Neutron
{
    public interface IProtobuffClientConnection<T> : IDisposable
    {
        string Id { get; }

        bool SendMessage(T message);

        event EventHandler<IProtobuffClientConnection<T>> OnClientDisconnection;

        bool Disconnect();
    }
}
