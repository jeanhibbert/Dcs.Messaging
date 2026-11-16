using System;
using System.Collections.Generic;
using System.Net;

namespace LabOriginal.Neutron
{
    public interface IProtobuffAsyncServer<T> : IProtobuffAsyncServerCallBack<T> where T : IBaseMessage
    {
        void Start();
        void Stop();
        IPEndPoint ListenerAddress { get; }
        int ConnectedClientCount { get; }
        bool Disconnect(IProtobuffClientConnection<T> connection);
        bool SendMessage(string id, T message);
        bool SendMessage(T message);
        event EventHandler<IProtobuffClientConnection<T>> OnClientConnection;
        event EventHandler<IProtobuffClientConnection<T>> OnClientDisconnection;
        event EventHandler<Exception> OnServerError;
        IDictionary<string, IProtobuffClientConnection<T>> ClientConnections { get; set; }
    }
}
