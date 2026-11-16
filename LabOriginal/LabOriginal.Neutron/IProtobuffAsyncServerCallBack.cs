using System;

namespace LabOriginal.Neutron
{
    public interface IProtobuffAsyncServerCallBack<T> : IDisposable where T : IBaseMessage
    {
        void RemoveClientConnection(string id);
    }
}
