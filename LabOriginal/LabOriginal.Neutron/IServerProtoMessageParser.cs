using System.Threading.Tasks;

namespace LabOriginal.Neutron
{
    public interface IServerProtoMessageParser<in T> where T : IBaseMessage
    {
        Task ReceivedMsgAsync(string connectionId, T message);
    }
}
