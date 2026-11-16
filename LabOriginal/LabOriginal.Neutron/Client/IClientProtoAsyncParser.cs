using System.Threading.Tasks;

namespace LabOriginal.Neutron.Client
{
    public interface IClientProtoAsyncParser<in T>
    {
        Task ReceivedMsgAsync(T message);
    }
}
