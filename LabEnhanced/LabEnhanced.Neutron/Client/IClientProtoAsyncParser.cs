namespace LabEnhanced.Neutron.Client;

public interface IClientProtoAsyncParser<in T>
{
    Task ReceivedMsgAsync(T message);
}
