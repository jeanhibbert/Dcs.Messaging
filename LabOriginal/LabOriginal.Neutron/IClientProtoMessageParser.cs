namespace LabOriginal.Neutron
{
    public interface IClientProtoMessageParser<in T>
    {
        void ReceivedMsg(T message);
    }
}
