namespace LabEnhanced.Neutron;

public interface IBaseMessage
{
    int AsyncId { get; set; }

    bool AsyncSend { get; set; }
}
