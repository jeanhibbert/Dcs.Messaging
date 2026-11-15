using ProtoBuf;

namespace LabLegacy.Neutron.SampleMessages;

public enum SampleMessageKind
{
    HeartBeat = 0,
    Price = 1,
    Order = 2,
    OrderAck = 3,
}

/// <summary>
/// The single root message type the sample server and client exchange. Server to client: HeartBeat, Price and
/// OrderAck. Client to server: Order.
/// </summary>
[ProtoContract]
public sealed class SampleMessage : IBaseMessage
{
    [ProtoMember(1)]
    public SampleMessageKind Kind { get; set; }

    [ProtoMember(2)]
    public long Sequence { get; set; }

    [ProtoMember(3)]
    public string? Symbol { get; set; }

    [ProtoMember(4)]
    public double Price { get; set; }

    [ProtoMember(5)]
    public string? Text { get; set; }
}
