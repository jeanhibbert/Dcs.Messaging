using ProtoBuf;

namespace Light.Messaging.Contracts;

[ProtoContract]
public sealed class PriceTick
{
    [ProtoMember(1)]
    public long Sequence { get; set; }

    /// <summary>
    /// <see cref="System.Diagnostics.Stopwatch"/> timestamp taken when published. Comparable across processes on the
    /// same machine only, so latency figures derived from it need client and server on one host.
    /// </summary>
    [ProtoMember(2)]
    public long PublishedAt { get; set; }

    [ProtoMember(3)]
    public string CurrencyPair { get; set; }

    [ProtoMember(4)]
    public double Bid { get; set; }

    [ProtoMember(5)]
    public double Ask { get; set; }
}

[ProtoContract]
public sealed class PricingRequest
{
    [ProtoMember(1)]
    public string CurrencyPair { get; set; }

    [ProtoMember(2)]
    public string Tenor { get; set; }

    [ProtoMember(3)]
    public double SpotRate { get; set; }
}

[ProtoContract]
public sealed class ForwardPrice
{
    [ProtoMember(1)]
    public string CurrencyPair { get; set; }

    [ProtoMember(2)]
    public string Tenor { get; set; }

    [ProtoMember(3)]
    public double Bid { get; set; }

    [ProtoMember(4)]
    public double Ask { get; set; }
}

[ProtoContract]
public sealed class TradeReport
{
    [ProtoMember(1)]
    public string TradeId { get; set; }

    [ProtoMember(2)]
    public string CurrencyPair { get; set; }

    [ProtoMember(3)]
    public bool IsBuy { get; set; }

    [ProtoMember(4)]
    public double Quantity { get; set; }

    [ProtoMember(5)]
    public double Price { get; set; }
}

[ProtoContract]
public sealed class BurstRequest
{
    [ProtoMember(1)]
    public int Count { get; set; }
}

[ProtoContract]
public sealed class BurstReply
{
    [ProtoMember(1)]
    public long Published { get; set; }

    [ProtoMember(2)]
    public double ElapsedMilliseconds { get; set; }
}
