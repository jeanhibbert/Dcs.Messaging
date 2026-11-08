namespace Light.Messaging.Contracts;

/// <summary>
/// Endpoints shared by the lab server and client. Declaring them once in a shared assembly is what keeps both sides
/// agreeing on addresses (and therefore on endpoint ids).
/// </summary>
public static class LabEndpoints
{
    /// <summary>
    /// Server publishes <see cref="PriceTick"/> to subscribed clients.
    /// </summary>
    public static readonly Endpoint Prices = new("Lab/Prices");

    /// <summary>
    /// Request <see cref="PricingRequest"/>, response <see cref="ForwardPrice"/>.
    /// </summary>
    public static readonly Endpoint ForwardPricing = new("Lab/Pricing/Forward");

    /// <summary>
    /// Clients publish <see cref="TradeReport"/> to the server.
    /// </summary>
    public static readonly Endpoint TradeReports = new("Lab/Trades");

    /// <summary>
    /// Request <see cref="BurstRequest"/>: the server streams that many <see cref="PriceTick"/>s to the requester on
    /// <see cref="BurstTicks"/>, then responds with <see cref="BurstReply"/>.
    /// </summary>
    public static readonly Endpoint Burst = new("Lab/Benchmark/Burst");

    public static readonly Endpoint BurstTicks = new("Lab/Benchmark/Ticks");
}
