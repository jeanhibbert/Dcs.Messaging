namespace Dcs.DataTransport.Demo;

/// <summary>
/// Demo market-data message. <see cref="PublishedAt"/> is a <see cref="System.Diagnostics.Stopwatch"/> timestamp,
/// which is comparable across processes on the same machine but not across machines, so latency figures are only
/// meaningful when the server and client share a host.
/// </summary>
public readonly record struct PriceTick(long Sequence, long PublishedAt, int CurrencyPairId, double Bid, double Ask);
