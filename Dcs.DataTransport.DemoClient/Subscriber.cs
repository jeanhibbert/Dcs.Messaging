using Dcs.DataTransport.Demo;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Dcs.DataTransport.DemoClient;

/// <summary>
/// One subscriber connection and what its handler measures. A slow subscriber spins in its handler for a fixed time
/// per message, like a consumer doing real work.
/// </summary>
internal sealed class Subscriber
{
    /// <summary>
    /// Only the most recent latencies are kept, so start-up (JIT, connecting) drops out of a long run.
    /// </summary>
    public const int LatencySamples = 1_000_000;

    private readonly string _name;
    private readonly DataClient<PriceTick> _client;
    private readonly long _workTicks;
    private readonly long[] _latencyTicks = new long[LatencySamples];
    private long _received;
    private long _missed;
    private long _nextSequence = 1;
    private long _firstAt;
    private long _lastAt;
    private string _error;

    public Subscriber(DataClientOptions options, TimeSpan work)
    {
        _name = options.SubscriberName;
        _client = new DataClient<PriceTick>(PriceTickCodec.Instance, options);
        _workTicks = (long)(work.TotalSeconds * Stopwatch.Frequency);
    }

    public async Task RunAsync()
    {
        try
        {
            await _client.RunAsync(OnMessage);
        }
        catch (Exception exception)
        {
            _error = exception.Message;
        }
    }

    public string FormatResults()
    {
        var count = (int)Math.Min(_received, LatencySamples);
        Array.Sort(_latencyTicks, 0, count);
        double Micros(double percentile) => count == 0 ? 0 : _latencyTicks[(int)((count - 1) * percentile)] * 1e6 / Stopwatch.Frequency;

        var rate = _received / Math.Max((_lastAt - _firstAt) / (double)Stopwatch.Frequency, 1e-9);
        return $"  {_name,-10} {_received,12:N0} {_missed,12:N0} {rate,11:N0} {Micros(0.5),9:N1} {Micros(0.99),9:N1} {Micros(0.999),9:N1} {Micros(1),9:N1}  {_error}";
    }

    private void OnMessage(PriceTick tick)
    {
        var now = Stopwatch.GetTimestamp();
        if (_firstAt == 0)
        {
            _firstAt = now;
        }

        _lastAt = now;
        _latencyTicks[_received++ % LatencySamples] = now - tick.PublishedAt;

        // Delivery is in order, so a jump in sequence is exactly the messages the server dropped for us.
        _missed += tick.Sequence - _nextSequence;
        _nextSequence = tick.Sequence + 1;

        while (Stopwatch.GetTimestamp() - now < _workTicks)
        {
        }
    }
}
