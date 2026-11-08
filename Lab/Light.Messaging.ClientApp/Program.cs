using Light.Messaging.Contracts;
using Light.Messaging.Grpc;
using Light.Messaging.Tcp;
using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging.ClientApp;

/// <summary>
/// Lab client: shows publish/subscribe, request/response and client-to-server publishing against the lab server,
/// then benchmarks request latency, pipelined request throughput and streaming throughput. Arguments, all optional:
/// <c>--transport tcp|grpc --host 127.0.0.1 --port 5070 --requests 20000 --burst 2000000</c>.
/// </summary>
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var settings = new ConfigurationBuilder().AddCommandLine(args).Build();
        var useGrpc = string.Equals(settings.GetValue("transport", "tcp"), "grpc", StringComparison.OrdinalIgnoreCase);
        var host = settings.GetValue("host", "127.0.0.1");
        var port = settings.GetValue("port", 5070);
        var requests = settings.GetValue("requests", 20_000);
        var burst = settings.GetValue("burst", 2_000_000);

        ClientTransport transport = useGrpc ? new GrpcClientTransport(host, port) : new TcpClientTransport(host, port);
        await using var client = await MessagingClient.ConnectAsync(transport, new MessagingClientOptions { Name = $"lab-client-{Environment.ProcessId}" });
        client.Disconnected += reason => Console.WriteLine($"Disconnected: {reason}");
        Console.WriteLine($"Connected over {(useGrpc ? "gRPC" : "TCP")} to {client.Server}");

        await ShowPublishSubscribeAsync(client);
        await ShowRequestResponseAsync(client);
        ShowClientPublish(client);

        Console.WriteLine();
        Console.WriteLine("Benchmarks (latency figures assume client and server share a host)");
        await MeasureRequestLatencyAsync(client, requests);
        await MeasurePipelinedRequestsAsync(client, requests * 10);
        await MeasureBurstAsync(client, burst);
    }

    private static async Task ShowPublishSubscribeAsync(MessagingClient client)
    {
        var received = 0;
        using (client.Subscribe<PriceTick>(LabEndpoints.Prices, tick =>
        {
            if (Interlocked.Increment(ref received) <= 3)
            {
                Console.WriteLine($"  price {tick.CurrencyPair} {tick.Bid:F5} / {tick.Ask:F5}");
            }
        }))
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
        }

        Console.WriteLine($"Publish/subscribe: {received} live prices in 1 s");
    }

    private static async Task ShowRequestResponseAsync(MessagingClient client)
    {
        var price = await client.RequestAsync<PricingRequest, ForwardPrice>(
            LabEndpoints.ForwardPricing,
            new PricingRequest { CurrencyPair = "EURUSD", Tenor = "3M", SpotRate = 1.08452 });

        Console.WriteLine($"Request/response: {price.CurrencyPair} {price.Tenor} forward {price.Bid:F5} / {price.Ask:F5}");
    }

    private static void ShowClientPublish(MessagingClient client)
    {
        client.Publish(LabEndpoints.TradeReports, new TradeReport
        {
            TradeId = Guid.NewGuid().ToString("N").Substring(0, 8),
            CurrencyPair = "EURUSD",
            IsBuy = true,
            Quantity = 5_000_000,
            Price = 1.08487,
        });

        Console.WriteLine("Client publish: trade report sent (see the server console)");
    }

    /// <summary>
    /// One request at a time: the full round trip, including both serializations and two thread-pool hops.
    /// </summary>
    private static async Task MeasureRequestLatencyAsync(MessagingClient client, int count)
    {
        var request = new PricingRequest { CurrencyPair = "EURUSD", Tenor = "1M", SpotRate = 1.08452 };
        for (var i = 0; i < 2_000; i++)
        {
            await client.RequestAsync<PricingRequest, ForwardPrice>(LabEndpoints.ForwardPricing, request);
        }

        var latencies = new long[count];
        var total = Stopwatch.StartNew();
        for (var i = 0; i < count; i++)
        {
            var start = Stopwatch.GetTimestamp();
            await client.RequestAsync<PricingRequest, ForwardPrice>(LabEndpoints.ForwardPricing, request);
            latencies[i] = Stopwatch.GetTimestamp() - start;
        }

        Console.WriteLine($"  Request/response, sequential: {count / total.Elapsed.TotalSeconds,12:N0} req/s   {Percentiles(latencies, count)}");
    }

    /// <summary>
    /// Many requests in flight, as a busy client would have; shows how far batching carries throughput.
    /// </summary>
    private static async Task MeasurePipelinedRequestsAsync(MessagingClient client, int count)
    {
        const int inFlight = 256;
        var request = new PricingRequest { CurrencyPair = "EURUSD", Tenor = "1M", SpotRate = 1.08452 };
        var total = Stopwatch.StartNew();

        await Task.WhenAll(Enumerable.Range(0, inFlight).Select(async worker =>
        {
            for (var i = worker; i < count; i += inFlight)
            {
                await client.RequestAsync<PricingRequest, ForwardPrice>(LabEndpoints.ForwardPricing, request);
            }
        }));

        Console.WriteLine($"  Request/response, {inFlight} in flight: {count / total.Elapsed.TotalSeconds,9:N0} req/s");
    }

    /// <summary>
    /// The server streams <paramref name="count"/> ticks to this client as fast as it can take them.
    /// </summary>
    private static async Task MeasureBurstAsync(MessagingClient client, int count)
    {
        var latencies = new long[count];
        var received = 0;
        long firstAt = 0;
        long lastAt = 0;

        using var subscription = client.Subscribe<PriceTick>(LabEndpoints.BurstTicks, tick =>
        {
            var now = Stopwatch.GetTimestamp();
            if (received == 0)
            {
                firstAt = now;
            }

            lastAt = now;
            latencies[received++] = now - tick.PublishedAt;
        });

        var reply = await client.RequestAsync<BurstRequest, BurstReply>(LabEndpoints.Burst, new BurstRequest { Count = count }, TimeSpan.FromMinutes(5));

        // The reply was queued after the last tick on the same connection, so every tick has been handled already.
        var seconds = (lastAt - firstAt) / (double)Stopwatch.Frequency;
        Console.WriteLine($"  Streaming {reply.Published:N0} ticks:        {received / seconds,12:N0} msg/s   {Percentiles(latencies, received)} (includes time queued at the server)");
    }

    private static string Percentiles(long[] ticks, int count)
    {
        Array.Sort(ticks, 0, count);
        string Micros(double percentile) => (ticks[(int)((count - 1) * percentile)] * 1e6 / Stopwatch.Frequency).ToString("N1");
        return $"p50 {Micros(0.5)} us, p99 {Micros(0.99)} us, p99.9 {Micros(0.999)} us, max {Micros(1)} us";
    }
}
