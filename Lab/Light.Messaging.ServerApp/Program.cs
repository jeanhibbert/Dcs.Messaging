using Light.Messaging.Contracts;
using Light.Messaging.Grpc;
using Light.Messaging.Tcp;
using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging.ServerApp;

/// <summary>
/// Lab server: streams live prices, prices forwards on request, logs trades that clients report, and serves the
/// client's benchmark bursts. Arguments, all optional: <c>--transport tcp|grpc --host 127.0.0.1 --port 5070</c>.
/// </summary>
internal static class Program
{
    private static readonly string[] CurrencyPairs = ["EURUSD", "GBPUSD", "USDJPY"];

    private static async Task Main(string[] args)
    {
        var settings = new ConfigurationBuilder().AddCommandLine(args).Build();
        var useGrpc = string.Equals(settings.GetValue("transport", "tcp"), "grpc", StringComparison.OrdinalIgnoreCase);
        var host = settings.GetValue("host", "127.0.0.1");
        var port = settings.GetValue("port", 5070);

        ServerTransport transport = useGrpc ? new GrpcServerTransport(host, port) : new TcpServerTransport(host, port);
        await using var server = await MessagingServer.StartAsync(transport, new MessagingServerOptions { Name = "lab-server" });
        server.PeerConnected += peer => Console.WriteLine($"+ {peer} connected");
        server.PeerDisconnected += peer => Console.WriteLine($"- {peer} disconnected: {peer.CloseReason}");
        server.HandlerError += exception => Console.WriteLine($"! handler error: {exception.Message}");

        // Request/response.
        using var pricing = server.Respond<PricingRequest, ForwardPrice>(LabEndpoints.ForwardPricing, (request, _) => new ForwardPrice
        {
            CurrencyPair = request.CurrencyPair,
            Tenor = request.Tenor,
            Bid = Math.Round(request.SpotRate - 0.00035, 5),
            Ask = Math.Round(request.SpotRate + 0.00035, 5),
        });

        // Messages published by clients.
        using var trades = server.Subscribe<TradeReport>(LabEndpoints.TradeReports, (trade, context) =>
            Console.WriteLine($"  trade {trade.TradeId} from {context.Sender.Name}: {(trade.IsBuy ? "buy" : "sell")} {trade.Quantity:N0} {trade.CurrencyPair} @ {trade.Price:F5}"));

        // Benchmark: published from the thread pool so the requesting client's receive loop is not held up.
        using var bursts = server.RespondAsync<BurstRequest, BurstReply>(LabEndpoints.Burst, (request, context) =>
            new ValueTask<BurstReply>(Task.Run(() => PublishBurst(server, request.Count, context.Sender))));

        using var stop = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stop.Cancel();
        };

        Console.WriteLine($"Listening: {(useGrpc ? "gRPC" : "TCP")} on {host}:{port}. Press Ctrl+C to stop.");
        await PublishLivePricesAsync(server, stop.Token);
        Console.WriteLine("Stopping: draining client connections");
    }

    /// <summary>
    /// Ten ticks a second per pair to whichever clients subscribe; costs nothing while nobody does.
    /// </summary>
    private static async Task PublishLivePricesAsync(MessagingServer server, CancellationToken cancellationToken)
    {
        var mids = new[] { 1.08450, 1.26530, 151.250 };
        var sequence = 0L;
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                for (var i = 0; i < CurrencyPairs.Length; i++)
                {
                    mids[i] *= 1 + (Random.Shared.NextDouble() - 0.5) * 0.0001;
                    server.Publish(LabEndpoints.Prices, new PriceTick
                    {
                        Sequence = ++sequence,
                        PublishedAt = Stopwatch.GetTimestamp(),
                        CurrencyPair = CurrencyPairs[i],
                        Bid = mids[i] - 0.00005,
                        Ask = mids[i] + 0.00005,
                    });
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>
    /// Publishes as fast as the client can take it. Watching the peer's queue depth is how a publisher applies flow
    /// control instead of tripping the slow-consumer policy. A single message object is reused, which is safe because
    /// Publish serializes before returning.
    /// </summary>
    private static BurstReply PublishBurst(MessagingServer server, int count, Peer peer)
    {
        const int maxQueued = 32_768;
        var tick = new PriceTick { CurrencyPair = "EURUSD" };
        var stopwatch = Stopwatch.StartNew();
        var published = 0;

        while (published < count && peer.IsConnected)
        {
            if (peer.QueuedFrames > maxQueued)
            {
                Thread.Yield();
                continue;
            }

            tick.Sequence = published + 1;
            tick.PublishedAt = Stopwatch.GetTimestamp();
            tick.Bid = 1.08450 + (published % 100) * 0.00001;
            tick.Ask = tick.Bid + 0.0001;
            if (!server.Publish(LabEndpoints.BurstTicks, tick, peer))
            {
                break;
            }

            published++;
        }

        return new BurstReply { Published = published, ElapsedMilliseconds = stopwatch.Elapsed.TotalMilliseconds };
    }
}
