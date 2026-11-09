using Light.Messaging.Contracts;
using Light.Messaging.Grpc;
using Light.Messaging.Tcp;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Reactive.Linq;
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

        // Connections, slow-consumer warnings and handler errors are logged by the library itself.
        using var loggerFactory = LoggerFactory.Create(logging => logging.AddSimpleConsole(console =>
        {
            console.SingleLine = true;
            console.TimestampFormat = "HH:mm:ss.fff ";
        }));

        ServerTransport transport = useGrpc ? new GrpcServerTransport(host, port) : new TcpServerTransport(host, port);
        await using var server = await MessagingServer.StartAsync(transport, new MessagingServerOptions
        {
            Name = "lab-server",
            Logger = loggerFactory.CreateLogger("Light.Messaging"),
        });

        // Request/response.
        using var pricing = server.Respond<PricingRequest, ForwardPrice>(LabEndpoints.ForwardPricing, (request, _) => new ForwardPrice
        {
            CurrencyPair = request.CurrencyPair,
            Tenor = request.Tenor,
            Bid = Math.Round(request.SpotRate - 0.00035, 5),
            Ask = Math.Round(request.SpotRate + 0.00035, 5),
        });

        // Messages published by clients, as an Rx stream.
        using var trades = server.ObserveWithSender<TradeReport>(LabEndpoints.TradeReports).Subscribe(trade =>
            Console.WriteLine($"  trade {trade.Message.TradeId} from {trade.Sender.Name}: {(trade.Message.IsBuy ? "buy" : "sell")} {trade.Message.Quantity:N0} {trade.Message.CurrencyPair} @ {trade.Message.Price:F5}"));

        // Benchmark: published from the thread pool so the requesting client's receive loop is not held up.
        using var bursts = server.RespondAsync<BurstRequest, BurstReply>(LabEndpoints.Burst, (request, context) =>
            new ValueTask<BurstReply>(Task.Run(() => PublishBurst(server, request.Count, context.Sender))));

        // Ten ticks a second per pair to whichever clients subscribe; costs nothing while nobody does. Rx calls the
        // handler for one tick at a time, so the random walk needs no locking.
        var mids = new[] { 1.08450, 1.26530, 151.250 };
        using var livePrices = Observable.Interval(TimeSpan.FromMilliseconds(100)).Subscribe(sequence => PublishPrices(server, mids, sequence));

        Console.WriteLine($"Listening: {(useGrpc ? "gRPC" : "TCP")} on {host}:{port}. Press Ctrl+C to stop.");
        await CtrlC();
        Console.WriteLine("Stopping: draining client connections");
    }

    private static void PublishPrices(MessagingServer server, double[] mids, long sequence)
    {
        for (var i = 0; i < CurrencyPairs.Length; i++)
        {
            mids[i] *= 1 + (Random.Shared.NextDouble() - 0.5) * 0.0001;
            server.Publish(LabEndpoints.Prices, new PriceTick
            {
                Sequence = sequence * CurrencyPairs.Length + i,
                PublishedAt = Stopwatch.GetTimestamp(),
                CurrencyPair = CurrencyPairs[i],
                Bid = mids[i] - 0.00005,
                Ask = mids[i] + 0.00005,
            });
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

    /// <summary>
    /// Completes on the first Ctrl+C, which is cancelled so the process can shut down cleanly.
    /// </summary>
    private static async Task CtrlC()
    {
        await Observable.FromEventPattern<ConsoleCancelEventHandler, ConsoleCancelEventArgs>(
                handler => Console.CancelKeyPress += handler,
                handler => Console.CancelKeyPress -= handler)
            .Do(e => e.EventArgs.Cancel = true)
            .FirstAsync();
    }
}
