using Dcs.DataTransport.Demo;
using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Dcs.DataTransport.DemoServer;

/// <summary>
/// Waits for the expected subscribers, publishes <see cref="PriceTick"/>s for a fixed time, then drains and prints
/// what each subscriber was sent. Arguments, all optional:
/// <code>
/// --transport Tcp|Grpc  --port 5050  --subscribers 3  --rate 200000 (msg/s, 0 = unthrottled)  --duration 10 (s)
/// --subscriberQueueCapacity 8192  --slowSubscriberPolicy DropOldest  (or any other DataServerOptions property)
/// </code>
/// </summary>
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var settings = new ConfigurationBuilder().AddCommandLine(args).Build();
        var options = new DataServerOptions { SubscriberQueueCapacity = 8192 };
        settings.Bind(options);
        var subscribers = settings.GetValue("subscribers", 3);
        var rate = settings.GetValue("rate", 200_000);
        var duration = TimeSpan.FromSeconds(settings.GetValue("duration", 10));

        await using var server = new DataServer<PriceTick>(PriceTickCodec.Instance, options);
        server.SubscriberConnected += stats => Console.WriteLine($"  + {stats.Name} connected");
        server.SubscriberDisconnected += stats => Console.WriteLine($"  - {stats.Name} closed: {stats.DisconnectReason}");
        await server.StartAsync();

        Console.WriteLine($"Listening: {options.Transport} on {options.Host}:{options.Port}, waiting for {subscribers} subscriber(s)");
        while (server.SubscriberCount < subscribers)
        {
            await Task.Delay(20);
        }

        Console.WriteLine($"Publishing {(rate == 0 ? "unthrottled" : $"{rate:N0} msg/s")} for {duration.TotalSeconds} s");
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        var gcsBefore = GC.CollectionCount(0);
        await Task.Factory.StartNew(() => Publish(server, rate, duration), TaskCreationOptions.LongRunning);
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var gcs = GC.CollectionCount(0) - gcsBefore;

        // Drains every subscriber's queue, then ends each stream, which is the clients' cue to print their results.
        await server.DisposeAsync();

        Console.WriteLine();
        Console.WriteLine($"SERVER RESULTS ({options.Transport}): published {server.Published:N0}; {(double)allocated / server.Published:N2} bytes allocated per message; {gcs} GCs");
        Console.WriteLine($"  {"Subscriber",-10} {"Sent",12} {"Dropped",12} {"Msgs/write",11}");
        foreach (var stats in server.GetSubscriberStats())
        {
            Console.WriteLine($"  {stats.Name,-10} {stats.Sent,12:N0} {stats.Dropped,12:N0} {(double)stats.Sent / Math.Max(stats.Batches, 1),11:N1}");
        }
    }

    /// <summary>
    /// Runs on a dedicated thread and spins between messages, so the rate is paced to microseconds rather than timer
    /// ticks. Each message carries its publish time, so client latency includes any time spent queued.
    /// </summary>
    private static void Publish(DataServer<PriceTick> server, int rate, TimeSpan duration)
    {
        var start = Stopwatch.GetTimestamp();
        long sequence = 0;

        while (Stopwatch.GetElapsedTime(start) < duration)
        {
            var due = rate == 0 ? sequence + 1000 : (long)(Stopwatch.GetElapsedTime(start).TotalSeconds * rate);
            while (sequence < due)
            {
                sequence++;
                server.Publish(new PriceTick(sequence, Stopwatch.GetTimestamp(), (int)(sequence % 50), 1.1000, 1.1001));
            }
        }
    }
}
