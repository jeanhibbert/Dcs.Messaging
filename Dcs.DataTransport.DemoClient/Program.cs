using Microsoft.Extensions.Configuration;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dcs.DataTransport.DemoClient;

/// <summary>
/// Opens fast and slow subscribers to a DemoServer and, once the server ends the stream, prints each one's throughput,
/// missed messages and latency. Arguments, all optional:
/// <code>
/// --transport Tcp|Grpc  --host 127.0.0.1  --port 5050  --fast 2  --slow 1  --slowWorkUs 20 (CPU time per message)
/// </code>
/// </summary>
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var settings = new ConfigurationBuilder().AddCommandLine(args).Build();
        var fast = settings.GetValue("fast", 2);
        var slow = settings.GetValue("slow", 1);
        var slowWork = TimeSpan.FromMicroseconds(settings.GetValue("slowWorkUs", 20));

        var subscribers = Enumerable.Range(1, fast + slow).Select(i =>
        {
            var options = new DataClientOptions();
            settings.Bind(options);
            options.SubscriberName = i <= fast ? $"fast-{i}" : $"slow-{i - fast}";
            return new Subscriber(options, i <= fast ? TimeSpan.Zero : slowWork);
        }).ToList();

        Console.WriteLine($"Running {fast} fast and {slow} slow ({slowWork.TotalMicroseconds} us/msg) subscriber(s) until the server ends the stream");
        await Task.WhenAll(subscribers.Select(subscriber => subscriber.RunAsync()));

        Console.WriteLine();
        Console.WriteLine($"CLIENT RESULTS ({settings.GetValue("transport", TransportKind.Tcp)}): latency is publish to handler in us, over each subscriber's last {Subscriber.LatencySamples:N0} messages");
        Console.WriteLine($"  {"Subscriber",-10} {"Received",12} {"Missed",12} {"Msg/s",11} {"p50",9} {"p99",9} {"p99.9",9} {"max",9}");
        foreach (var subscriber in subscribers)
        {
            Console.WriteLine(subscriber.FormatResults());
        }
    }
}
