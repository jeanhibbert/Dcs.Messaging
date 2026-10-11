[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Dcs.Messaging.PerformanceTests;

/// <summary>
/// Server-to-client throughput for each transport. Results are written to the test output; run with
/// <c>dotnet test Dcs.Messaging.PerformanceTests -c Release --logger "console;verbosity=detailed"</c> to see them.
/// The assertions only check that every message arrived, because absolute numbers depend on the machine.
/// </summary>
[Trait("Category", "Performance")]
public class TransportThroughputTests
{
    private readonly ITestOutputHelper _output;

    public TransportThroughputTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public Task Tcp_ServerPumpsProtobufMessagesToClient()
    {
        return RunAndReportAsync(TransportKind.Tcp);
    }

    [Fact]
    public Task Grpc_ServerPumpsProtobufMessagesToClient()
    {
        return RunAndReportAsync(TransportKind.Grpc);
    }

    [Fact]
    public Task Sbe_ServerPumpsSbeMessagesToClient()
    {
        return RunAndReportAsync(TransportKind.Sbe);
    }

    [Fact]
    public async Task AllTransports_SideBySideComparison()
    {
        var results = new List<ThroughputResult>();
        foreach (var transport in Enum.GetValues<TransportKind>())
        {
            results.Add(await ThroughputScenario.RunAsync(transport, ThroughputScenario.MessageCount, ThroughputScenario.WarmupCount));
        }

        _output.WriteLine(ThroughputScenario.FormatTable(results));

        var fastest = results.MaxBy(r => r.MessagesPerSecond)!;
        foreach (var result in results)
        {
            _output.WriteLine($"{result.Transport}: {result.MessagesPerSecond / fastest.MessagesPerSecond:P0} of {fastest.Transport} throughput");
        }

        Assert.All(results, r => Assert.Equal(ThroughputScenario.MessageCount, r.MessageCount));
    }

    private async Task RunAndReportAsync(TransportKind transport)
    {
        var result = await ThroughputScenario.RunAsync(transport, ThroughputScenario.MessageCount, ThroughputScenario.WarmupCount);

        _output.WriteLine(ThroughputScenario.FormatTable(new[] { result }));
        Assert.Equal(ThroughputScenario.MessageCount, result.MessageCount);
    }
}
