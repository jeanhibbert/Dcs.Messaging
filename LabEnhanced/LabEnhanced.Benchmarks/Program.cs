using LabEnhanced.Benchmarks;
using System.Diagnostics;

// Runs LabOriginal and LabEnhanced through the same scenarios over loopback and prints them side by side.
//
//   dotnet run -c Release --project LabEnhanced/LabEnhanced.Benchmarks [-- <scale>]
//
// Every scenario is timed end to end: from the first send until the last message has been received (or the last
// reply awaited). Each is run once to warm up and then Rounds times; the median is reported. <scale> multiplies
// the message counts (default 1).

const int Rounds = 5;
var scale = args.Length > 0 ? double.Parse(args[0]) : 1;

Scenario[] scenarios =
[
    new("Client -> server, one-way", Clients: 1, Operations: 500_000, DeliveriesPerOperation: 1, (rig, operations) =>
    {
        var tick = new Tick();
        for (var i = 0; i < operations; i++)
        {
            rig.Send(tick);
        }

        return Task.CompletedTask;
    }),

    new("Server -> 8 clients, broadcast", Clients: 8, Operations: 100_000, DeliveriesPerOperation: 8, (rig, operations) =>
    {
        var tick = new Tick();
        for (var i = 0; i < operations; i++)
        {
            rig.Broadcast(tick);
        }

        return Task.CompletedTask;
    }),

    new("Request/reply, one at a time", Clients: 1, Operations: 30_000, DeliveriesPerOperation: 0, async (rig, operations) =>
    {
        for (var i = 0; i < operations; i++)
        {
            await rig.RequestAsync(new Tick { Sequence = i }).ConfigureAwait(false);
        }
    }),

    new("Request/reply, 64 in flight", Clients: 1, Operations: 256_000, DeliveriesPerOperation: 0, (rig, operations) =>
        Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < operations / 64; i++)
            {
                await rig.RequestAsync(new Tick { Sequence = i }).ConfigureAwait(false);
            }
        })))),
];

Console.WriteLine($"{Environment.ProcessorCount} logical cores, .NET {Environment.Version}, median of {Rounds} rounds");
Console.WriteLine();
Console.WriteLine($"| {"Scenario",-30} | {"Messages",9} | {"Original",14} | {"Enhanced",14} | {"Speed-up",8} | {"Original B/msg",14} | {"Enhanced B/msg",14} |");
Console.WriteLine($"|{new string('-', 32)}|{new string('-', 10)}:|{new string('-', 15)}:|{new string('-', 15)}:|{new string('-', 9)}:|{new string('-', 15)}:|{new string('-', 15)}:|");

foreach (var scenario in scenarios)
{
    var operations = (int)(scenario.Operations * scale) / 64 * 64;
    var original = await MeasureAsync(() => new OriginalRig(), scenario, operations);
    var enhanced = await MeasureAsync(() => new EnhancedRig(), scenario, operations);
    var messages = operations * Math.Max(1, scenario.DeliveriesPerOperation);

    Console.WriteLine(
        $"| {scenario.Name,-30} | {messages,9:N0} | {messages / original.Seconds,10:N0} /s | {messages / enhanced.Seconds,10:N0} /s | " +
        $"{original.Seconds / enhanced.Seconds,7:N1}x | {original.Bytes / messages,14:N0} | {enhanced.Bytes / messages,14:N0} |");
}

static async Task<Result> MeasureAsync(Func<IRig> create, Scenario scenario, int operations)
{
    var results = new List<Result>();
    for (var round = 0; round <= Rounds; round++)
    {
        var remaining = operations * scenario.DeliveriesPerOperation;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (remaining == 0)
        {
            delivered.SetResult();
        }

        using var rig = create();
        await rig.StartAsync(scenario.Clients, () =>
        {
            if (Interlocked.Decrement(ref remaining) == 0)
            {
                delivered.SetResult();
            }
        });

        GC.Collect();
        GC.WaitForPendingFinalizers();
        var bytes = GC.GetTotalAllocatedBytes(precise: true);
        var stopwatch = Stopwatch.StartNew();

        await scenario.Run(rig, operations);
        await delivered.Task.WaitAsync(TimeSpan.FromMinutes(5));

        stopwatch.Stop();
        bytes = GC.GetTotalAllocatedBytes(precise: true) - bytes;

        // The first round only warms up the JIT, the serializer and the thread pool.
        if (round > 0)
        {
            results.Add(new Result(stopwatch.Elapsed.TotalSeconds, bytes));
        }
    }

    results.Sort((left, right) => left.Seconds.CompareTo(right.Seconds));
    return results[Rounds / 2];
}

internal sealed record Scenario(string Name, int Clients, int Operations, int DeliveriesPerOperation, Func<IRig, int, Task> Run);

internal sealed record Result(double Seconds, double Bytes);
