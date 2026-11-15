using LabLegacy.Neutron;
using LabLegacy.Neutron.SampleMessages;
using Microsoft.Extensions.Logging;
using System.Net;

// Sample server: heartbeats every 2 s, a price broadcast twice a second, and an acknowledgement for every order a
// client sends. Usage: SampleServer [port] [seconds]   (defaults: 5080, run until Ctrl+C)
var port = args.Length > 0 ? int.Parse(args[0]) : 5080;
var runFor = args.Length > 1 ? TimeSpan.FromSeconds(int.Parse(args[1])) : Timeout.InfiniteTimeSpan;

using var loggerFactory = LoggerFactory.Create(logging => logging.AddSimpleConsole(console =>
{
    console.SingleLine = true;
    console.TimestampFormat = "HH:mm:ss.fff ";
}));

var orders = new OrderHandler();
using var server = new NeutronAsyncServer<SampleMessage>(
    orders,
    new NeutronServerConfig<SampleMessage>
    {
        ListenerAddress = new IPEndPoint(IPAddress.Loopback, port),
        HeartBeatMsg = new SampleMessage { Kind = SampleMessageKind.HeartBeat },
        HeartBeatIntervalInSec = 2,
    },
    loggerFactory.CreateLogger<NeutronAsyncServer<SampleMessage>>());
orders.Server = server;

server.OnClientConnection += (_, connection) => Console.WriteLine($"+ client connected: {connection.Id}");
server.OnClientDisconnection += (_, connection) => Console.WriteLine($"- client disconnected: {connection.Id}");
server.OnServerError += (_, exception) => Console.WriteLine($"! server error: {exception.Message}");
server.Start();
Console.WriteLine($"Listening on {server.ListenerAddress}. Press Ctrl+C to stop.");

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};
if (runFor != Timeout.InfiniteTimeSpan)
{
    stop.CancelAfter(runFor);
}

// Broadcast: serialized once per tick and queued to every client; SendMessage returns without waiting on any socket.
var sequence = 0L;
using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
try
{
    while (await timer.WaitForNextTickAsync(stop.Token))
    {
        server.SendMessage(new SampleMessage
        {
            Kind = SampleMessageKind.Price,
            Sequence = ++sequence,
            Symbol = "EURUSD",
            Price = Math.Round(1.0845 + Random.Shared.NextDouble() * 0.001, 5),
        });
    }
}
catch (OperationCanceledException)
{
}

Console.WriteLine($"Stopping: {sequence} prices broadcast, {orders.Handled} orders acknowledged");
server.Stop();

/// <summary>
/// Called for each message a client sends, one at a time per connection, in order. Replies to the sender only.
/// </summary>
internal sealed class OrderHandler : IServerProtoMessageParser<SampleMessage>
{
    private int _handled;

    public NeutronAsyncServer<SampleMessage>? Server { get; set; }

    public int Handled => _handled;

    public Task ReceivedMsgAsync(string connectionId, SampleMessage message)
    {
        if (message.Kind == SampleMessageKind.Order)
        {
            Interlocked.Increment(ref _handled);
            Console.WriteLine($"  order #{message.Sequence} {message.Text} {message.Symbol} @ {message.Price:F5}");
            Server!.SendMessage(connectionId, new SampleMessage
            {
                Kind = SampleMessageKind.OrderAck,
                Sequence = message.Sequence,
                Symbol = message.Symbol,
                Text = "accepted",
            });
        }

        return Task.CompletedTask;
    }
}
