using LabLegacy.Neutron;
using LabLegacy.Neutron.SampleMessages;
using System.Net;
using System.Reactive.Linq;

// Sample client built on NeutronClient<T>. Each time it connects it sends three orders; it prints what the server
// sends back, and after a few seconds checks that it received an acknowledgement for each order plus prices and
// heartbeats. Exit code 0 means everything arrived. Stop and restart the server while it runs to watch it reconnect.
// Usage: SampleClient [port] [seconds]   (defaults: 5080, 5)
var port = args.Length > 0 ? int.Parse(args[0]) : 5080;
var listenFor = TimeSpan.FromSeconds(args.Length > 1 ? int.Parse(args[1]) : 5);
const int orderCount = 3;

var client = new NeutronClient<SampleMessage>(new NeutronClientConfig
{
    ServerAddress = new IPEndPoint(IPAddress.Loopback, port),

    // The server heartbeats every 2 s; three missed heartbeats mean the connection is dead.
    HeartBeatTimeout = TimeSpan.FromSeconds(6),
});

var prices = 0;
var heartBeats = 0;
var connections = 0;
var acknowledged = new HashSet<long>();
long lastPriceSequence = 0;
var pricesInOrder = true;

client.State.Subscribe(state => Console.WriteLine($"[{state}]"));
client.Errors.Subscribe(error => Console.WriteLine($"  ! {error.Message}"));

// Nothing is replayed after a reconnect, so whatever the server needs is sent again on every connection.
client.State.Where(state => state == NeutronClientState.Connected).Subscribe(_ =>
{
    connections++;
    lastPriceSequence = 0;
    for (var i = 1; i <= orderCount; i++)
    {
        client.SendMessage(new SampleMessage { Kind = SampleMessageKind.Order, Sequence = i, Symbol = "EURUSD", Price = 1.0845 + i * 0.0001, Text = "buy 1M" });
    }

    Console.WriteLine($"  sent {orderCount} orders");
});

// One subscription per kind of message. They all run on the connection's read loop, one message at a time.
client.Messages.Where(message => message.Kind == SampleMessageKind.Price).Subscribe(message =>
{
    pricesInOrder &= message.Sequence > lastPriceSequence;
    lastPriceSequence = message.Sequence;
    if (++prices <= 3)
    {
        Console.WriteLine($"  price #{message.Sequence} {message.Symbol} {message.Price:F5}");
    }
});

client.Messages.Where(message => message.Kind == SampleMessageKind.OrderAck).Subscribe(message =>
{
    acknowledged.Add(message.Sequence);
    Console.WriteLine($"  order #{message.Sequence} {message.Text}");
});

client.Messages.Where(message => message.Kind == SampleMessageKind.HeartBeat).Subscribe(_ =>
{
    heartBeats++;
    Console.WriteLine("  heartbeat");
});

client.Start();
await Task.Delay(listenFor);

// Dispose stops the client; the last state, Disconnected, arrives once its loops have ended.
client.Dispose();
await client.State.LastOrDefaultAsync();

var checks = new (string Name, bool Passed)[]
{
    ($"connected ({connections} time{(connections == 1 ? "" : "s")})", connections > 0),
    ($"every order acknowledged ({acknowledged.Count}/{orderCount})", acknowledged.Count == orderCount),
    ($"prices received ({prices}) and in order", prices > 0 && pricesInOrder),
    ($"heartbeats received ({heartBeats})", heartBeats > 0),
};

Console.WriteLine();
foreach (var (name, passed) in checks)
{
    Console.WriteLine($"{(passed ? "PASS" : "FAIL")}  {name}");
}

return checks.All(check => check.Passed) ? 0 : 1;
