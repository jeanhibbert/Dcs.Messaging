using LabLegacy.Neutron.SampleMessages;
using ProtoBuf;
using System.Net.Sockets;

// Sample client. It talks to the server exactly as an existing legacy client does: protobuf-net's length-prefixed
// stream calls over a TcpClient. It sends three orders, listens for a few seconds, and checks that it received an
// acknowledgement for each order plus prices and heartbeats. Exit code 0 means everything arrived.
// Usage: SampleClient [port] [seconds]   (defaults: 5080, 5)
var port = args.Length > 0 ? int.Parse(args[0]) : 5080;
var listenFor = TimeSpan.FromSeconds(args.Length > 1 ? int.Parse(args[1]) : 5);
const int orderCount = 3;

using var tcpClient = new TcpClient { NoDelay = true };
await tcpClient.ConnectAsync("127.0.0.1", port);
var stream = tcpClient.GetStream();
Console.WriteLine($"Connected to 127.0.0.1:{port}");

var prices = 0;
var heartBeats = 0;
var acknowledged = new HashSet<long>();
long lastPriceSequence = 0;
var pricesInOrder = true;

// Reads until the connection is closed, which the main thread does when the listening time is up.
var reading = Task.Run(() =>
{
    try
    {
        while (Serializer.DeserializeWithLengthPrefix<SampleMessage>(stream, PrefixStyle.Base128) is { } message)
        {
            switch (message.Kind)
            {
                case SampleMessageKind.Price:
                    pricesInOrder &= message.Sequence > lastPriceSequence;
                    lastPriceSequence = message.Sequence;
                    if (++prices <= 3)
                    {
                        Console.WriteLine($"  price #{message.Sequence} {message.Symbol} {message.Price:F5}");
                    }

                    break;

                case SampleMessageKind.OrderAck:
                    acknowledged.Add(message.Sequence);
                    Console.WriteLine($"  order #{message.Sequence} {message.Text}");
                    break;

                case SampleMessageKind.HeartBeat:
                    heartBeats++;
                    Console.WriteLine("  heartbeat");
                    break;
            }
        }
    }
    catch (IOException)
    {
    }
});

for (var i = 1; i <= orderCount; i++)
{
    Serializer.SerializeWithLengthPrefix(
        stream,
        new SampleMessage { Kind = SampleMessageKind.Order, Sequence = i, Symbol = "EURUSD", Price = 1.0845 + i * 0.0001, Text = "buy 1M" },
        PrefixStyle.Base128);
}

Console.WriteLine($"Sent {orderCount} orders; listening for {listenFor.TotalSeconds:N0} s");
await Task.Delay(listenFor);
tcpClient.Close();
await reading;

var checks = new (string Name, bool Passed)[]
{
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
