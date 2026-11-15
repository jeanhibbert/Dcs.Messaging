using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;

namespace LabLegacy.Neutron.Tests;

/// <summary>
/// <see cref="NeutronClient{T}"/> against a real <see cref="NeutronAsyncServer{T}"/> on the loopback interface.
/// </summary>
public sealed class NeutronClientTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task MessagesFlowBothWaysInOrder()
    {
        var parser = new RecordingParser();
        using var server = StartServer(parser);
        using var client = NewClient(server.ListenerAddress);
        var received = client.Messages.Take(1000).ToList().ToTask();

        await StartAndConnectAsync(client, server);
        for (var i = 0; i < 1000; i++)
        {
            Assert.True(client.SendMessage(new TestMessage { Number = i }));
            Assert.True(server.SendMessage(new TestMessage { Number = i }));
        }

        Assert.Equal(Enumerable.Range(0, 1000), (await received.WaitAsync(Timeout)).Select(message => message.Number));
        await WaitUntilAsync(() => parser.Received.Count == 1000);
        Assert.Equal(Enumerable.Range(0, 1000), parser.Received.Select(message => message.Number));
    }

    [Fact]
    public async Task ConcurrentSendersDoNotCorruptTheStream()
    {
        const int senders = 4;
        const int perSender = 2_000;
        var parser = new RecordingParser();
        using var server = StartServer(parser);
        using var client = NewClient(server.ListenerAddress);
        await StartAndConnectAsync(client, server);

        await Task.WhenAll(Enumerable.Range(0, senders).Select(sender => Task.Run(() =>
        {
            for (var i = 0; i < perSender; i++)
            {
                Assert.True(client.SendMessage(new TestMessage { Number = sender, Text = new string((char)('a' + sender), 50) }));
            }
        })));

        await WaitUntilAsync(() => parser.Received.Count == senders * perSender);
        Assert.All(parser.Received, message => Assert.Equal(new string((char)('a' + message.Number), 50), message.Text));
    }

    [Fact]
    public async Task StateIsReplayedThenFollowsTheConnectionAndCompletesOnDispose()
    {
        using var server = StartServer(new RecordingParser());
        var client = NewClient(server.ListenerAddress);
        var states = client.State.ToList().ToTask();
        Assert.False(client.IsConnected);

        await StartAndConnectAsync(client, server);
        Assert.True(client.IsConnected);
        client.Dispose();

        Assert.Equal(
            [NeutronClientState.Disconnected, NeutronClientState.Connecting, NeutronClientState.Connected, NeutronClientState.Disconnected],
            await states.WaitAsync(Timeout));
        Assert.False(client.IsConnected);
        Assert.False(client.SendMessage(new TestMessage()));
        await client.Messages.DefaultIfEmpty().ToTask().WaitAsync(Timeout);
    }

    [Fact]
    public async Task ReconnectsWhenTheServerStopsAndStartsAgain()
    {
        var parser = new RecordingParser();
        using var server = StartServer(parser);
        using var client = NewClient(server.ListenerAddress);
        await StartAndConnectAsync(client, server);

        var lost = client.State.FirstAsync(state => state == NeutronClientState.Connecting).ToTask();
        server.Stop();
        await lost.WaitAsync(Timeout);
        Assert.False(client.SendMessage(new TestMessage { Number = 1 }));

        // The same server object, on the same port.
        var reconnected = client.State.FirstAsync(state => state == NeutronClientState.Connected).ToTask();
        var received = client.Messages.FirstAsync().ToTask();
        server.Start();
        await reconnected.WaitAsync(Timeout);
        await WaitUntilAsync(() => server.ConnectedClientCount == 1);

        Assert.True(client.SendMessage(new TestMessage { Number = 2 }));
        Assert.True(server.SendMessage(new TestMessage { Number = 3 }));
        Assert.Equal(3, (await received.WaitAsync(Timeout)).Number);
        await WaitUntilAsync(() => parser.Received.Count == 1);
        Assert.Equal(2, parser.Received.Single().Number);
    }

    [Fact]
    public async Task KeepsTryingUntilTheServerAppearsAndReportsEachFailure()
    {
        // Take a free port, then release it, so that nothing is listening when the client starts.
        using var placeholder = StartServer(new RecordingParser());
        var address = placeholder.ListenerAddress;
        placeholder.Stop();

        using var client = NewClient(address);
        var failures = client.Errors.Take(2).ToList().ToTask();
        client.Start();
        Assert.All(await failures.WaitAsync(Timeout), failure => Assert.IsType<SocketException>(failure));
        Assert.False(client.IsConnected);

        using var server = StartServer(new RecordingParser(), address);
        await client.State.FirstAsync(state => state == NeutronClientState.Connected).ToTask().WaitAsync(Timeout);
    }

    [Fact]
    public async Task ASilentConnectionIsReplacedAfterTheHeartBeatTimeout()
    {
        // A server that accepts and then says nothing, as one that has hung would.
        using var silent = new TcpListener(IPAddress.Loopback, 0);
        silent.Start();
        using var client = NewClient((IPEndPoint)silent.LocalEndpoint, heartBeatTimeout: TimeSpan.FromMilliseconds(200));
        var error = client.Errors.FirstAsync().ToTask();
        client.Start();

        using var first = await silent.AcceptSocketAsync().WaitAsync(Timeout);
        Assert.IsType<TimeoutException>(await error.WaitAsync(Timeout));
        using var second = await silent.AcceptSocketAsync().WaitAsync(Timeout);
    }

    [Fact]
    public async Task HeartBeatsKeepTheConnectionAlive()
    {
        using var server = StartServer(new RecordingParser(), heartBeat: new TestMessage { Text = "heartbeat" });
        using var client = NewClient(server.ListenerAddress, heartBeatTimeout: TimeSpan.FromSeconds(2.5));
        var errors = new ConcurrentQueue<Exception>();
        using var subscription = client.Errors.Subscribe(errors.Enqueue);
        var heartBeats = client.Messages.Take(6).ToList().ToTask();

        await StartAndConnectAsync(client, server);

        // Six one-second heartbeats span two full timeouts.
        await heartBeats.WaitAsync(Timeout);
        Assert.Empty(errors);
        Assert.True(client.IsConnected);
    }

    private static async Task StartAndConnectAsync(NeutronClient<TestMessage> client, NeutronAsyncServer<TestMessage> server)
    {
        var connected = client.State.FirstAsync(state => state == NeutronClientState.Connected).ToTask();
        client.Start();
        await connected.WaitAsync(Timeout);
        await WaitUntilAsync(() => server.ConnectedClientCount == 1);
    }

    private static NeutronClient<TestMessage> NewClient(IPEndPoint address, TimeSpan heartBeatTimeout = default)
    {
        return new NeutronClient<TestMessage>(new NeutronClientConfig
        {
            ServerAddress = address,
            ReconnectDelay = TimeSpan.FromMilliseconds(20),
            MaxReconnectDelay = TimeSpan.FromMilliseconds(100),
            HeartBeatTimeout = heartBeatTimeout,
        });
    }

    private static NeutronAsyncServer<TestMessage> StartServer(RecordingParser parser, IPEndPoint? address = null, TestMessage? heartBeat = null)
    {
        var server = new NeutronAsyncServer<TestMessage>(
            parser,
            new NeutronServerConfig<TestMessage>
            {
                ListenerAddress = address ?? new IPEndPoint(IPAddress.Loopback, 0),
                HeartBeatMsg = heartBeat,
                HeartBeatIntervalInSec = heartBeat == null ? 0 : 1,
            },
            NullLogger<NeutronAsyncServer<TestMessage>>.Instance);
        server.Start();
        return server;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met in time.");
            await Task.Delay(20);
        }
    }

    private sealed class RecordingParser : IServerProtoMessageParser<TestMessage>
    {
        public ConcurrentQueue<TestMessage> Received { get; } = new();

        public Task ReceivedMsgAsync(string connectionId, TestMessage message)
        {
            Received.Enqueue(message);
            return Task.CompletedTask;
        }
    }
}
