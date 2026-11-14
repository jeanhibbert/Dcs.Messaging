using Microsoft.Extensions.Logging.Abstractions;
using ProtoBuf;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace LabLegacy.Neutron.Tests;

[ProtoContract]
public sealed class TestMessage : IBaseMessage
{
    [ProtoMember(1)]
    public int Number { get; set; }

    [ProtoMember(2)]
    public string? Text { get; set; }

    [ProtoMember(3)]
    public byte[]? Blob { get; set; }
}

/// <summary>
/// The client side of every test is the legacy wire code (protobuf-net's length-prefixed stream calls), so these tests
/// also prove that existing clients work unchanged against the modernised server.
/// </summary>
public sealed class NeutronAsyncServerTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task MessagesFromALegacyClientArriveInOrderWithTheConnectionId()
    {
        var parser = new RecordingParser();
        using var server = StartServer(parser);
        using var client = await LegacyClient.ConnectAsync(server);

        for (var i = 0; i < 1000; i++)
        {
            client.Send(new TestMessage { Number = i, Text = "from client" });
        }

        await WaitUntilAsync(() => parser.Received.Count == 1000);
        Assert.Equal(Enumerable.Range(0, 1000), parser.Received.Select(received => received.Message.Number));
        Assert.All(parser.Received, received => Assert.Equal(server.ClientConnections.Keys.Single(), received.ConnectionId));
    }

    [Fact]
    public async Task BroadcastReachesEveryClientAndTargetedSendReachesOne()
    {
        using var server = StartServer(new RecordingParser());
        using var first = await LegacyClient.ConnectAsync(server);
        var firstId = server.ClientConnections.Keys.Single();
        using var second = await LegacyClient.ConnectAsync(server);

        Assert.True(server.SendMessage(new TestMessage { Number = 1, Text = "all" }));
        Assert.True(server.SendMessage(firstId, new TestMessage { Number = 2, Text = "first only" }));
        Assert.True(server.SendMessage(new TestMessage { Number = 3, Text = "all" }));
        Assert.False(server.SendMessage("unknown-id", new TestMessage()));

        Assert.Equal([1, 2, 3], new[] { first.Receive().Number, first.Receive().Number, first.Receive().Number });
        Assert.Equal([1, 3], new[] { second.Receive().Number, second.Receive().Number });
    }

    [Fact]
    public async Task ConcurrentSendersDoNotCorruptTheStream()
    {
        // The legacy connection wrote to the stream from whichever thread called SendMessage, so concurrent senders
        // could interleave bytes. Now every message must arrive whole.
        const int senders = 8;
        const int perSender = 2_000;
        using var server = StartServer(new RecordingParser());
        using var client = await LegacyClient.ConnectAsync(server);

        var sending = Task.WhenAll(Enumerable.Range(0, senders).Select(sender => Task.Run(() =>
        {
            for (var i = 0; i < perSender; i++)
            {
                Assert.True(server.SendMessage(new TestMessage { Number = sender, Text = new string((char)('a' + sender), 50) }));
            }
        })));

        var counts = new int[senders];
        for (var i = 0; i < senders * perSender; i++)
        {
            var message = client.Receive();
            Assert.Equal(new string((char)('a' + message.Number), 50), message.Text);
            counts[message.Number]++;
        }

        await sending;
        Assert.All(counts, count => Assert.Equal(perSender, count));
    }

    [Fact]
    public async Task MessagesLargerThanTheBuffersRoundTrip()
    {
        var parser = new RecordingParser();
        using var server = StartServer(parser);
        using var client = await LegacyClient.ConnectAsync(server);
        var blob = new byte[3 * 1024 * 1024];
        Random.Shared.NextBytes(blob);

        client.Send(new TestMessage { Number = 1, Blob = blob });
        await WaitUntilAsync(() => parser.Received.Count == 1);
        Assert.Equal(blob, parser.Received.Single().Message.Blob);

        // Read on another thread: the message is bigger than the socket buffers.
        var received = Task.Run(client.Receive);
        Assert.True(server.SendMessage(new TestMessage { Number = 2, Blob = blob }));
        Assert.Equal(blob, (await received.WaitAsync(Timeout)).Blob);
    }

    [Fact]
    public async Task HeartBeatsAreSentToEveryClient()
    {
        using var server = StartServer(new RecordingParser(), heartBeat: new TestMessage { Text = "heartbeat" }, heartBeatIntervalInSec: 1);
        using var first = await LegacyClient.ConnectAsync(server);
        using var second = await LegacyClient.ConnectAsync(server);

        Assert.Equal("heartbeat", (await Task.Run(first.Receive).WaitAsync(Timeout)).Text);
        Assert.Equal("heartbeat", (await Task.Run(second.Receive).WaitAsync(Timeout)).Text);
    }

    [Fact]
    public async Task ClientDisconnectionIsReportedAndTheConnectionRemoved()
    {
        using var server = StartServer(new RecordingParser());
        var connected = new TaskCompletionSource<INeutronClientConnection<TestMessage>>();
        var disconnected = new TaskCompletionSource<INeutronClientConnection<TestMessage>>();
        server.OnClientConnection += (_, connection) => connected.TrySetResult(connection);
        server.OnClientDisconnection += (_, connection) => disconnected.TrySetResult(connection);

        var client = await LegacyClient.ConnectAsync(server);
        var connection = await connected.Task.WaitAsync(Timeout);
        Assert.True(connection.IsConnected);

        client.Dispose();

        Assert.Same(connection, await disconnected.Task.WaitAsync(Timeout));
        Assert.False(connection.IsConnected);
        Assert.Equal(0, server.ConnectedClientCount);
        Assert.False(connection.SendMessage(new TestMessage()));
    }

    [Fact]
    public async Task ASlowClientIsDisconnectedWithoutHoldingUpTheOthers()
    {
        using var server = StartServer(new RecordingParser(), sendQueueCapacity: 100);
        using var slow = await LegacyClient.ConnectAsync(server);
        var slowId = server.ClientConnections.Keys.Single();
        using var healthy = await LegacyClient.ConnectAsync(server);

        var healthyReceived = 0;
        var reading = Task.Run(() =>
        {
            while (true)
            {
                healthy.Receive();
                Interlocked.Increment(ref healthyReceived);
            }
        });

        // The slow client never reads. Once the socket buffers and its 100-message queue fill, it is dropped; the
        // broadcast never blocks, and the healthy client keeps receiving throughout.
        // Sent in bursts of 50, each at the pace the healthy client reads, so only the slow one falls behind.
        var payload = new byte[4096];
        var sent = 0;
        await WaitUntilAsync(() =>
        {
            for (var burst = 0; burst < 200 && server.ClientConnections.ContainsKey(slowId); burst++)
            {
                for (var i = 0; i < 50; i++)
                {
                    server.SendMessage(new TestMessage { Blob = payload });
                }

                sent += 50;
                SpinWait.SpinUntil(() => Volatile.Read(ref healthyReceived) == sent, Timeout);
            }

            return !server.ClientConnections.ContainsKey(slowId);
        });

        Assert.Equal(1, server.ConnectedClientCount);
        Assert.True(server.SendMessage(new TestMessage { Number = 1 }));
        await WaitUntilAsync(() => Volatile.Read(ref healthyReceived) == sent + 1);
    }

    [Fact]
    public async Task StopDisconnectsClientsAndReleasesThePort()
    {
        using var server = StartServer(new RecordingParser());
        using var client = await LegacyClient.ConnectAsync(server);
        Assert.True(server.IsBound);

        server.Stop();

        Assert.False(server.IsBound);
        await WaitUntilAsync(() => server.ConnectedClientCount == 0);
        Assert.Null(await Task.Run(client.TryReceive).WaitAsync(Timeout));
    }

    [Fact]
    public void StartFailureIsThrownOrReportedAsConfigured()
    {
        using var occupying = StartServer(new RecordingParser());

        using var throwing = NewServer(occupying.ListenerAddress, handleStartException: false);
        Assert.Throws<SocketException>(throwing.Start);

        using var reporting = NewServer(occupying.ListenerAddress, handleStartException: true);
        Exception? reported = null;
        reporting.OnServerError += (_, exception) => reported = exception;
        reporting.Start();
        Assert.IsType<SocketException>(reported);
    }

    private static NeutronAsyncServer<TestMessage> StartServer(
        RecordingParser parser,
        TestMessage? heartBeat = null,
        int heartBeatIntervalInSec = 0,
        int sendQueueCapacity = 10_000)
    {
        var server = new NeutronAsyncServer<TestMessage>(
            parser,
            new NeutronServerConfig<TestMessage>
            {
                ListenerAddress = new IPEndPoint(IPAddress.Loopback, 0),
                HeartBeatMsg = heartBeat,
                HeartBeatIntervalInSec = heartBeatIntervalInSec,
                SendQueueCapacity = sendQueueCapacity,
            },
            NullLogger<NeutronAsyncServer<TestMessage>>.Instance);
        server.Start();
        return server;
    }

    private static NeutronAsyncServer<TestMessage> NewServer(IPEndPoint address, bool handleStartException)
    {
        return new NeutronAsyncServer<TestMessage>(
            new RecordingParser(),
            new NeutronServerConfig<TestMessage> { ListenerAddress = address, HandleStartException = handleStartException },
            NullLogger<NeutronAsyncServer<TestMessage>>.Instance);
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
        public ConcurrentQueue<(string ConnectionId, TestMessage Message)> Received { get; } = new();

        public Task ReceivedMsgAsync(string connectionId, TestMessage message)
        {
            Received.Enqueue((connectionId, message));
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Exactly what the legacy clients do on the wire.
    /// </summary>
    private sealed class LegacyClient : IDisposable
    {
        private readonly TcpClient _tcpClient;

        private LegacyClient(TcpClient tcpClient)
        {
            _tcpClient = tcpClient;
        }

        public static async Task<LegacyClient> ConnectAsync(NeutronAsyncServer<TestMessage> server)
        {
            var expected = server.ConnectedClientCount + 1;
            var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync(server.ListenerAddress);
            await WaitUntilAsync(() => server.ConnectedClientCount == expected);
            return new LegacyClient(tcpClient);
        }

        public void Send(TestMessage message)
        {
            Serializer.SerializeWithLengthPrefix(_tcpClient.GetStream(), message, PrefixStyle.Base128);
        }

        public TestMessage Receive()
        {
            return TryReceive() ?? throw new EndOfStreamException("The server closed the connection.");
        }

        public TestMessage? TryReceive()
        {
            try
            {
                return Serializer.DeserializeWithLengthPrefix<TestMessage>(_tcpClient.GetStream(), PrefixStyle.Base128);
            }
            catch (IOException)
            {
                return null;
            }
        }

        public void Dispose()
        {
            _tcpClient.Dispose();
        }
    }
}
