using LabEnhanced.Neutron.Client;
using Microsoft.Extensions.Logging.Abstractions;
using ProtoBuf;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Channels;
using Original = LabOriginal.Neutron;

namespace LabEnhanced.Neutron.Tests;

[ProtoContract]
public sealed class TestMessage : IBaseMessage, Original.IBaseMessage
{
    [ProtoMember(1)]
    public int AsyncId { get; set; }

    [ProtoMember(2)]
    public bool AsyncSend { get; set; }

    [ProtoMember(3)]
    public int Sequence { get; set; }

    [ProtoMember(4)]
    public byte[]? Payload { get; set; }
}

/// <summary>
/// Records what either end receives. As the server's parser it also echoes requests back, which is what makes
/// SendMessageAsync complete.
/// </summary>
public sealed class Recorder :
    IServerProtoMessageParser<TestMessage>,
    IClientProtoAsyncParser<TestMessage>,
    Original.IServerProtoMessageParser<TestMessage>,
    Original.Client.IClientProtoAsyncParser<TestMessage>
{
    private readonly Channel<TestMessage> _received = Channel.CreateUnbounded<TestMessage>();

    public Func<string, TestMessage, bool>? Reply { get; set; }

    public bool EchoRequests { get; set; } = true;

    public Task ReceivedMsgAsync(string connectionId, TestMessage message)
    {
        if (message.AsyncSend && EchoRequests)
        {
            Reply!(connectionId, message);
        }
        else
        {
            _received.Writer.TryWrite(message);
        }

        return Task.CompletedTask;
    }

    public Task ReceivedMsgAsync(TestMessage message)
    {
        _received.Writer.TryWrite(message);
        return Task.CompletedTask;
    }

    public async Task<TestMessage> NextAsync()
    {
        using var timeout = new CancellationTokenSource(Timeout);
        return await _received.Reader.ReadAsync(timeout.Token);
    }

    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
}

public sealed class EnhancedTests : IDisposable
{
    private readonly Recorder _serverSide = new();
    private readonly Recorder _clientSide = new();
    private readonly List<IDisposable> _disposables = [];

    public void Dispose() => _disposables.ForEach(disposable => disposable.Dispose());

    [Fact]
    public async Task Messages_arrive_in_order_in_both_directions()
    {
        var server = StartServer();
        var client = await ConnectAsync(server);

        for (var i = 0; i < 5000; i++)
        {
            Assert.True(client.SendMessage(new TestMessage { Sequence = i }));
        }

        for (var i = 0; i < 5000; i++)
        {
            Assert.Equal(i, (await _serverSide.NextAsync()).Sequence);
        }

        for (var i = 0; i < 5000; i++)
        {
            Assert.True(server.SendMessage(new TestMessage { Sequence = i }));
        }

        for (var i = 0; i < 5000; i++)
        {
            Assert.Equal(i, (await _clientSide.NextAsync()).Sequence);
        }
    }

    [Fact]
    public async Task Concurrent_senders_do_not_corrupt_the_stream_and_every_request_gets_its_own_reply()
    {
        var server = StartServer();
        var client = await ConnectAsync(server);

        var replies = await Task.WhenAll(Enumerable.Range(0, 8).Select(sender => Task.Run(async () =>
        {
            for (var i = 0; i < 500; i++)
            {
                var sequence = (sender * 1000) + i;
                Assert.True(client.SendMessage(new TestMessage { Sequence = -1 }));
                var reply = await client.SendMessageAsync(new TestMessage { Sequence = sequence, Payload = new byte[sequence % 300] });
                Assert.Equal(sequence, reply.Sequence);
            }

            return true;
        })));

        Assert.Equal(8, replies.Length);
        for (var i = 0; i < 4000; i++)
        {
            Assert.Equal(-1, (await _serverSide.NextAsync()).Sequence);
        }
    }

    [Fact]
    public async Task Large_messages_cross_in_both_directions()
    {
        var server = StartServer();
        var client = await ConnectAsync(server);
        var payload = new byte[3 * 1024 * 1024];
        Random.Shared.NextBytes(payload);

        client.SendMessage(new TestMessage { Payload = payload });
        Assert.Equal(payload, (await _serverSide.NextAsync()).Payload);

        server.SendMessage(new TestMessage { Payload = payload });
        Assert.Equal(payload, (await _clientSide.NextAsync()).Payload);
    }

    [Fact]
    public async Task A_request_is_cancelled_when_no_reply_arrives_in_time()
    {
        _serverSide.EchoRequests = false;
        var server = StartServer();
        var client = await ConnectAsync(server, asyncTimeout: 200);

        await Assert.ThrowsAsync<TaskCanceledException>(() => client.SendMessageAsync(new TestMessage()));
    }

    [Fact]
    public async Task A_request_fails_at_once_when_not_connected()
    {
        var client = NewClient(new IPEndPoint(IPAddress.Loopback, 1));

        await Assert.ThrowsAsync<IOException>(() => client.SendMessageAsync(new TestMessage()));
        Assert.False(client.SendMessage(new TestMessage()));
    }

    [Fact]
    public async Task Each_end_reports_a_connection_and_its_loss_exactly_once()
    {
        _serverSide.EchoRequests = false;
        var server = StartServer();
        var serverConnections = new List<IProtobuffClientConnection<TestMessage>>();
        var serverDisconnections = new List<IProtobuffClientConnection<TestMessage>>();
        server.OnClientConnection.Subscribe(serverConnections.Add);
        server.OnClientDisconnection.Subscribe(serverDisconnections.Add);

        var client = NewClient(server.ListenerAddress);
        var connected = 0;
        var disconnected = 0;
        client.Connected.Subscribe(_ => connected++);
        client.Disconnected.Subscribe(_ => Interlocked.Increment(ref disconnected));

        await client.ConnectAsync();
        Assert.True(client.IsConnected);
        Assert.Equal(server.ListenerAddress, client.ConnectedEndpoint);
        await WaitUntilAsync(() => server.ConnectedClientCount == 1);
        var onConnection = serverConnections.Single().OnClientDisconnection.ToTask();

        var pending = client.SendMessageAsync(new TestMessage());
        client.Disconnect();

        await Assert.ThrowsAsync<TaskCanceledException>(() => pending);
        await WaitUntilAsync(() => server.ConnectedClientCount == 0 && serverDisconnections.Count == 1 && Volatile.Read(ref disconnected) == 1);
        Assert.Same(serverConnections.Single(), await onConnection.WaitAsync(Recorder.Timeout));
        Assert.False(client.IsConnected);
        Assert.Null(client.ConnectedEndpoint);

        await Task.Delay(200);
        Assert.Equal((1, 1, 1, 1), (connected, disconnected, serverConnections.Count, serverDisconnections.Count));
    }

    [Fact]
    public async Task The_server_can_stop_and_start_again_and_the_client_can_reconnect()
    {
        var server = StartServer();
        var client = await ConnectAsync(server);
        var lost = client.Disconnected.FirstAsync().ToTask();

        server.Stop();
        await lost.WaitAsync(Recorder.Timeout);
        Assert.False(server.IsBound);

        server.Start();
        await client.ConnectAsync();
        Assert.True(client.IsConnected);
        Assert.Equal(7, (await client.SendMessageAsync(new TestMessage { Sequence = 7 })).Sequence);
    }

    [Fact]
    public async Task The_client_uses_the_first_end_point_that_accepts()
    {
        var server = StartServer();
        var client = NewClient(new IPEndPoint(IPAddress.Loopback, 1), server.ListenerAddress);

        await client.ConnectAsync();

        Assert.True(client.IsConnected);
        Assert.Null(client.ConnectionException);
        Assert.Equal(server.ListenerAddress, client.ConnectedEndpoint);
    }

    [Fact]
    public async Task A_failed_connect_is_reported_not_thrown()
    {
        var client = NewClient(new IPEndPoint(IPAddress.Loopback, 1));

        await client.ConnectAsync();

        Assert.False(client.IsConnected);
        Assert.IsType<SocketException>(client.ConnectionException, exactMatch: false);
    }

    [Fact]
    public async Task Heartbeats_flow_in_both_directions()
    {
        var server = StartServer(heartBeat: new TestMessage { Sequence = 111 });
        await ConnectAsync(server, heartBeat: new TestMessage { Sequence = 222 });

        Assert.Equal(111, (await _clientSide.NextAsync()).Sequence);
        Assert.Equal(222, (await _serverSide.NextAsync()).Sequence);
    }

    [Fact]
    public async Task A_message_over_the_size_limit_closes_the_connection()
    {
        var server = StartServer(configure: config => config.MaxMessageBytes = 1024);
        var client = await ConnectAsync(server);
        var lost = client.Disconnected.FirstAsync().ToTask();

        client.SendMessage(new TestMessage { Payload = new byte[4096] });

        await lost.WaitAsync(Recorder.Timeout);
        await WaitUntilAsync(() => server.ConnectedClientCount == 0);
    }

    [Fact]
    public async Task A_client_that_stops_reading_is_dropped_and_the_others_keep_receiving()
    {
        var server = StartServer(configure: config => config.MaxSendBacklogBytes = 256 * 1024);
        using var stuck = new Socket(SocketType.Stream, ProtocolType.Tcp);
        await stuck.ConnectAsync(server.ListenerAddress);
        var healthy = await ConnectAsync(server);
        await WaitUntilAsync(() => server.ConnectedClientCount == 2);

        var message = new TestMessage { Payload = new byte[64 * 1024] };
        for (var i = 0; server.ConnectedClientCount == 2; i++)
        {
            Assert.True(i < 10_000, "The stuck client was never dropped.");
            server.SendMessage(message);
            await _clientSide.NextAsync();
        }

        Assert.True(healthy.IsConnected);
        Assert.True(server.SendMessage(new TestMessage { Sequence = 5 }));
        Assert.Equal(5, (await _clientSide.NextAsync()).Sequence);
    }

    [Fact]
    public async Task A_start_failure_is_thrown_or_reported_as_configured()
    {
        var first = StartServer();

        var throwing = NewServer(first.ListenerAddress, handleStartException: false);
        Assert.ThrowsAny<SocketException>(throwing.Start);

        var reporting = NewServer(first.ListenerAddress, handleStartException: true);
        var error = reporting.OnServerError.FirstAsync().ToTask();
        reporting.Start();
        Assert.IsType<SocketException>(await error.WaitAsync(Recorder.Timeout), exactMatch: false);
        Assert.False(reporting.IsBound);
    }

    [Fact]
    public async Task An_original_client_works_against_the_enhanced_server()
    {
        var server = StartServer();
        var config = new Original.ProtobuffClientConfig<TestMessage>("original", [server.ListenerAddress], null!, 0);
        using var client = new Original.Client.ProtobufAsyncClient<TestMessage>(_clientSide, config, NullLogger.Instance);
        client.Connect();
        Assert.True(client.IsConnected);

        Assert.Equal(9, (await client.SendMessageAsync(new TestMessage { Sequence = 9 }, false)).Sequence);
        client.SendMessage(new TestMessage { Sequence = 10 });
        Assert.Equal(10, (await _serverSide.NextAsync()).Sequence);
        server.SendMessage(new TestMessage { Sequence = 11 });
        Assert.Equal(11, (await _clientSide.NextAsync()).Sequence);
    }

    [Fact]
    public async Task An_enhanced_client_works_against_the_original_server()
    {
        var address = new IPEndPoint(IPAddress.Loopback, FreePort());
        var config = new Original.ProtobuffServerConfig<TestMessage>(address, null!, 0, handleStartException: false);
        using var server = new Original.ProtobuffAsyncServer<TestMessage>(_serverSide, config, NullLogger<Original.ProtobuffAsyncServer<TestMessage>>.Instance);
        _serverSide.Reply = server.SendMessage;
        server.Start();

        var client = NewClient(address);
        await client.ConnectAsync();
        Assert.True(client.IsConnected);

        Assert.Equal(9, (await client.SendMessageAsync(new TestMessage { Sequence = 9 })).Sequence);
        client.SendMessage(new TestMessage { Sequence = 10 });
        Assert.Equal(10, (await _serverSide.NextAsync()).Sequence);
        server.SendMessage(new TestMessage { Sequence = 11 });
        Assert.Equal(11, (await _clientSide.NextAsync()).Sequence);
    }

    private ProtobuffAsyncServer<TestMessage> StartServer(TestMessage? heartBeat = null, Action<ProtobuffServerConfig<TestMessage>>? configure = null)
    {
        var server = NewServer(new IPEndPoint(IPAddress.Loopback, 0), handleStartException: false, heartBeat, configure);
        server.Start();
        return server;
    }

    private ProtobuffAsyncServer<TestMessage> NewServer(
        IPEndPoint address,
        bool handleStartException,
        TestMessage? heartBeat = null,
        Action<ProtobuffServerConfig<TestMessage>>? configure = null)
    {
        var config = new ProtobuffServerConfig<TestMessage>(address, heartBeat, heartBeat == null ? 0 : 1, handleStartException);
        configure?.Invoke(config);
        var server = new ProtobuffAsyncServer<TestMessage>(_serverSide, config, NullLogger<ProtobuffAsyncServer<TestMessage>>.Instance);
        _serverSide.Reply = server.SendMessage;
        _disposables.Add(server);
        return server;
    }

    private async Task<ProtobufAsyncClient<TestMessage>> ConnectAsync(ProtobuffAsyncServer<TestMessage> server, int asyncTimeout = 20_000, TestMessage? heartBeat = null)
    {
        var config = new ProtobuffClientConfig<TestMessage>("test", [server.ListenerAddress], heartBeat, heartBeat == null ? 0 : 1, asyncTimeout);
        var client = new ProtobufAsyncClient<TestMessage>(_clientSide, config, NullLogger.Instance);
        _disposables.Add(client);
        await client.ConnectAsync();
        Assert.True(client.IsConnected);
        return client;
    }

    private ProtobufAsyncClient<TestMessage> NewClient(params IPEndPoint[] endPoints)
    {
        var client = new ProtobufAsyncClient<TestMessage>(_clientSide, new ProtobuffClientConfig<TestMessage>("test", endPoints, null, 0), NullLogger.Instance);
        _disposables.Add(client);
        return client;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (var waited = TimeSpan.Zero; !condition(); waited += TimeSpan.FromMilliseconds(10))
        {
            Assert.True(waited < Recorder.Timeout, "Timed out waiting for a condition.");
            await Task.Delay(10);
        }
    }

    private static int FreePort()
    {
        using var probe = new Socket(SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)probe.LocalEndPoint!).Port;
    }
}
