using Microsoft.Extensions.Logging.Abstractions;
using ProtoBuf;
using System.Net;
using System.Net.Sockets;
using Enhanced = LabEnhanced.Neutron;
using Original = LabOriginal.Neutron;

namespace LabEnhanced.Benchmarks;

/// <summary>
/// A small market-data style message, about 40 bytes on the wire. One class serves both frameworks.
/// </summary>
[ProtoContract]
public sealed class Tick : Original.IBaseMessage, Enhanced.IBaseMessage
{
    [ProtoMember(1)]
    public int AsyncId { get; set; }

    [ProtoMember(2)]
    public bool AsyncSend { get; set; }

    [ProtoMember(3)]
    public long Sequence { get; set; }

    [ProtoMember(4)]
    public string Symbol { get; set; } = "EURUSD";

    [ProtoMember(5)]
    public double Bid { get; set; } = 1.08547;

    [ProtoMember(6)]
    public double Ask { get; set; } = 1.08552;

    [ProtoMember(7)]
    public long Timestamp { get; set; } = 638_000_000_000_000_000;
}

/// <summary>
/// One server with its clients connected over loopback. The server echoes requests back to their sender and counts
/// everything else; the clients count everything that is not a reply.
/// </summary>
public interface IRig : IDisposable
{
    Task StartAsync(int clients, Action received);

    /// <summary>One-way, first client to server.</summary>
    void Send(Tick tick);

    /// <summary>Server to every client.</summary>
    void Broadcast(Tick tick);

    /// <summary>First client to server and back.</summary>
    Task<Tick> RequestAsync(Tick tick);
}

public sealed class OriginalRig : IRig, Original.IServerProtoMessageParser<Tick>, Original.Client.IClientProtoAsyncParser<Tick>
{
    private readonly List<Original.Client.ProtobufAsyncClient<Tick>> _clients = [];
    private readonly Lock _requestGate = new();
    private Original.ProtobuffAsyncServer<Tick> _server = null!;
    private Action _received = null!;

    public async Task StartAsync(int clients, Action received)
    {
        _received = received;

        // The original cannot report the port it bound, so a free one is found first.
        int port;
        using (var probe = new Socket(SocketType.Stream, ProtocolType.Tcp))
        {
            probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            port = ((IPEndPoint)probe.LocalEndPoint!).Port;
        }

        var address = new IPEndPoint(IPAddress.Loopback, port);
        _server = new Original.ProtobuffAsyncServer<Tick>(
            this,
            new Original.ProtobuffServerConfig<Tick>(address, null!, 0, handleStartException: false),
            NullLogger<Original.ProtobuffAsyncServer<Tick>>.Instance);
        _server.Start();

        for (var i = 0; i < clients; i++)
        {
            var client = new Original.Client.ProtobufAsyncClient<Tick>(this, new Original.ProtobuffClientConfig<Tick>($"bench{i}", [address], null!, 0), NullLogger.Instance);
            client.Connect();
            _clients.Add(client);
        }

        while (_server.ConnectedClientCount < clients)
        {
            await Task.Delay(10);
        }
    }

    public void Send(Tick tick) => _clients[0].SendMessage(tick);

    public void Broadcast(Tick tick) => _server.SendMessage(tick);

    public Task<Tick> RequestAsync(Tick tick)
    {
        // The original writes to the socket on the caller's thread with no lock, so concurrent callers corrupt the
        // stream. A caller has to serialize its own sends; the write happens before the first await, so this does it.
        lock (_requestGate)
        {
            return _clients[0].SendMessageAsync(tick, continueOnCapturedContext: false);
        }
    }

    public Task ReceivedMsgAsync(string connectionId, Tick message)
    {
        if (message.AsyncSend)
        {
            _server.SendMessage(connectionId, message);
        }
        else
        {
            _received();
        }

        return Task.CompletedTask;
    }

    public Task ReceivedMsgAsync(Tick message)
    {
        _received();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _clients.ForEach(client => client.Dispose());
        _server.Dispose();
    }
}

public sealed class EnhancedRig : IRig, Enhanced.IServerProtoMessageParser<Tick>, Enhanced.Client.IClientProtoAsyncParser<Tick>
{
    // Sending never blocks in the enhanced framework, so a benchmark that sends flat out needs room to queue.
    private const int Backlog = int.MaxValue;

    private readonly List<Enhanced.Client.ProtobufAsyncClient<Tick>> _clients = [];
    private Enhanced.ProtobuffAsyncServer<Tick> _server = null!;
    private Action _received = null!;

    public async Task StartAsync(int clients, Action received)
    {
        _received = received;
        _server = new Enhanced.ProtobuffAsyncServer<Tick>(
            this,
            new Enhanced.ProtobuffServerConfig<Tick>(new IPEndPoint(IPAddress.Loopback, 0), null, 0, handleStartException: false) { MaxSendBacklogBytes = Backlog },
            NullLogger<Enhanced.ProtobuffAsyncServer<Tick>>.Instance);
        _server.Start();

        for (var i = 0; i < clients; i++)
        {
            var config = new Enhanced.ProtobuffClientConfig<Tick>($"bench{i}", [_server.ListenerAddress], null, 0) { MaxSendBacklogBytes = Backlog };
            var client = new Enhanced.Client.ProtobufAsyncClient<Tick>(this, config, NullLogger.Instance);
            await client.ConnectAsync();
            _clients.Add(client);
        }

        while (_server.ConnectedClientCount < clients)
        {
            await Task.Delay(10);
        }
    }

    public void Send(Tick tick) => _clients[0].SendMessage(tick);

    public void Broadcast(Tick tick) => _server.SendMessage(tick);

    public Task<Tick> RequestAsync(Tick tick) => _clients[0].SendMessageAsync(tick, continueOnCapturedContext: false);

    public Task ReceivedMsgAsync(string connectionId, Tick message)
    {
        if (message.AsyncSend)
        {
            _server.SendMessage(connectionId, message);
        }
        else
        {
            _received();
        }

        return Task.CompletedTask;
    }

    public Task ReceivedMsgAsync(Tick message)
    {
        _received();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _clients.ForEach(client => client.Dispose());
        _server.Dispose();
    }
}
