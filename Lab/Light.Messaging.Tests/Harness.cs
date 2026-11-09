using Light.Messaging.Grpc;
using Light.Messaging.Tcp;
using ProtoBuf;
using System.Buffers;
using System.Net;
using System.Net.Sockets;

namespace Light.Messaging.Tests;

public enum Transport
{
    Tcp,
    Grpc
}

[ProtoContract]
public sealed class TestMessage
{
    [ProtoMember(1)]
    public int Number { get; set; }

    [ProtoMember(2)]
    public string? Text { get; set; }

    [ProtoMember(3)]
    public byte[]? Blob { get; set; }
}

internal static class Endpoints
{
    public static readonly Endpoint Ping = new("Tests/Ping");
    public static readonly Endpoint Events = new("Tests/Events");
    public static readonly Endpoint FromClient = new("Tests/FromClient");
    public static readonly Endpoint Echo = new("Tests/Echo");
    public static readonly Endpoint Fail = new("Tests/Fail");
    public static readonly Endpoint Slow = new("Tests/Slow");
    public static readonly Endpoint Missing = new("Tests/Missing");
}

internal static class Harness
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Reconnect is off unless a test asks for it, so a client whose server shuts down completes instead of retrying.
    /// </summary>
    public static async Task<(MessagingServer Server, MessagingClient Client)> StartAsync(
        Transport transport,
        MessagingServerOptions? serverOptions = null,
        MessagingClientOptions? clientOptions = null)
    {
        var port = FreePort();
        var server = await StartServerAsync(transport, port, serverOptions);
        var client = await MessagingClient.ConnectAsync(
            ClientTransport(transport, port),
            clientOptions ?? new MessagingClientOptions { Name = "test-client", AutoReconnect = false });
        return (server, client);
    }

    public static async Task<MessagingServer> StartServerAsync(Transport transport, int port, MessagingServerOptions? options = null)
    {
        ServerTransport serverTransport = transport == Transport.Tcp
            ? new TcpServerTransport("127.0.0.1", port)
            : new GrpcServerTransport("127.0.0.1", port);

        var server = await MessagingServer.StartAsync(serverTransport, options);
        server.Respond<TestMessage, TestMessage>(Endpoints.Ping, (message, _) => message);
        return server;
    }

    public static ClientTransport ClientTransport(Transport transport, int port)
    {
        return transport == Transport.Tcp
            ? new TcpClientTransport("127.0.0.1", port)
            : new GrpcClientTransport("127.0.0.1", port);
    }

    /// <summary>
    /// A round trip on the client's connection. Frames are processed in order, so when it returns the server has
    /// handled everything the client sent before it, including subscriptions.
    /// </summary>
    public static async Task BarrierAsync(MessagingClient client)
    {
        await client.RequestAsync<TestMessage, TestMessage>(Endpoints.Ping, new TestMessage());
    }

    public static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Condition not met in time.");
            }

            await Task.Delay(20);
        }
    }

    public static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>
/// Keeps every log entry so tests can assert on warnings.
/// </summary>
internal sealed class ListLogger : Microsoft.Extensions.Logging.ILogger
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> _entries = new();

    public IReadOnlyCollection<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> Entries => _entries;

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

    public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        _entries.Enqueue((logLevel, formatter(state, exception)));
    }
}

/// <summary>
/// Counts serializations of <see cref="TestMessage"/>, to prove when publishing does and does not serialize.
/// </summary>
internal sealed class CountingSerializer : IMessageSerializer
{
    private int _testMessages;

    public int TestMessagesSerialized => Volatile.Read(ref _testMessages);

    public void Serialize<T>(T message, IBufferWriter<byte> destination)
    {
        if (message is TestMessage { Number: >= 0 })
        {
            Interlocked.Increment(ref _testMessages);
        }

        ProtobufNetSerializer.Instance.Serialize(message, destination);
    }

    public T Deserialize<T>(ReadOnlySpan<byte> source) => ProtobufNetSerializer.Instance.Deserialize<T>(source);

    public void Prepare<T>() => ProtobufNetSerializer.Instance.Prepare<T>();
}
