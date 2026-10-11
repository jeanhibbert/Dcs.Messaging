using Dcs.Messaging.Common;
using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Grpc;
using Dcs.Messaging.Sbe;
using Dcs.Messaging.Tcp;
using Dcs.Messaging.Transport;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;

namespace Dcs.Messaging.PerformanceTests;

public enum TransportKind
{
    Tcp,
    Grpc,
    Sbe
}

public sealed record ThroughputResult(
    TransportKind Transport,
    int MessageCount,
    TimeSpan Elapsed,
    int PayloadBytes,
    int FrameBytes,
    long AllocatedBytes,
    int Gen0Collections)
{
    public double MessagesPerSecond => MessageCount / Elapsed.TotalSeconds;

    public double WireMegabytesPerSecond => (double)FrameBytes * MessageCount / Elapsed.TotalSeconds / (1024 * 1024);

    public double AllocatedBytesPerMessage => (double)AllocatedBytes / MessageCount;
}

/// <summary>
/// One server and one client in the same process, connected over loopback. After a handshake and a warm-up burst
/// the server pumps <c>messageCount</c> <see cref="ForwardPricingDto"/> messages through the public
/// <see cref="IMessagingService"/> API and the clock stops when the client has received and deserialized the last one.
/// Each transport uses its builder's default serializer: protobuf-net for TCP and gRPC, SBE for SBE.
/// </summary>
public static class ThroughputScenario
{
    private static readonly IEndpointDetails PricesEndpoint = new MessageEndpointDetails("Perf/Prices", EndpointType.Default);
    private static readonly IEndpointDetails ControlEndpoint = new MessageEndpointDetails("Perf/Control", EndpointType.Default);
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan PumpTimeout = TimeSpan.FromMinutes(3);

    /// <summary>
    /// Messages per run; override with the <c>DCS_PERF_MESSAGES</c> environment variable.
    /// </summary>
    public static int MessageCount => ReadSetting("DCS_PERF_MESSAGES", 100_000);

    /// <summary>
    /// Untimed messages sent first so JIT, connection and buffer setup are excluded; override with <c>DCS_PERF_WARMUP</c>.
    /// </summary>
    public static int WarmupCount => ReadSetting("DCS_PERF_WARMUP", 10_000);

    public static async Task<ThroughputResult> RunAsync(TransportKind transport, int messageCount, int warmupCount)
    {
        var port = GetFreePort();
        var endpoints = new TestEndpointDetailsProvider();
        var server = CreateServer(transport, port, endpoints);
        using var serverTransport = server.MessagingTransport;

        if (transport == TransportKind.Grpc)
        {
            // Kestrel starts asynchronously; the gRPC client does not retry a failed initial call.
            await WaitForListenerAsync(port);
        }

        var client = CreateClient(transport, port, endpoints);
        using var clientTransport = client.MessagingTransport;

        var clientReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverControl = await server.MessagingService.GetMessageStream(ControlEndpoint);
        using var controlSubscription = serverControl.Messages.Subscribe(_ => clientReady.TrySetResult());

        var received = 0;
        var checksum = 0d;
        var warmupReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clientPrices = await client.MessagingService.GetMessageStream(PricesEndpoint);
        using var pricesSubscription = clientPrices.Messages.Subscribe(message =>
        {
            var pricing = client.Serializer.Deserialize<ForwardPricingDto>(message.Payload);
            checksum += pricing.BidRate;

            var count = Interlocked.Increment(ref received);
            if (count == warmupCount)
            {
                warmupReceived.TrySetResult();
            }

            if (count == warmupCount + messageCount)
            {
                allReceived.TrySetResult();
            }
        });

        await client.MessagingService.Send(client.MessageFactory.Create(Array.Empty<byte>(), TimeSpan.Zero, false), ControlEndpoint);
        await WaitAsync(clientReady.Task, HandshakeTimeout, $"{transport}: server never received the client's ready message.");

        if (warmupCount > 0)
        {
            Pump(server, 0, warmupCount);
            await WaitAsync(warmupReceived.Task, PumpTimeout, $"{transport}: warm-up incomplete, received {Volatile.Read(ref received)} of {warmupCount}.");
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        var gen0Before = GC.CollectionCount(0);
        var stopwatch = Stopwatch.StartNew();

        Pump(server, warmupCount, messageCount);
        await WaitAsync(allReceived.Task, PumpTimeout, $"{transport}: received {Volatile.Read(ref received) - warmupCount} of {messageCount} messages.");

        stopwatch.Stop();
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var gen0 = GC.CollectionCount(0) - gen0Before;
        GC.KeepAlive(checksum);

        var sample = CreatePriceMessage(server, 0);
        return new ThroughputResult(
            transport,
            messageCount,
            stopwatch.Elapsed,
            sample.Payload.Length,
            GetFrameLength(transport, sample),
            allocated,
            gen0);
    }

    public static string FormatTable(IEnumerable<ThroughputResult> results)
    {
        var lines = new List<string>
        {
            "| Transport | Messages | Elapsed (ms) |     Msg/s | Wire MB/s | Payload B | Frame B | Alloc B/msg | Gen0 GCs |",
            "|-----------|---------:|-------------:|----------:|----------:|----------:|--------:|------------:|---------:|"
        };

        lines.AddRange(results.Select(r =>
            $"| {r.Transport,-9} | {r.MessageCount,8:N0} | {r.Elapsed.TotalMilliseconds,12:N0} | {r.MessagesPerSecond,9:N0} | {r.WireMegabytesPerSecond,9:N1} | {r.PayloadBytes,9} | {r.FrameBytes,7} | {r.AllocatedBytesPerMessage,11:N0} | {r.Gen0Collections,8} |"));

        return string.Join(Environment.NewLine, lines);
    }

    private static void Pump(IMessagingSessionBuilder server, int start, int count)
    {
        for (var i = start; i < start + count; i++)
        {
            server.MessagingService.Send(CreatePriceMessage(server, i), PricesEndpoint);
        }
    }

    private static IMessage CreatePriceMessage(IMessagingSessionBuilder server, int sequence)
    {
        var pricing = new ForwardPricingDto
        {
            CurrencyPair = new CurrencyPairDto { BaseCcy = "EUR", QuoteCcy = "USD", DecimalPlaces = 5 },
            BidRate = 1.08417 + sequence * 0.00001,
            AskRate = 1.08487 + sequence * 0.00001,
            IsContra = (sequence & 1) == 0
        };

        return server.MessageFactory.Create(server.Serializer.Serialize(pricing), TimeSpan.Zero, false);
    }

    /// <summary>
    /// Bytes one message occupies on the wire. For gRPC this is the protobuf envelope plus the 5-byte gRPC
    /// message prefix; HTTP/2 DATA frame headers (9 bytes, shared when messages are coalesced) are excluded.
    /// </summary>
    private static int GetFrameLength(TransportKind transport, IMessage message)
    {
        switch (transport)
        {
            case TransportKind.Sbe:
                return SbeTransportCodec.EncodeFrame(PricesEndpoint.Address, message, null!).Length;
            case TransportKind.Grpc:
                const int GrpcMessagePrefixLength = 5;
                const int TcpLengthPrefixLength = 4;
                return TcpFrameLength(message) - TcpLengthPrefixLength + GrpcMessagePrefixLength;
            default:
                return TcpFrameLength(message);
        }
    }

    private static int TcpFrameLength(IMessage message)
    {
        return TransportTcpFraming.SerializeLengthPrefixed(TransportEnvelopeFactory.FromMessage(PricesEndpoint.Address, message, null!)).Length;
    }

    private static IMessagingSessionBuilder CreateServer(TransportKind transport, int port, IEndpointDetailsProvider endpoints)
    {
        var sessionId = "perf-server-" + Guid.NewGuid().ToString("N");
        return transport switch
        {
            TransportKind.Tcp => new TcpMessagingSessionBuilder("PerfServer", endpoints, TcpSessionOptions.CreateServer("127.0.0.1", port, sessionId)),
            TransportKind.Grpc => new GrpcMessagingSessionBuilder("PerfServer", endpoints, GrpcSessionOptions.CreateServer($"http://127.0.0.1:{port}", sessionId)),
            TransportKind.Sbe => new SbeMessagingSessionBuilder("PerfServer", endpoints, SbeSessionOptions.CreateServer("127.0.0.1", port, sessionId)),
            _ => throw new ArgumentOutOfRangeException(nameof(transport))
        };
    }

    private static IMessagingSessionBuilder CreateClient(TransportKind transport, int port, IEndpointDetailsProvider endpoints)
    {
        var sessionId = "perf-client-" + Guid.NewGuid().ToString("N");
        return transport switch
        {
            TransportKind.Tcp => new TcpMessagingSessionBuilder("PerfClient", endpoints, TcpSessionOptions.CreateClient("127.0.0.1", port, sessionId)),
            TransportKind.Grpc => new GrpcMessagingSessionBuilder("PerfClient", endpoints, GrpcSessionOptions.CreateClient($"http://127.0.0.1:{port}", sessionId)),
            TransportKind.Sbe => new SbeMessagingSessionBuilder("PerfClient", endpoints, SbeSessionOptions.CreateClient("127.0.0.1", port, sessionId)),
            _ => throw new ArgumentOutOfRangeException(nameof(transport))
        };
    }

    private static async Task WaitAsync(Task task, TimeSpan timeout, string timeoutMessage)
    {
        try
        {
            await task.WaitAsync(timeout);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException(timeoutMessage);
        }
    }

    private static async Task WaitForListenerAsync(int port)
    {
        var deadline = DateTime.UtcNow + HandshakeTimeout;
        while (true)
        {
            try
            {
                using var probe = new TcpClient();
                await probe.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(50);
            }
        }
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static int ReadSetting(string name, int defaultValue)
    {
        return int.TryParse(Environment.GetEnvironmentVariable(name), out var value) && value >= 0 ? value : defaultValue;
    }
}
