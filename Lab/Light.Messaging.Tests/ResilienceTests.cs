using Microsoft.Extensions.Logging;

namespace Light.Messaging.Tests;

public sealed class ResilienceTests
{
    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task SlowConsumerUnderDropOldestStaysConnectedAndIsReported(Transport transport)
    {
        const int final = int.MaxValue;
        var logger = new ListLogger();
        var (server, client) = await Harness.StartAsync(transport, new MessagingServerOptions { SendQueueCapacity = 100, Logger = logger });
        await using var _ = server;
        await using var __ = client;

        using var release = new ManualResetEventSlim(false);
        var finalReceived = new TaskCompletionSource();
        using var subscription = client.Subscribe<TestMessage>(Endpoints.Events, message =>
        {
            release.Wait();
            if (message.Number == final)
            {
                finalReceived.TrySetResult();
            }
        });
        await Harness.BarrierAsync(client);
        var peer = server.Peers.Single();

        // The blocked handler stops the client reading; once the buffers and the 100-message queue fill, publishing
        // starts dropping the oldest messages.
        var payload = new byte[1024];
        await Harness.WaitUntilAsync(() =>
        {
            for (var i = 0; i < 1000; i++)
            {
                server.Publish(Endpoints.Events, new TestMessage { Number = 1, Blob = payload });
            }

            return peer.DroppedFrames > 0;
        });

        // One aggregated warning at the next watchdog check, naming the peer and the endpoint.
        await Harness.WaitUntilAsync(() => logger.Entries.Any(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains("Slow consumer") && entry.Message.Contains("Tests/Events")));

        release.Set();
        server.Publish(Endpoints.Events, new TestMessage { Number = final });
        await finalReceived.Task.WaitAsync(Harness.Timeout);

        Assert.True(peer.IsConnected);
        await Harness.BarrierAsync(client);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ClientReconnectsAndRestoresSubscriptionsWhenTheConnectionDrops(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport, clientOptions: ReconnectingClient());
        await using var _ = server;
        await using var __ = client;

        var received = new TaskCompletionSource<int>();
        using var subscription = client.Subscribe<TestMessage>(Endpoints.Events, message => received.TrySetResult(message.Number));
        await Harness.BarrierAsync(client);

        var disconnected = new TaskCompletionSource<string>();
        var reconnected = new TaskCompletionSource<Peer>();
        client.Disconnected += reason => disconnected.TrySetResult(reason);
        client.Reconnected += peer => reconnected.TrySetResult(peer);

        server.Peers.Single().Connection.Close("killed by the test");
        await disconnected.Task.WaitAsync(Harness.Timeout);
        await reconnected.Task.WaitAsync(Harness.Timeout);

        // The barrier returns only after the server has processed the restored subscription.
        await Harness.BarrierAsync(client);
        server.Publish(Endpoints.Events, new TestMessage { Number = 7 });

        Assert.Equal(7, await received.Task.WaitAsync(Harness.Timeout));
        Assert.True(client.IsConnected);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ClientFailsFastWhileDownAndReconnectsWhenTheServerReturns(Transport transport)
    {
        var port = Harness.FreePort();
        var logger = new ListLogger();
        var server = await Harness.StartServerAsync(transport, port);
        await using var client = await MessagingClient.ConnectAsync(Harness.ClientTransport(transport, port), ReconnectingClient(logger));

        var received = new TaskCompletionSource<int>();
        using var subscription = client.Subscribe<TestMessage>(Endpoints.Events, message => received.TrySetResult(message.Number));
        await Harness.BarrierAsync(client);

        var disconnected = new TaskCompletionSource<string>();
        var reconnected = new TaskCompletionSource<Peer>();
        client.Disconnected += reason => disconnected.TrySetResult(reason);
        client.Reconnected += peer => reconnected.TrySetResult(peer);

        await server.DisposeAsync();
        Assert.Equal("server shutting down", await disconnected.Task.WaitAsync(Harness.Timeout));

        // Down: requests and publishes are refused at once rather than queued.
        var refused = await Assert.ThrowsAsync<MessagingException>(async () => await client.RequestAsync<TestMessage, TestMessage>(Endpoints.Ping, new TestMessage()));
        Assert.Contains("reconnecting", refused.Message);
        Assert.Throws<MessagingException>(() => client.Publish(Endpoints.FromClient, new TestMessage()));
        await Harness.WaitUntilAsync(() => logger.Entries.Any(entry => entry.Level == LogLevel.Warning && entry.Message.Contains("Reconnect attempt")));

        await using var restarted = await Harness.StartServerAsync(transport, port);
        await reconnected.Task.WaitAsync(Harness.Timeout);
        await Harness.BarrierAsync(client);
        restarted.Publish(Endpoints.Events, new TestMessage { Number = 9 });

        Assert.Equal(9, await received.Task.WaitAsync(Harness.Timeout));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Message.Contains("restored 1 subscription"));
    }

    private static MessagingClientOptions ReconnectingClient(ILogger? logger = null)
    {
        return new MessagingClientOptions
        {
            Name = "test-client",
            AutoReconnect = true,
            ReconnectMinDelay = TimeSpan.FromMilliseconds(20),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(200),
            Logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
        };
    }
}
