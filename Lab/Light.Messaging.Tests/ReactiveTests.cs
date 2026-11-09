using Microsoft.Reactive.Testing;

namespace Light.Messaging.Tests;

/// <summary>
/// Tests written with Rx: the framework's observable streams are queried with Rx operators, and time-based behaviour
/// runs on a <see cref="TestScheduler"/>, so a 30-second timeout is checked without waiting 30 seconds.
/// </summary>
public sealed class ReactiveTests
{
    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ObserveComposesWithRxOperators(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;

        var evens = client.Observe<TestMessage>(Endpoints.Events)
            .Select(message => message.Number)
            .Where(number => number % 2 == 0)
            .Take(5)
            .ToArray()
            .ToTask();
        await Harness.BarrierAsync(client);

        for (var i = 0; i < 10; i++)
        {
            server.Publish(Endpoints.Events, new TestMessage { Number = i });
        }

        var received = await evens.WaitAsync(Harness.Timeout);
        Assert.Equal([0, 2, 4, 6, 8], received);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task EndingAnRxSubscriptionUnsubscribesOnTheServer(Transport transport)
    {
        var serializer = new CountingSerializer();
        var (server, client) = await Harness.StartAsync(transport, new MessagingServerOptions { Serializer = serializer });
        await using var _ = server;
        await using var __ = client;

        var first = client.Observe<TestMessage>(Endpoints.Events).FirstAsync().ToTask();
        await Harness.BarrierAsync(client);
        server.Publish(Endpoints.Events, new TestMessage { Number = 1 });
        Assert.Equal(1, (await first.WaitAsync(Harness.Timeout)).Number);

        // FirstAsync disposed its subscription, which told the server: publishing soon costs nothing again.
        await Harness.WaitUntilAsync(() =>
        {
            var before = serializer.TestMessagesSerialized;
            server.Publish(Endpoints.Events, new TestMessage { Number = 2 });
            return serializer.TestMessagesSerialized == before;
        });
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ObserveCompletesWhenTheSessionIsDisposed(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;

        var messages = client.Observe<TestMessage>(Endpoints.Events).ToList().ToTask();
        await client.DisposeAsync();

        Assert.Empty(await messages.WaitAsync(Harness.Timeout));
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ServerObservesManyClientsOneMessageAtATime(Transport transport)
    {
        const int perClient = 2_000;
        var port = Harness.FreePort();
        await using var server = await Harness.StartServerAsync(transport, port);
        var clients = await Task.WhenAll(Enumerable.Range(1, 3).Select(i => MessagingClient.ConnectAsync(
            Harness.ClientTransport(transport, port),
            new MessagingClientOptions { Name = $"client-{i}", AutoReconnect = false })));

        try
        {
            // Three receive loops deliver at once; Rx's contract still holds: OnNext calls never overlap.
            var inside = 0;
            var overlapped = false;
            var senders = server.ObserveWithSender<TestMessage>(Endpoints.FromClient)
                .Do(_ =>
                {
                    overlapped |= Interlocked.Increment(ref inside) > 1;
                    Thread.SpinWait(50);
                    Interlocked.Decrement(ref inside);
                })
                .Select(received => received.Sender.Name)
                .Take(3 * perClient)
                .ToList()
                .ToTask();

            await Task.WhenAll(clients.Select(client => Task.Run(() =>
            {
                for (var i = 0; i < perClient; i++)
                {
                    client.Publish(Endpoints.FromClient, new TestMessage { Number = i });
                }
            })));

            var names = await senders.WaitAsync(Harness.Timeout);
            Assert.False(overlapped);
            Assert.All(names.GroupBy(name => name), group => Assert.Equal(perClient, group.Count()));
        }
        finally
        {
            foreach (var client in clients)
            {
                await client.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task PeerStreamsReportClientsComingAndGoing()
    {
        var port = Harness.FreePort();
        await using var server = await Harness.StartServerAsync(Transport.Tcp, port);
        var connected = server.PeerConnected.Select(peer => peer.Name).FirstAsync().ToTask();
        var disconnected = server.PeerDisconnected.Select(peer => peer.CloseReason).FirstAsync().ToTask();

        var client = await MessagingClient.ConnectAsync(Harness.ClientTransport(Transport.Tcp, port), new MessagingClientOptions { Name = "observed", AutoReconnect = false });
        Assert.Equal("observed", await connected.WaitAsync(Harness.Timeout));

        await client.DisposeAsync();
        Assert.Equal("client disposed", await disconnected.WaitAsync(Harness.Timeout));
    }

    [Fact]
    public async Task StatusStreamFollowsAReconnect()
    {
        var (server, client) = await Harness.StartAsync(Transport.Tcp, clientOptions: Harness.ReconnectingClient());
        await using var _ = server;
        await using var __ = client;

        var states = client.Status.Select(status => status.State).Take(3).ToArray().ToTask();
        server.Peers.Single().Connection.Close("killed by the test");

        var observed = await states.WaitAsync(Harness.Timeout);
        Assert.Equal([ConnectionState.Connected, ConnectionState.Reconnecting, ConnectionState.Connected], observed);
    }

    [Fact]
    public async Task StatusStreamEndsWithClosedWhenDisposed()
    {
        var (server, client) = await Harness.StartAsync(Transport.Tcp);
        await using var _ = server;

        var states = client.Status.Select(status => status.State).ToArray().ToTask();
        await client.DisposeAsync();

        var observed = await states.WaitAsync(Harness.Timeout);
        Assert.Equal([ConnectionState.Connected, ConnectionState.Closed], observed);
    }

    [Fact]
    public async Task RequestTimeoutRunsInVirtualTime()
    {
        var scheduler = new TestScheduler();
        var (server, client) = await Harness.StartAsync(
            Transport.Tcp,
            clientOptions: new MessagingClientOptions { Name = "test-client", AutoReconnect = false, Scheduler = scheduler });
        await using var _ = server;
        await using var __ = client;

        var never = new TaskCompletionSource<TestMessage>();
        using var responder = server.RespondAsync<TestMessage, TestMessage>(Endpoints.Slow, (_, _) => new ValueTask<TestMessage>(never.Task));
        var response = client.RequestAsync<TestMessage, TestMessage>(Endpoints.Slow, new TestMessage(), TimeSpan.FromSeconds(30)).AsTask();

        scheduler.AdvanceBy(TimeSpan.FromSeconds(29).Ticks);
        Assert.False(response.IsCompleted);

        scheduler.AdvanceBy(TimeSpan.FromSeconds(2).Ticks);
        await Assert.ThrowsAsync<TimeoutException>(() => response.WaitAsync(Harness.Timeout));
    }

    [Fact]
    public async Task SlowConsumerReportArrivesOnTheWatchdogTick()
    {
        var scheduler = new TestScheduler();
        var (server, client) = await Harness.StartAsync(Transport.Tcp, new MessagingServerOptions { SendQueueCapacity = 100, Scheduler = scheduler });
        await using var _ = server;
        await using var __ = client;

        using var release = new ManualResetEventSlim(false);
        using var subscription = client.Subscribe<TestMessage>(Endpoints.Events, _ => release.Wait());
        await Harness.BarrierAsync(client);
        var peer = server.Peers.Single();
        var report = server.SlowConsumers.FirstAsync().ToTask();

        var payload = new byte[1024];
        await Harness.WaitUntilAsync(() =>
        {
            for (var i = 0; i < 1000; i++)
            {
                server.Publish(Endpoints.Events, new TestMessage { Number = 1, Blob = payload });
            }

            return peer.DroppedFrames > 0;
        });

        // Drops are only counted until the watchdog runs; one tick of virtual time produces the report.
        Assert.False(report.IsCompleted);
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        var dropped = await report.WaitAsync(Harness.Timeout);
        release.Set();

        Assert.Equal("test-client", dropped.Peer.Name);
        Assert.Equal("Tests/Events", dropped.LastEndpoint);
        Assert.True(dropped.Dropped > 0);
        Assert.True(dropped.TotalDropped >= dropped.Dropped);
    }

    [Fact]
    public async Task RequestObservableIsColdSoRetrySendsAgain()
    {
        var (server, client) = await Harness.StartAsync(Transport.Tcp);
        await using var _ = server;
        await using var __ = client;

        var calls = 0;
        using var responder = server.Respond<TestMessage, TestMessage>(Endpoints.Echo, (request, _) =>
            Interlocked.Increment(ref calls) == 1 ? throw new InvalidOperationException("transient") : request);

        var response = await client.Request<TestMessage, TestMessage>(Endpoints.Echo, new TestMessage { Number = 5 }).Retry(2);

        Assert.Equal(5, response.Number);
        Assert.Equal(2, calls);
    }
}
