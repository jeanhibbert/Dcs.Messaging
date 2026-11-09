using System.Diagnostics;

namespace Light.Messaging.Tests;

public sealed class MessagingTests
{
    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task PublishedMessagesReachSubscribedClientInOrder(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;

        const int count = 10_000;
        var received = new List<int>();
        var done = new TaskCompletionSource();
        using var subscription = client.Subscribe<TestMessage>(Endpoints.Events, message =>
        {
            received.Add(message.Number);
            if (message.Number == count - 1)
            {
                done.TrySetResult();
            }
        });
        await Harness.BarrierAsync(client);

        for (var i = 0; i < count; i++)
        {
            server.Publish(Endpoints.Events, new TestMessage { Number = i });
        }

        await done.Task.WaitAsync(Harness.Timeout);
        Assert.Equal(Enumerable.Range(0, count), received);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ClientPublishReachesServerWithSender(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;

        var received = new TaskCompletionSource<(int Number, string SenderName)>();
        using var subscription = server.Subscribe<TestMessage>(Endpoints.FromClient, (message, context) => received.TrySetResult((message.Number, context.Sender.Name)));

        client.Publish(Endpoints.FromClient, new TestMessage { Number = 42 });

        var (number, senderName) = await received.Task.WaitAsync(Harness.Timeout);
        Assert.Equal(42, number);
        Assert.Equal("test-client", senderName);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ConcurrentRequestsGetTheirOwnResponses(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;
        using var responder = server.Respond<TestMessage, TestMessage>(Endpoints.Echo, (request, context) => new TestMessage { Number = request.Number * 2, Text = context.Sender.Name });

        var responses = await Task.WhenAll(Enumerable.Range(0, 1000).Select(async i =>
            await client.RequestAsync<TestMessage, TestMessage>(Endpoints.Echo, new TestMessage { Number = i })));

        Assert.Equal(Enumerable.Range(0, 1000).Select(i => i * 2), responses.Select(response => response.Number));
        Assert.All(responses, response => Assert.Equal("test-client", response.Text));
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task AsyncResponderAnswers(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;
        using var responder = server.RespondAsync<TestMessage, TestMessage>(Endpoints.Echo, async (request, _) =>
        {
            await Task.Delay(10);
            return new TestMessage { Number = request.Number + 1 };
        });

        var response = await client.RequestAsync<TestMessage, TestMessage>(Endpoints.Echo, new TestMessage { Number = 1 });

        Assert.Equal(2, response.Number);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ResponderExceptionFailsTheRequest(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;
        var reported = server.HandlerErrors.FirstAsync().ToTask();
        using var responder = server.Respond<TestMessage, TestMessage>(Endpoints.Fail, (_, _) => throw new InvalidOperationException("no price for EURXYZ"));

        var exception = await Assert.ThrowsAsync<RemoteException>(async () =>
            await client.RequestAsync<TestMessage, TestMessage>(Endpoints.Fail, new TestMessage()));

        Assert.Equal("no price for EURXYZ", exception.Message);
        Assert.IsType<InvalidOperationException>(await reported.WaitAsync(Harness.Timeout));
        Assert.True(client.IsConnected);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task RequestWithoutResponderFails(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;

        var exception = await Assert.ThrowsAsync<RemoteException>(async () =>
            await client.RequestAsync<TestMessage, TestMessage>(Endpoints.Missing, new TestMessage()));

        Assert.Contains("No responder", exception.Message);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task RequestTimesOut(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;
        using var responder = server.RespondAsync<TestMessage, TestMessage>(Endpoints.Slow, async (request, _) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(2));
            return request;
        });

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAsync<TimeoutException>(async () =>
            await client.RequestAsync<TestMessage, TestMessage>(Endpoints.Slow, new TestMessage(), TimeSpan.FromMilliseconds(100)));

        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromMilliseconds(90), TimeSpan.FromSeconds(1));
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task MessagesLargerThanTheBuffersRoundTrip(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;
        await using var __ = client;
        using var responder = server.Respond<TestMessage, TestMessage>(Endpoints.Echo, (request, _) => request);

        // Bigger than the 64 KB send batch and the 256 KB receive buffer, and sent alongside small messages.
        var blob = new byte[3 * 1024 * 1024];
        Random.Shared.NextBytes(blob);
        var small = client.RequestAsync<TestMessage, TestMessage>(Endpoints.Echo, new TestMessage { Number = 1 });
        var large = client.RequestAsync<TestMessage, TestMessage>(Endpoints.Echo, new TestMessage { Number = 2, Blob = blob });
        var smallAfter = client.RequestAsync<TestMessage, TestMessage>(Endpoints.Echo, new TestMessage { Number = 3 });

        Assert.Equal(1, (await small).Number);
        Assert.Equal(blob, (await large).Blob);
        Assert.Equal(3, (await smallAfter).Number);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task PublishSerializesOnlyWhenSomeoneIsSubscribed(Transport transport)
    {
        var serializer = new CountingSerializer();
        var (server, client) = await Harness.StartAsync(transport, new MessagingServerOptions { Serializer = serializer });
        await using var _ = server;
        await using var __ = client;

        server.Publish(Endpoints.Events, new TestMessage { Number = 1 });
        Assert.Equal(0, serializer.TestMessagesSerialized);

        var received = new TaskCompletionSource<int>();
        var subscription = client.Subscribe<TestMessage>(Endpoints.Events, message => received.TrySetResult(message.Number));
        await Harness.BarrierAsync(client);
        var serializedBefore = serializer.TestMessagesSerialized;

        server.Publish(Endpoints.Events, new TestMessage { Number = 2 });
        Assert.Equal(2, await received.Task.WaitAsync(Harness.Timeout));
        Assert.Equal(serializedBefore + 1, serializer.TestMessagesSerialized);

        subscription.Dispose();
        await Harness.BarrierAsync(client);
        serializedBefore = serializer.TestMessagesSerialized;

        server.Publish(Endpoints.Events, new TestMessage { Number = 3 });
        Assert.Equal(serializedBefore, serializer.TestMessagesSerialized);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task SlowConsumerIsDisconnectedWithoutBlockingThePublisher(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport, new MessagingServerOptions { SendQueueCapacity = 100, SlowConsumerPolicy = SlowConsumerPolicy.Disconnect });
        await using var _ = server;
        await using var __ = client;

        using var release = new ManualResetEventSlim(false);
        using var subscription = client.Subscribe<TestMessage>(Endpoints.Events, _ => release.Wait());
        await Harness.BarrierAsync(client);

        var disconnected = server.PeerDisconnected.FirstAsync().ToTask();

        // The blocked handler stops the client reading; once the kernel and HTTP/2 buffers fill, the server's
        // 100-frame queue fills and the Disconnect policy closes the connection.
        var payload = new byte[1024];
        var stopwatch = Stopwatch.StartNew();
        while (!disconnected.IsCompleted && stopwatch.Elapsed < Harness.Timeout)
        {
            server.Publish(Endpoints.Events, new TestMessage { Number = 1, Blob = payload });
        }

        release.Set();
        var peer = await disconnected.WaitAsync(Harness.Timeout);
        Assert.Contains("queue full", peer.CloseReason);
        await client.Completion.WaitAsync(Harness.Timeout);
        Assert.False(client.IsConnected);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ServerShutdownDeliversQueuedMessagesFirst(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var __ = client;

        const int count = 20_000;
        var received = 0;
        using var subscription = client.Subscribe<TestMessage>(Endpoints.Events, _ => received++);
        await Harness.BarrierAsync(client);

        for (var i = 0; i < count; i++)
        {
            server.Publish(Endpoints.Events, new TestMessage { Number = i });
        }

        await server.DisposeAsync();
        await client.Completion.WaitAsync(Harness.Timeout);

        Assert.Equal(count, received);
        Assert.Equal("server shutting down", client.Server.CloseReason);
    }

    [Theory]
    [InlineData(Transport.Tcp)]
    [InlineData(Transport.Grpc)]
    public async Task ClientDisposeDeliversQueuedMessagesFirst(Transport transport)
    {
        var (server, client) = await Harness.StartAsync(transport);
        await using var _ = server;

        const int count = 20_000;
        var received = 0;
        var disconnected = server.PeerDisconnected.FirstAsync().ToTask();
        using var subscription = server.Subscribe<TestMessage>(Endpoints.FromClient, _ => received++);

        for (var i = 0; i < count; i++)
        {
            client.Publish(Endpoints.FromClient, new TestMessage { Number = i });
        }

        await client.DisposeAsync();
        var peer = await disconnected.WaitAsync(Harness.Timeout);

        Assert.Equal(count, received);
        Assert.Equal("client disposed", peer.CloseReason);
    }

    [Fact]
    public async Task ServerClosesConnectionsThatNeverSayHello()
    {
        var transport = new Tcp.TcpServerTransport("127.0.0.1", 0);
        await using var server = await MessagingServer.StartAsync(transport);

        using var socket = new System.Net.Sockets.Socket(System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
        await socket.ConnectAsync(transport.LocalEndPoint);

        // The server's own Hello arrives first; after the handshake timeout the server closes the connection.
        var buffer = new byte[1024];
        var stopwatch = Stopwatch.StartNew();
        int read;
        do
        {
            read = await socket.ReceiveAsync(buffer).WaitAsync(Harness.Timeout);
        }
        while (read > 0);

        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void EndpointIdIsDerivedFromTheAddress()
    {
        Assert.Equal(new Endpoint("Tests/Stable").Id, new Endpoint("Tests/Stable").Id);
        Assert.Equal(Endpoint.ComputeId("Tests/Stable"), new Endpoint("Tests/Stable").Id);
        Assert.NotEqual(new Endpoint("Tests/A").Id, new Endpoint("Tests/B").Id);
    }
}
