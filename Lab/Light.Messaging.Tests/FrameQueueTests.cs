using Light.Messaging.Internal;

namespace Light.Messaging.Tests;

public sealed class FrameQueueTests
{
    [Fact]
    public void DropOldestDropsOnlyPublishedMessagesAndKeepsOrder()
    {
        var queue = new FrameQueue(capacity: 2, SlowConsumerPolicy.DropOldest);
        queue.Enqueue(Make(FrameKind.Response, 100), out _);
        queue.Enqueue(Make(FrameKind.Publish, 1), out _);
        queue.Enqueue(Make(FrameKind.Publish, 2), out _);

        // Full of publishes: the oldest publish goes, not the response in front of it.
        Assert.Equal(EnqueueResult.Queued, queue.Enqueue(Make(FrameKind.Publish, 3), out var dropped));
        Assert.Equal(1, dropped!.EndpointId);
        dropped.Release();

        // Control frames are queued past the capacity rather than dropped or refused.
        Assert.Equal(EnqueueResult.Queued, queue.Enqueue(Make(FrameKind.Subscribe, 200), out var none));
        Assert.Null(none);

        Assert.Equal(
            [(FrameKind.Response, 100), (FrameKind.Publish, 2), (FrameKind.Publish, 3), (FrameKind.Subscribe, 200)],
            Drain(queue));
    }

    [Fact]
    public void DropNewestRefusesTheIncomingMessage()
    {
        var queue = new FrameQueue(capacity: 1, SlowConsumerPolicy.DropNewest);
        queue.Enqueue(Make(FrameKind.Publish, 1), out _);

        var incoming = Make(FrameKind.Publish, 2);
        Assert.Equal(EnqueueResult.DroppedNewest, queue.Enqueue(incoming, out _));
        incoming.Release();

        Assert.Equal([(FrameKind.Publish, 1)], Drain(queue));
    }

    [Fact]
    public void DisconnectPolicyReportsFull()
    {
        var queue = new FrameQueue(capacity: 1, SlowConsumerPolicy.Disconnect);
        queue.Enqueue(Make(FrameKind.Publish, 1), out _);

        var incoming = Make(FrameKind.Publish, 2);
        Assert.Equal(EnqueueResult.Full, queue.Enqueue(incoming, out _));
        incoming.Release();
    }

    [Fact]
    public async Task ReaderWakesForNewFramesAndStopsOnceCompletedAndEmpty()
    {
        var queue = new FrameQueue(capacity: 10, SlowConsumerPolicy.DropOldest);

        var wait = queue.WaitToReadAsync();
        Assert.False(wait.IsCompleted);
        queue.Enqueue(Make(FrameKind.Publish, 1), out _);
        Assert.True(await wait);

        queue.Complete(discard: false);
        Assert.True(await queue.WaitToReadAsync());
        Assert.Single(Drain(queue));
        Assert.False(await queue.WaitToReadAsync());
        Assert.Equal(EnqueueResult.Completed, queue.Enqueue(Make(FrameKind.Publish, 2), out _));
    }

    private static Frame Make(FrameKind kind, int endpointId)
    {
        var frame = Frame.Rent(kind, endpointId, correlationId: 1);
        frame.Seal(1024 * 1024);
        return frame;
    }

    private static List<(FrameKind Kind, int EndpointId)> Drain(FrameQueue queue)
    {
        var frames = new List<(FrameKind, int)>();
        while (queue.TryDequeue(out var frame))
        {
            frames.Add((frame.Kind, frame.EndpointId));
            frame.Release();
        }

        return frames;
    }
}
