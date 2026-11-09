using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Light.Messaging.Internal;

internal enum EnqueueResult
{
    /// <summary>
    /// Queued. If the queue was full, the oldest published message was dropped to make room.
    /// </summary>
    Queued,

    /// <summary>
    /// Not queued: the queue was full and this frame was the one dropped (DropNewest).
    /// </summary>
    DroppedNewest,

    /// <summary>
    /// Not queued: the queue was full and the policy is Disconnect.
    /// </summary>
    Full,

    /// <summary>
    /// Not queued: the queue has been completed (shutting down or closed).
    /// </summary>
    Completed
}

/// <summary>
/// The per-connection outbound queue: many writers, one reader (the send loop), first in first out.
/// <para>
/// Unlike a bounded <c>Channel</c>, it knows which frames may be dropped. Only Publish frames count towards the
/// capacity and only they are ever dropped. Responses, requests and control frames (Hello, Goodbye, Subscribe,
/// Unsubscribe) are always queued, even past the capacity, because losing one breaks the protocol: a request would
/// time out for no visible reason, or a subscription would silently never start. Those frames are rare compared with
/// publishes, and a peer that stops reading altogether is closed by the send-stall watchdog, so the overflow stays
/// small.
/// </para>
/// </summary>
internal sealed class FrameQueue : IValueTaskSource<bool>
{
    private const int InitialSize = 64;

    private readonly object _gate = new();
    private readonly int _capacity;
    private readonly SlowConsumerPolicy _policy;
    private Frame[] _items = new Frame[InitialSize];
    private int _head;
    private int _count;
    private int _droppableCount;
    private bool _completed;
    private bool _readerWaiting;

    // Continuations run on the thread pool, so a writer never ends up running the send loop.
    private ManualResetValueTaskSourceCore<bool> _readerSignal = new() { RunContinuationsAsynchronously = true };

    public FrameQueue(int capacity, SlowConsumerPolicy policy)
    {
        _capacity = capacity;
        _policy = policy;
    }

    public int Count => Volatile.Read(ref _count);

    /// <summary>
    /// Takes ownership of <paramref name="frame"/>'s reference only when the result is <see cref="EnqueueResult.Queued"/>.
    /// <paramref name="dropped"/> is the frame dropped to make room, if any; the caller releases it.
    /// </summary>
    public EnqueueResult Enqueue(Frame frame, out Frame dropped)
    {
        dropped = null;
        var droppable = frame.Kind == FrameKind.Publish;
        bool wakeReader;

        lock (_gate)
        {
            if (_completed)
            {
                return EnqueueResult.Completed;
            }

            if (droppable && _droppableCount >= _capacity)
            {
                switch (_policy)
                {
                    case SlowConsumerPolicy.Disconnect:
                        return EnqueueResult.Full;

                    case SlowConsumerPolicy.DropNewest:
                        return EnqueueResult.DroppedNewest;

                    default:
                        dropped = RemoveOldestDroppable();
                        break;
                }
            }

            if (_count == _items.Length)
            {
                Grow();
            }

            _items[(_head + _count) & (_items.Length - 1)] = frame;
            Volatile.Write(ref _count, _count + 1);
            if (droppable)
            {
                _droppableCount++;
            }

            wakeReader = _readerWaiting;
            _readerWaiting = false;
        }

        if (wakeReader)
        {
            _readerSignal.SetResult(true);
        }

        return EnqueueResult.Queued;
    }

    public bool TryDequeue(out Frame frame)
    {
        lock (_gate)
        {
            if (_count == 0)
            {
                frame = null;
                return false;
            }

            frame = _items[_head];
            _items[_head] = null;
            _head = (_head + 1) & (_items.Length - 1);
            Volatile.Write(ref _count, _count - 1);
            if (frame.Kind == FrameKind.Publish)
            {
                _droppableCount--;
            }

            return true;
        }
    }

    /// <summary>
    /// True when a frame is available; false once the queue is completed and empty. For the single reader only.
    /// </summary>
    public ValueTask<bool> WaitToReadAsync()
    {
        lock (_gate)
        {
            if (_count > 0)
            {
                return new ValueTask<bool>(true);
            }

            if (_completed)
            {
                return new ValueTask<bool>(false);
            }

            _readerWaiting = true;
            return new ValueTask<bool>(this, _readerSignal.Version);
        }
    }

    /// <summary>
    /// Accepts nothing more. Frames already queued stay readable, unless <paramref name="discard"/> is set, in which case
    /// they are released.
    /// </summary>
    public void Complete(bool discard)
    {
        bool wakeReader;
        lock (_gate)
        {
            _completed = true;
            if (discard)
            {
                while (_count > 0)
                {
                    _items[_head].Release();
                    _items[_head] = null;
                    _head = (_head + 1) & (_items.Length - 1);
                    _count--;
                }

                Volatile.Write(ref _count, 0);
                _droppableCount = 0;
            }

            wakeReader = _readerWaiting;
            _readerWaiting = false;
        }

        if (wakeReader)
        {
            _readerSignal.SetResult(Count > 0);
        }
    }

    bool IValueTaskSource<bool>.GetResult(short token)
    {
        try
        {
            return _readerSignal.GetResult(token);
        }
        finally
        {
            _readerSignal.Reset();
        }
    }

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _readerSignal.GetStatus(token);

    void IValueTaskSource<bool>.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        _readerSignal.OnCompleted(continuation, state, token, flags);
    }

    /// <summary>
    /// Almost always the head of the queue. If control frames or responses sit in front of it, the frames before it
    /// are shifted along one slot.
    /// </summary>
    private Frame RemoveOldestDroppable()
    {
        var mask = _items.Length - 1;
        for (var offset = 0; offset < _count; offset++)
        {
            var index = (_head + offset) & mask;
            var candidate = _items[index];
            if (candidate.Kind != FrameKind.Publish)
            {
                continue;
            }

            for (var i = offset; i > 0; i--)
            {
                _items[(_head + i) & mask] = _items[(_head + i - 1) & mask];
            }

            _items[_head] = null;
            _head = (_head + 1) & mask;
            Volatile.Write(ref _count, _count - 1);
            _droppableCount--;
            return candidate;
        }

        return null;
    }

    private void Grow()
    {
        var larger = new Frame[_items.Length * 2];
        for (var i = 0; i < _count; i++)
        {
            larger[i] = _items[(_head + i) & (_items.Length - 1)];
        }

        _items = larger;
        _head = 0;
    }
}
