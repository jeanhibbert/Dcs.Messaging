using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Light.Messaging.Internal;

internal abstract class Subscription
{
    protected Subscription(Endpoint endpoint)
    {
        Endpoint = endpoint;
    }

    public Endpoint Endpoint { get; }

    public abstract void Dispatch(ReadOnlySpan<byte> payload, MessageContext context);
}

/// <summary>
/// Deserializes straight from the receive buffer and calls the handler on the receive loop: no queue, no thread hop.
/// </summary>
internal sealed class Subscription<T> : Subscription
{
    private readonly Action<T, MessageContext> _handler;
    private readonly IMessageSerializer _serializer;

    public Subscription(Endpoint endpoint, Action<T, MessageContext> handler, IMessageSerializer serializer)
        : base(endpoint)
    {
        _handler = handler;
        _serializer = serializer;
    }

    public override void Dispatch(ReadOnlySpan<byte> payload, MessageContext context)
    {
        _handler(_serializer.Deserialize<T>(payload), context);
    }
}

internal abstract class Responder
{
    protected Responder(Endpoint endpoint)
    {
        Endpoint = endpoint;
    }

    public Endpoint Endpoint { get; }

    public abstract void Handle(Connection connection, long correlationId, ReadOnlySpan<byte> payload);
}

/// <summary>
/// A synchronous handler answers on the receive loop. An asynchronous handler that completes synchronously takes the
/// same path; otherwise the response is sent from its continuation, and the receive loop moves straight on.
/// </summary>
internal sealed class Responder<TRequest, TResponse> : Responder
{
    private readonly MessagingServer _server;
    private readonly Func<TRequest, MessageContext, TResponse> _handler;
    private readonly Func<TRequest, MessageContext, ValueTask<TResponse>> _asyncHandler;

    public Responder(
        MessagingServer server,
        Endpoint endpoint,
        Func<TRequest, MessageContext, TResponse> handler,
        Func<TRequest, MessageContext, ValueTask<TResponse>> asyncHandler)
        : base(endpoint)
    {
        _server = server;
        _handler = handler;
        _asyncHandler = asyncHandler;
    }

    public override void Handle(Connection connection, long correlationId, ReadOnlySpan<byte> payload)
    {
        ValueTask<TResponse> pending;
        try
        {
            var request = _server.Serializer.Deserialize<TRequest>(payload);
            var context = new MessageContext(connection.Peer);
            pending = _handler != null ? new ValueTask<TResponse>(_handler(request, context)) : _asyncHandler(request, context);
        }
        catch (Exception exception)
        {
            Fail(connection, correlationId, exception);
            return;
        }

        if (pending.IsCompletedSuccessfully)
        {
            Reply(connection, correlationId, pending.Result);
        }
        else
        {
            _ = ReplyWhenCompleteAsync(connection, correlationId, pending);
        }
    }

    private async Task ReplyWhenCompleteAsync(Connection connection, long correlationId, ValueTask<TResponse> pending)
    {
        try
        {
            Reply(connection, correlationId, await pending.ConfigureAwait(false));
        }
        catch (Exception exception)
        {
            Fail(connection, correlationId, exception);
        }
    }

    private void Reply(Connection connection, long correlationId, TResponse response)
    {
        Frame frame;
        try
        {
            frame = _server.CreateFrame(FrameKind.Response, Endpoint.Id, correlationId, response);
        }
        catch (Exception exception)
        {
            Fail(connection, correlationId, exception);
            return;
        }

        connection.Enqueue(frame);
    }

    private void Fail(Connection connection, long correlationId, Exception exception)
    {
        _server.ReportHandlerError(exception);
        MessagingServer.SendError(connection, Endpoint.Id, correlationId, exception.Message);
    }
}

internal abstract class PendingRequest
{
    /// <summary>
    /// UTC ticks on the session's scheduler clock after which the request times out.
    /// </summary>
    public long Deadline { get; protected set; }

    public abstract void Complete(ReadOnlySpan<byte> payload);

    public abstract void Fail(Exception exception);
}

/// <summary>
/// A pooled <see cref="IValueTaskSource{TResult}"/>, so a request allocates no Task or TaskCompletionSource. The
/// usual <see cref="ValueTask{TResult}"/> rule applies: await the result exactly once.
/// </summary>
internal sealed class PendingRequest<T> : PendingRequest, IValueTaskSource<T>
{
    private static readonly ConcurrentQueue<PendingRequest<T>> Pool = new();

    // Continuations run on the thread pool, never on the receive loop that completes the request.
    private ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = true };
    private IMessageSerializer _serializer;

    public ValueTask<T> Task => new(this, _core.Version);

    public static PendingRequest<T> Rent(IMessageSerializer serializer, long deadline)
    {
        if (!Pool.TryDequeue(out var request))
        {
            request = new PendingRequest<T>();
        }

        request._serializer = serializer;
        request.Deadline = deadline;
        return request;
    }

    public override void Complete(ReadOnlySpan<byte> payload)
    {
        T response;
        try
        {
            response = _serializer.Deserialize<T>(payload);
        }
        catch (Exception exception)
        {
            _core.SetException(exception);
            return;
        }

        _core.SetResult(response);
    }

    public override void Fail(Exception exception)
    {
        _core.SetException(exception);
    }

    public T GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            _core.Reset();
            _serializer = null;
            Pool.Enqueue(this);
        }
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        _core.OnCompleted(continuation, state, token, flags);
    }
}
