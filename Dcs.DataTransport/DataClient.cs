using Dcs.DataTransport.Grpc;
using Dcs.DataTransport.Internal;
using Dcs.DataTransport.Tcp;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.DataTransport;

/// <summary>
/// Subscribes to a <see cref="DataServer{T}"/>. Messages are decoded straight out of the receive buffer and passed to
/// the handler on the receive loop, with no intermediate queue. A slow handler slows only this subscriber: the server
/// queues for it and then applies its <see cref="SlowSubscriberPolicy"/>.
/// </summary>
public sealed class DataClient<T>
{
    private readonly ClientTransport<T> _transport;

    public DataClient(IMessageCodec<T> codec, DataClientOptions options = null)
    {
        ArgumentNullException.ThrowIfNull(codec);
        options ??= new DataClientOptions();

        if (options.ReceiveBufferBytes < 2 * Framing.MaxFrameLength(codec))
        {
            throw new ArgumentException($"ReceiveBufferBytes must be at least {2 * Framing.MaxFrameLength(codec)} (two frames).", nameof(options));
        }

        _transport = options.Transport switch
        {
            TransportKind.Tcp => new TcpClientTransport<T>(codec, options),
            TransportKind.Grpc => new GrpcClientTransport<T>(codec, options),
            _ => throw new ArgumentOutOfRangeException(nameof(options), options.Transport, "Unknown transport.")
        };
    }

    /// <summary>
    /// Messages delivered to the handler so far. Safe to read from any thread.
    /// </summary>
    public long Received => _transport.Received;

    /// <summary>
    /// Connects and calls <paramref name="handler"/> for each message until the server ends the stream (the task completes),
    /// <paramref name="cancellationToken"/> is cancelled (<see cref="OperationCanceledException"/>), or the connection
    /// fails. The handler always runs on a single thread at a time, so it needs no locking for its own state.
    /// </summary>
    public Task RunAsync(Action<T> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return _transport.RunAsync(handler, cancellationToken);
    }
}
