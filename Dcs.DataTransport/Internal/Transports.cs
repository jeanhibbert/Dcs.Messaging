using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.DataTransport.Internal;

/// <summary>
/// Accepts subscribers and runs a writer loop per subscriber. Fan-out, queues and policies live in
/// <see cref="DataServer{T}"/>, so transports only move bytes.
/// </summary>
internal interface IServerTransport : IAsyncDisposable
{
    Task StartAsync(CancellationToken cancellationToken);
}

internal abstract class ClientTransport<T>
{
    private long _received;

    protected ClientTransport(IMessageCodec<T> codec, DataClientOptions options)
    {
        Codec = codec;
        Options = options;
    }

    public long Received => Volatile.Read(ref _received);

    protected IMessageCodec<T> Codec { get; }

    protected DataClientOptions Options { get; }

    public abstract Task RunAsync(Action<T> handler, CancellationToken cancellationToken);

    /// <summary>
    /// Only the receive loop writes the counter, so a plain volatile write suffices (no interlocked operation).
    /// </summary>
    protected void AddReceived(int count)
    {
        Volatile.Write(ref _received, _received + count);
    }
}
