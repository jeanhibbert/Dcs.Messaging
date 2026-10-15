using Dcs.DataTransport.Internal;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.DataTransport.Tcp;

internal sealed class TcpServerTransport<T> : IServerTransport
{
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(5);

    private readonly DataServer<T> _server;
    private readonly IMessageCodec<T> _codec;
    private readonly DataServerOptions _options;
    private readonly CancellationTokenSource _stopAccepting = new();
    private readonly CancellationTokenSource _abort = new();
    private readonly ConcurrentDictionary<Task, bool> _connections = new();
    private Socket _listener;
    private Task _acceptLoop = Task.CompletedTask;

    public TcpServerTransport(DataServer<T> server, IMessageCodec<T> codec, DataServerOptions options)
    {
        _server = server;
        _codec = codec;
        _options = options;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var endPoint = new IPEndPoint(IPAddress.Parse(_options.Host), _options.Port);
        _listener = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(endPoint);
        _listener.Listen(512);
        _acceptLoop = AcceptLoopAsync();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _stopAccepting.Cancel();
        _listener?.Dispose();
        await _acceptLoop.ConfigureAwait(false);

        // DataServer has completed every subscriber queue, so the writer loops are draining. Wait for them, then cut
        // off any that are still going (typically a slow subscriber with a full queue).
        var connections = Task.WhenAll(_connections.Keys);
        if (await Task.WhenAny(connections, Task.Delay(_options.ShutdownDrainTimeout)).ConfigureAwait(false) != connections)
        {
            _abort.Cancel();
        }

        await connections.ConfigureAwait(false);
    }

    private async Task AcceptLoopAsync()
    {
        while (true)
        {
            Socket socket;
            try
            {
                socket = await _listener.AcceptAsync(_stopAccepting.Token).ConfigureAwait(false);
            }
            catch (Exception) when (_stopAccepting.IsCancellationRequested)
            {
                return;
            }
            catch (SocketException)
            {
                // A client reset the connection before it was accepted; keep listening.
                continue;
            }

            var connection = RunConnectionAsync(socket);
            _connections.TryAdd(connection, true);
            _ = connection.ContinueWith(completed => _connections.TryRemove(completed, out _), TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Never throws: every failure ends up as the subscriber's disconnect reason.
    /// </summary>
    private async Task RunConnectionAsync(Socket socket)
    {
        Subscriber<T> subscriber = null;
        var closeReason = "server shutdown";
        try
        {
            // Frames go out as soon as they are written; batching is done by the writer loop, not by Nagle's algorithm.
            socket.NoDelay = true;
            if (_options.SocketSendBufferBytes > 0)
            {
                socket.SendBufferSize = _options.SocketSendBufferBytes;
            }

            string name;
            using (var handshakeTimeout = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token))
            {
                handshakeTimeout.CancelAfter(HandshakeTimeout);
                name = await socket.ReceiveSubscriberNameAsync(handshakeTimeout.Token).ConfigureAwait(false);
            }

            subscriber = _server.AddSubscriber(name, TransportKind.Tcp, socket.RemoteEndPoint?.ToString());
            if (subscriber == null)
            {
                return;
            }

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(subscriber.DisconnectToken, _abort.Token);

            // Disposing the socket is what reliably breaks a send that is blocked on a peer that stopped reading.
            using var abortSend = stop.Token.Register(static state => ((Socket)state).Dispose(), socket);

            _ = WatchForPeerCloseAsync(socket, subscriber);
            await WriteLoopAsync(socket, subscriber, stop.Token).ConfigureAwait(false);

            // Queue drained after DataServer completed it: end the stream so the client's receive returns 0.
            socket.Shutdown(SocketShutdown.Send);
        }
        catch (Exception exception) when (exception is OperationCanceledException or SocketException or IOException or ObjectDisposedException)
        {
            closeReason = _abort.IsCancellationRequested ? "shutdown drain timeout" : exception.Message;
        }
        finally
        {
            // Record the reason before closing the socket, so the peer-close watcher cannot overwrite it.
            if (subscriber != null)
            {
                _server.RemoveSubscriber(subscriber, closeReason);
            }

            socket.Dispose();
        }
    }

    private async Task WriteLoopAsync(Socket socket, Subscriber<T> subscriber, CancellationToken cancellationToken)
    {
        // Allocated on the pinned object heap so the socket layer does not pin and unpin it on every send.
        var buffer = GC.AllocateUninitializedArray<byte>(_options.MaxBatchBytes, pinned: true);
        var reader = subscriber.Reader;

        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var length = Framing.EncodeBatch(reader, _codec, buffer, out var count);
            if (count == 0)
            {
                continue;
            }

            subscriber.BeginSend();
            await socket.SendAllAsync(buffer.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
            subscriber.EndSend(count);
        }
    }

    /// <summary>
    /// After the handshake the client sends nothing, so any completed receive means it has gone away.
    /// </summary>
    private static async Task WatchForPeerCloseAsync(Socket socket, Subscriber<T> subscriber)
    {
        try
        {
            await socket.ReceiveAsync(new byte[1], SocketFlags.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }

        subscriber.Disconnect("peer closed connection");
    }
}
