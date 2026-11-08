using Light.Messaging.Internal;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Light.Messaging.Tcp;

/// <summary>
/// Socket settings shared by <see cref="TcpServerTransport"/> and <see cref="TcpClientTransport"/>.
/// </summary>
public sealed class TcpSocketOptions
{
    /// <summary>
    /// Size of the buffer each socket receive fills; frames are decoded straight out of it.
    /// </summary>
    public int ReceiveBufferBytes { get; init; } = 256 * 1024;

    /// <summary>
    /// Kernel socket buffer sizes; 0 keeps the operating system default (auto-tuned on Windows and Linux).
    /// </summary>
    public int SocketSendBufferBytes { get; init; }

    public int SocketReceiveBufferBytes { get; init; }

    /// <summary>
    /// TCP keep-alive probes after this much idle time detect a peer that vanished without closing (power loss,
    /// cable pull). <see cref="TimeSpan.Zero"/> disables them.
    /// </summary>
    public TimeSpan KeepAliveIdle { get; init; } = TimeSpan.FromSeconds(10);

    internal void Configure(Socket socket)
    {
        // Frames leave as soon as the send loop writes them. Batching is the send loop's job, not Nagle's.
        socket.NoDelay = true;

        if (SocketSendBufferBytes > 0)
        {
            socket.SendBufferSize = SocketSendBufferBytes;
        }

        if (SocketReceiveBufferBytes > 0)
        {
            socket.ReceiveBufferSize = SocketReceiveBufferBytes;
        }

        if (KeepAliveIdle > TimeSpan.Zero)
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, (int)Math.Max(1, KeepAliveIdle.TotalSeconds));
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 3);
        }
    }
}

public sealed class TcpServerTransport : ServerTransport
{
    private readonly CancellationTokenSource _stopAccepting = new();
    private Socket _listener;
    private Task _acceptLoop = Task.CompletedTask;

    public TcpServerTransport(string host, int port)
    {
        Host = host;
        Port = port;
    }

    public string Host { get; }

    public int Port { get; }

    public TcpSocketOptions SocketOptions { get; init; } = new();

    /// <summary>
    /// The bound address once started; useful when <see cref="Port"/> is 0.
    /// </summary>
    public IPEndPoint LocalEndPoint => (IPEndPoint)_listener?.LocalEndPoint;

    internal override Task StartAsync(IConnectionHost host, CancellationToken cancellationToken)
    {
        var endPoint = new IPEndPoint(IPAddress.Parse(Host), Port);
        _listener = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(endPoint);
        _listener.Listen(512);
        _acceptLoop = AcceptLoopAsync(host);
        return Task.CompletedTask;
    }

    public override async ValueTask DisposeAsync()
    {
        _stopAccepting.Cancel();
        _listener?.Dispose();
        await _acceptLoop.ConfigureAwait(false);
    }

    private async Task AcceptLoopAsync(IConnectionHost host)
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
                // A client reset before the connection was accepted; keep listening.
                continue;
            }

            try
            {
                SocketOptions.Configure(socket);
            }
            catch (SocketException)
            {
                socket.Dispose();
                continue;
            }

            _ = host.RunConnectionAsync(new TcpConnection(socket, host.ConnectionSettings, SocketOptions.ReceiveBufferBytes));
        }
    }
}

public sealed class TcpClientTransport : ClientTransport
{
    public TcpClientTransport(string host, int port)
    {
        Host = host;
        Port = port;
    }

    public string Host { get; }

    public int Port { get; }

    public TcpSocketOptions SocketOptions { get; init; } = new();

    internal override async Task<Connection> ConnectAsync(ConnectionSettings settings, CancellationToken cancellationToken)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        try
        {
            SocketOptions.Configure(socket);
            await socket.ConnectAsync(Host, Port, cancellationToken).ConfigureAwait(false);
            return new TcpConnection(socket, settings, SocketOptions.ReceiveBufferBytes);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}
