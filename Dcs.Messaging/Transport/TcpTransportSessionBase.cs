using Dcs.Messaging;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.Messaging.Transport
{
    /// <summary>
    /// Socket plumbing shared by transports that exchange self-delimiting frames over raw TCP
    /// (<see cref="Tcp.TcpMessagingSession"/> and <see cref="Sbe.SbeMessagingSession"/>).
    /// Derived sessions only supply the wire encoding. Encoding members are invoked from the
    /// base constructor's accept/read loops, so implementations must not depend on derived state.
    /// </summary>
    public abstract class TcpTransportSessionBase : IDisposable
    {
        private readonly ISubject<IncomingTransportMessage> _incomingMessages;
        private readonly CancellationTokenSource _shutdown;
        private readonly ConcurrentDictionary<string, TcpConnectionContext> _connectionsBySessionId;
        private readonly ConcurrentDictionary<long, TcpConnectionContext> _connections;
        private readonly bool _isServer;
        private TcpListener _listener;
        private long _connectionCounter;

        private protected TcpTransportSessionBase(string host, int port, bool isServer, string sessionId)
        {
            SessionId = sessionId;
            _isServer = isServer;
            _incomingMessages = Subject.Synchronize(new Subject<IncomingTransportMessage>());
            _shutdown = new CancellationTokenSource();
            _connectionsBySessionId = new ConcurrentDictionary<string, TcpConnectionContext>(StringComparer.Ordinal);
            _connections = new ConcurrentDictionary<long, TcpConnectionContext>();

            if (isServer)
            {
                StartServer(host, port);
            }
            else
            {
                ConnectClient(host, port);
            }
        }

        public string SessionId { get; }

        public void Dispose()
        {
            if (_shutdown.IsCancellationRequested)
            {
                return;
            }

            _shutdown.Cancel();

            try
            {
                _listener?.Stop();
            }
            catch
            {
            }

            foreach (var connection in _connections.Values)
            {
                connection.Dispose();
            }

            _incomingMessages.OnCompleted();
            _shutdown.Dispose();
        }

        internal IMessageStream GetMessageStream(IEndpointDetails endpointDetails)
        {
            var messages = _incomingMessages
                .Where(incoming => string.Equals(incoming.EndpointAddress, endpointDetails.Address, StringComparison.Ordinal))
                .Where(incoming => string.IsNullOrEmpty(incoming.TargetSessionId) || string.Equals(incoming.TargetSessionId, SessionId, StringComparison.Ordinal))
                .Select(incoming => incoming.Message);

            return new MessageStream(messages);
        }

        internal void Send(IEndpointDetails endpointDetails, IMessage message, string targetSessionId)
        {
            var frame = EncodeFrame(endpointDetails.Address, message, targetSessionId);

            if (!_isServer)
            {
                var connection = _connections.Values.SingleOrDefault();
                if (connection == null)
                {
                    throw new InvalidOperationException("Client is not connected to a server endpoint.");
                }

                connection.Send(frame);
                return;
            }

            if (!string.IsNullOrEmpty(targetSessionId))
            {
                if (_connectionsBySessionId.TryGetValue(targetSessionId, out var connection))
                {
                    connection.Send(frame);
                }

                return;
            }

            foreach (var connection in _connections.Values)
            {
                connection.Send(frame);
            }
        }

        /// <summary>
        /// Encodes a complete, self-delimiting frame ready to be written to the socket.
        /// </summary>
        private protected abstract byte[] EncodeFrame(string endpointAddress, IMessage message, string targetSessionId);

        /// <summary>
        /// Reads and decodes the next frame, or returns <c>null</c> when the remote end closed the connection
        /// or sent a frame that cannot be delimited (the connection is then dropped).
        /// </summary>
        private protected abstract Task<IncomingTransportMessage> ReadFrameAsync(NetworkStream stream, CancellationToken cancellationToken);

        private void ConnectClient(string host, int port)
        {
            var tcpClient = new TcpClient();
            tcpClient.Connect(host, port);
            RegisterConnection(tcpClient);
        }

        private void HandleIncomingMessage(TcpConnectionContext connection, IncomingTransportMessage incoming)
        {
            var senderSessionId = incoming.Message.SenderSessionId;
            if (!string.IsNullOrWhiteSpace(senderSessionId))
            {
                connection.SetRemoteSessionId(senderSessionId);
                _connectionsBySessionId[senderSessionId] = connection;
            }

            _incomingMessages.OnNext(incoming);
        }

        private async Task ProcessConnectionAsync(TcpConnectionContext connection)
        {
            try
            {
                while (!_shutdown.IsCancellationRequested)
                {
                    var incoming = await ReadFrameAsync(connection.Stream, _shutdown.Token).ConfigureAwait(false);
                    if (incoming == null)
                    {
                        break;
                    }

                    HandleIncomingMessage(connection, incoming);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (IOException)
            {
            }
            catch (InvalidDataException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            finally
            {
                RemoveConnection(connection);
                connection.Dispose();
            }
        }

        private void RegisterConnection(TcpClient client)
        {
            var id = Interlocked.Increment(ref _connectionCounter);
            var connection = new TcpConnectionContext(id, client);
            _connections[id] = connection;
            _ = Task.Run(() => ProcessConnectionAsync(connection), _shutdown.Token);
        }

        private void RemoveConnection(TcpConnectionContext connection)
        {
            _connections.TryRemove(connection.Id, out _);
            if (!string.IsNullOrWhiteSpace(connection.RemoteSessionId))
            {
                _connectionsBySessionId.TryRemove(connection.RemoteSessionId, out _);
            }
        }

        private void StartServer(string host, int port)
        {
            var ipAddress = IPAddress.Parse(host);
            _listener = new TcpListener(ipAddress, port);
            _listener.Start();
            _ = Task.Run(AcceptLoopAsync, _shutdown.Token);
        }

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_shutdown.IsCancellationRequested)
                {
                    var tcpClient = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                    RegisterConnection(tcpClient);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private sealed class TcpConnectionContext : IDisposable
        {
            private readonly SemaphoreSlim _sendLock;
            private readonly TcpClient _tcpClient;

            public TcpConnectionContext(long id, TcpClient tcpClient)
            {
                Id = id;
                _tcpClient = tcpClient;
                Stream = tcpClient.GetStream();
                _sendLock = new SemaphoreSlim(1, 1);
            }

            public long Id { get; }

            public string RemoteSessionId { get; private set; }

            public NetworkStream Stream { get; }

            public void Dispose()
            {
                _sendLock.Dispose();
                Stream.Dispose();
                _tcpClient.Dispose();
            }

            public void Send(byte[] frame)
            {
                _sendLock.Wait();
                try
                {
                    Stream.Write(frame, 0, frame.Length);
                    Stream.Flush();
                }
                finally
                {
                    _sendLock.Release();
                }
            }

            public void SetRemoteSessionId(string remoteSessionId)
            {
                RemoteSessionId = remoteSessionId;
            }
        }
    }
}
