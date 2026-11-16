using LabOriginal.Neutron.Tools;
using Microsoft.Extensions.Logging;
using ProtoBuf.Meta;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace LabOriginal.Neutron
{
    public class ProtobuffAsyncServer<T> : IProtobuffAsyncServer<T> where T : IBaseMessage
    {
        public IPEndPoint ListenerAddress { get; }
        private CancellationTokenSource _cts;
        private readonly IServerProtoMessageParser<T> _messageParser;
        private readonly ILogger<ProtobuffAsyncServer<T>> _log;
        private readonly T _heartBeatMsg;
        private bool _disposedValue = false; // To detect redundant calls
        private readonly int _heartBeatIntervalInSec;
        private readonly TcpListener _listener;
        public event EventHandler<IProtobuffClientConnection<T>> OnClientConnection;
        public event EventHandler<IProtobuffClientConnection<T>> OnClientDisconnection;
        public event EventHandler<Exception> OnServerError;
        private bool _handleStartException = false;

        public ProtobuffAsyncServer(IServerProtoMessageParser<T> messageParser, ProtobuffServerConfig<T> config, ILogger<ProtobuffAsyncServer<T>> log)
        {
            RuntimeTypeModel.Default.IncludeDateTimeKind = true;
            ListenerAddress = config.ListenerAddress;
            _messageParser = messageParser;
            _log = log;
            _heartBeatMsg = config.HeartBeatMsg;
            _heartBeatIntervalInSec = config.HeartBeatIntervalInSec;
            _listener = new TcpListener(ListenerAddress);
            _handleStartException = config.HandleStartException;

        }

        public IDictionary<string, IProtobuffClientConnection<T>> ClientConnections { get; set; } = new ConcurrentDictionary<string, IProtobuffClientConnection<T>>();

        public void Start()
        {
            try
            {
                _log.LogInformation($"Starting ProtoBuff listener on {ListenerAddress}");

                _listener.Start();
                _cts = new CancellationTokenSource();
                Task.Run(() => AcceptConnections(_cts.Token));

            }
            catch (Exception exp)
            {
                if (_handleStartException)
                {
                    _log.LogError(exp, $"ProtobuffAsyncServer Start");
                    HandleServerException(exp);
                }
                else
                {
                    throw;
                }
            }
        }

        private async Task AcceptConnections(CancellationToken ctsToken)
        {
            while (!ctsToken.IsCancellationRequested)
            {
                TcpClient tcpClient = await _listener.AcceptTcpClientAsync();

                HandleConnection(tcpClient);
            }

        }

        public void Stop()
        {
            _log.LogInformation("Stopping ProtoBuff listener");
            _cts?.Cancel();
            _listener.Stop();
            DisconnectAll();
        }

        private void HandleServerException(Exception ex)
        {
            OnServerError?.Invoke(this, ex);
        }

        public int ConnectedClientCount => ClientConnections.Count;

        public bool Disconnect(IProtobuffClientConnection<T> connection)
        {
            bool result = false;
            _log.LogInformation($"Disconnecting client connection {connection.Id}");
            if (ClientConnections.TryGetValue(connection.Id, out IProtobuffClientConnection<T> localConnection))
            {
                result = localConnection.Disconnect();
            }

            if (localConnection != null)
            {
                ClientConnections.Remove(localConnection.Id);
            }

            return result;
        }

        private void HandleConnection(TcpClient tcpClient)
        {
            string id = null;
            try
            {
                var ipAddress = ((IPEndPoint)tcpClient.Client.RemoteEndPoint).Address;
                string port = ((IPEndPoint)tcpClient.Client.RemoteEndPoint).Port.ToString();

                id = $"{Guid.NewGuid()}_{ipAddress}:{port}";

                var clientConnection = new ProtobuffClientConnection<T>(this, id, tcpClient, _messageParser, _heartBeatMsg, _heartBeatIntervalInSec);
                ClientConnections.Add(clientConnection.Id, clientConnection);
                OnClientConnection?.Invoke(this, clientConnection);
                clientConnection.OnClientDisconnection += HandleOnClientDisconnection;
                clientConnection.Start();
            }
            catch (Exception ex)
            {
                _log.LogError(ex, $"ProtobuffAsyncServer.HandleConnectionAsync - Client Connection will be closed");

                //try and remove from collection
                if (id != null)
                {
                    ClientConnections.Remove(id);
                }

                NetworkTools.CloseConnectionFully(tcpClient);
                HandleServerException(ex);
            }
        }

        private void HandleOnClientDisconnection(object sender, IProtobuffClientConnection<T> e)
        {
            OnClientDisconnection?.Invoke(this, e);
        }

        public bool SendMessage(string id, T message)
        {
            if (ClientConnections.TryGetValue(id, out IProtobuffClientConnection<T> connection))
            {
                return connection.SendMessage(message);
            }

            return false;
        }

        public bool SendMessage(T message)
        {
            var result = true;

            foreach (var connection in ClientConnections.Values)
            {
                result = result & connection.SendMessage(message);
            }

            return result;
        }

        public bool IsBound
        {
            get
            {
                return _listener.Server.IsBound;
            }
        }


        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                Stop();
                if (disposing)
                {
                    foreach (var connection in ClientConnections.Values)
                    {
                        connection.Dispose();
                    }

                    ClientConnections.Clear();
                }

                _disposedValue = true;
            }
        }

        private void DisconnectAll()
        {
            foreach (var connection in ClientConnections.Values)
            {
                Disconnect(connection);
            }

        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void RemoveClientConnection(string id)
        {
            if (ClientConnections.TryGetValue(id, out var clientConnection))
            {
                OnClientDisconnection?.Invoke(this, clientConnection);

                ClientConnections.Remove(id);
            }
            else
            {
                _log.LogWarning($"{id} Client Connection was not present");
            }
        }

        #endregion
    }
}
