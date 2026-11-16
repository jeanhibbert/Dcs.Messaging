using LabOriginal.Neutron.Tools;
using ProtoBuf.Meta;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace LabOriginal.Neutron
{
    // NOTE: only the middle of this file (Stop() through Dispose(bool)) was available from the original.
    // The sections marked RECONSTRUCTED were not visible and are inferred from ProtobuffAsyncServer,
    // using Trace instead of ILogger to match the visible part of this class.
    public class ProtobuffHandshakeServer<T> : IProtobuffAsyncServer<T> where T : IBaseMessage
    {
        #region RECONSTRUCTED - fields, constructor, Start, AcceptConnections

        private readonly IPEndPoint _listenerAddress;
        private CancellationTokenSource _cts;
        private readonly IServerProtoMessageParser<T> _messageParser;
        private readonly T _heartBeatMsg;
        private bool _disposedValue = false; // To detect redundant calls
        private readonly int _heartBeatIntervalInSec;
        private readonly TcpListener _listener;
        public event EventHandler<IProtobuffClientConnection<T>> OnClientConnection;
        public event EventHandler<IProtobuffClientConnection<T>> OnClientDisconnection;
        public event EventHandler<Exception> OnServerError;

        public ProtobuffHandshakeServer(IServerProtoMessageParser<T> messageParser, ProtobuffServerConfig<T> config)
        {
            RuntimeTypeModel.Default.IncludeDateTimeKind = true;
            _listenerAddress = config.ListenerAddress;
            _messageParser = messageParser;
            _heartBeatMsg = config.HeartBeatMsg;
            _heartBeatIntervalInSec = config.HeartBeatIntervalInSec;
            _listener = new TcpListener(_listenerAddress);
        }

        public IDictionary<string, IProtobuffClientConnection<T>> ClientConnections { get; set; } = new ConcurrentDictionary<string, IProtobuffClientConnection<T>>();

        public void Start()
        {
            Trace.WriteLine($"Starting ProtoBuff listener on {_listenerAddress}");

            _listener.Start();
            _cts = new CancellationTokenSource();
            Task.Run(() => AcceptConnections(_cts.Token));
        }

        private async Task AcceptConnections(CancellationToken ctsToken)
        {
            while (!ctsToken.IsCancellationRequested)
            {
                TcpClient tcpClient = await _listener.AcceptTcpClientAsync();

                HandleConnection(tcpClient);
            }
        }

        #endregion

        public void Stop()
        {
            Trace.WriteLine("Stopping ProtoBuff listener");
            _cts.Cancel();
            _listener.Stop();
            DisconnectAll();
        }

        void HandleServerException(Exception ex)
        {
            OnServerError?.Invoke(this, ex);
        }

        public IPEndPoint ListenerAddress => _listenerAddress;
        public int ConnectedClientCount => ClientConnections.Count;

        private void DisconnectAll()
        {
            foreach (var connection in ClientConnections.Values)
            {
                Disconnect(connection);
            }

        }

        public bool Disconnect(IProtobuffClientConnection<T> connection)
        {
            bool result = false;
            IProtobuffClientConnection<T> localConnection;
            if (ClientConnections.TryGetValue(connection.Id, out localConnection))
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
            try
            {
                var ipAddress = ((IPEndPoint)tcpClient.Client.RemoteEndPoint).Address;
                string port = ((IPEndPoint)tcpClient.Client.RemoteEndPoint).Port.ToString();

                string id = $"{Guid.NewGuid()}_{ipAddress}:{port}";

                var clientConnection = new ProtobuffClientConnection<T>(this, id, tcpClient, _messageParser, _heartBeatMsg, _heartBeatIntervalInSec);
                ClientConnections.Add(clientConnection.Id, clientConnection);
                clientConnection.OnClientDisconnection += HandleOnClientDisconnection;
                OnClientConnection?.Invoke(this, clientConnection);

                clientConnection.Start();
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"ProtobuffAsyncServer.HandleConnectionAsync exception {ex}", LoggingConstants.TRACELEVEL_ERROR);
                HandleServerException(ex);
            }
        }

        private void HandleOnClientDisconnection(object sender, IProtobuffClientConnection<T> e)
        {
            OnClientDisconnection?.Invoke(this, e);
        }

        public bool SendMessage(string id, T message)
        {
            if (ClientConnections.TryGetValue(id, out var connection))
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

        #region RECONSTRUCTED - Dispose, RemoveClientConnection

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
                Trace.WriteLine($"{id} Client Connection was not present");
            }
        }

        #endregion

        #endregion
    }
}
