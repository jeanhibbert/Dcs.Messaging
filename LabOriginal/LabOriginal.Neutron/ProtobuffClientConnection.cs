using LabOriginal.Neutron.Tools;
using ProtoBuf;
using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Timers;

namespace LabOriginal.Neutron
{
    public class ProtobuffClientConnection<T> : IProtobuffClientConnection<T> where T : IBaseMessage
    {
        private bool _disposedValue; // = false; // To detect redundant calls
        private readonly T _heartBeatMsg;
        private readonly IServerProtoMessageParser<T> _messageParser;
        //private ServerMutualAuthenticator _serverMutualAuthenticator;
        private bool _shouldStop;
        private readonly TcpClient _tcpClient;
        private System.Timers.Timer _timer;
        private readonly IProtobuffAsyncServerCallBack<T> _callBack;
        public event EventHandler<IProtobuffClientConnection<T>> OnClientDisconnection;

        public ProtobuffClientConnection(IProtobuffAsyncServerCallBack<T> callBack, string id, TcpClient tcpClient, IServerProtoMessageParser<T> messageParser, T heartBeatMsg, int heartBeatIntervalInSec)
        {
            _tcpClient = tcpClient;
            Id = id;
            _callBack = callBack;

            _tcpClient.NoDelay = true;
            _messageParser = messageParser;

            _heartBeatMsg = heartBeatMsg;
            if (_heartBeatMsg != null && heartBeatIntervalInSec > 0)
            {
                _timer = new Timer
                {
                    AutoReset = true,
                    Interval = TimeSpan.FromSeconds(heartBeatIntervalInSec).TotalMilliseconds
                };
                _timer.Elapsed += SendHeartBeat;
            }
        }

        public void Start()
        {
            _timer?.Start();

            Task.Run(Read);

        }

        private async Task Read()
        {
            try
            {
                while (!_shouldStop)
                {
                    var baseMessage =
                        Serializer.DeserializeWithLengthPrefix<T>(_tcpClient.GetStream(), PrefixStyle.Base128);
                    if (baseMessage == null)
                        break;
                    await _messageParser.ReceivedMsgAsync(Id, baseMessage).ConfigureAwait(false);
                }
            }
            catch (AggregateException aex)
            {
                foreach (var innerException in aex.Flatten().InnerExceptions)
                {
                    Trace.TraceError($"Error reading from connection_id={Id} {innerException}");
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError($"Error reading from connection_id={Id} {ex}");
            }
            finally
            {
                NetworkTools.CloseConnectionFully(_tcpClient);
                _callBack.RemoveClientConnection(Id);
                Dispose();
                Trace.TraceInformation($"connection_id={Id} {nameof(Read)}: has stopped");
                OnClientDisconnection?.Invoke(this, this);
            }
        }

        private void SendHeartBeat(object sender, ElapsedEventArgs e)
        {
            SendMessage(_heartBeatMsg);
        }

        /// <summary>
        /// external code expecting exception inc tests. But must always get return also. RPI
        /// </summary>
        public bool SendMessage(T message)
        {
            var status = false;

            try
            {
                if (IsConnected)
                {
                    Serializer.SerializeWithLengthPrefix(_tcpClient.GetStream(), message, PrefixStyle.Base128);
                    status = true;
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError($"ProtobuffClientConnection failed to serialize := {ex}");
                throw;
            }

            return status;
        }

        public string Id { get; }

        public bool IsConnected => _tcpClient != null && _tcpClient.Client != null && _tcpClient.Client.Connected;

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            _shouldStop = true;

            if (!_disposedValue)
            {
                if (disposing)
                {
                    _timer?.Stop();
                    if (_timer != null) _timer.Elapsed -= SendHeartBeat;
                    _timer?.Dispose();
                    _timer = null;
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public bool Disconnect()
        {
            try
            {
                NetworkTools.CloseConnectionFully(_tcpClient);
                return true;
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Disconnect Error for {Id} exception={ex}", LoggingConstants.TRACELEVEL_ERROR);
                return false;
            }
        }

        #endregion
    }
}
