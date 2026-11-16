using LabOriginal.Neutron.Tools;
using Microsoft.Extensions.Logging;
using ProtoBuf;
using ProtoBuf.Meta;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace LabOriginal.Neutron.Client
{
    public class ProtobufAsyncClient<T> : IProtobufClient<T> where T : IBaseMessage
    {
        private TcpClient _client;
        private readonly string _connectionName;
        private readonly IClientProtoAsyncParser<T> _messageParser;
        private readonly IDictionary<int, TaskCompletionSource<T>> _asyncSend = new ConcurrentDictionary<int, TaskCompletionSource<T>>();
        private readonly ProtobuffClientConfig<T> _config;
        private readonly ILogger _log;
        private int _asyncSendId;
        public event EventHandler Connected;
        public event EventHandler Disconnected;
        private System.Timers.Timer _heartbeatTimer;
        private readonly T _heartBeatMsg;
        private CancellationTokenSource _cts;

        public Exception ConnectionException { get; set; }

        public ProtobufAsyncClient(IClientProtoAsyncParser<T> messageParser, ProtobuffClientConfig<T> config, ILogger log)
        {
            RuntimeTypeModel.Default.IncludeDateTimeKind = true;
            _messageParser = messageParser;
            _config = config;
            _log = log;
            _heartBeatMsg = config.HeartBeatMsg;
            _connectionName = $"ProtobuffSingleConnection_{config.Name}";

            if (config.HeartBeatMsg != null && config.HeartBeatInterval > 0)
            {
                _heartbeatTimer = new System.Timers.Timer
                {
                    AutoReset = true,
                    Interval = TimeSpan.FromSeconds(config.HeartBeatInterval).TotalMilliseconds
                };

                _heartbeatTimer.Elapsed += SendHeartBeat;
            }



        }

        private async Task Read(CancellationToken ctsToken)
        {
            _log.LogInformation($"{_connectionName} Starting Receiver read thread");

            try
            {
                while (!ctsToken.IsCancellationRequested)
                {
                    var baseMsg = Serializer.DeserializeWithLengthPrefix<T>(_client.GetStream(), PrefixStyle.Base128);

                    if (baseMsg == null) break;

                    if (baseMsg.AsyncSend)
                    {
                        if (_asyncSend.TryGetValue(baseMsg.AsyncId, out var taskCompletion))
                        {
                            if (!taskCompletion.TrySetResult(baseMsg))
                            {
                                _log.LogDebug($"{_connectionName} Cannot set result");
                            }
                        }
                    }
                    else
                    {
                        await _messageParser.ReceivedMsgAsync(baseMsg).ConfigureAwait(false);
                    }
                }
            }
            catch (IOException ioe)
            {
                _log.LogError(ioe, $"{_connectionName} caught IO exception in read thread");
                NetworkTools.CloseConnectionFully(_client);
            }
            catch (ThreadAbortException)
            {
                NetworkTools.CloseConnectionFully(_client);
            }
            catch (Exception exc)
            {
                _log.LogError(exc, $"{_connectionName} caught exception in read thread");
            }
            finally
            {
                NetworkTools.CloseConnectionFully(_client);
                ClearAsyncTasks();
                Disconnected?.Invoke(null, EventArgs.Empty);
            }

            _log.LogDebug($"{_connectionName} Stopped Receiver read thread");
        }

        private void SendHeartBeat(object sender, ElapsedEventArgs e)
        {
            SendMessage(_heartBeatMsg);
        }

        public IPEndPoint ConnectedEndpoint
        {
            get
            {
                IPEndPoint endpoint = null;
                try
                {
                    endpoint = (IPEndPoint)_client?.Client?.RemoteEndPoint;
                }
                catch
                {
                    // ignore and return null
                }

                return endpoint;
            }
        }

        public void Connect()
        {
            ConnectionException = null;

            if (!IsConnected)
            {
                foreach (var ipEndPoint in _config.IPEndPoints)
                {
                    try
                    {
                        _client = new TcpClient
                        {
                            NoDelay = true
                        };

                        _client.Connect(ipEndPoint);
                        ConnectionException = null;
                        Connected?.Invoke(null, EventArgs.Empty);
                        _heartbeatTimer?.Start();
                        _cts = new CancellationTokenSource();
                        Task.Run(() => Read(_cts.Token));

                        // we only want to connect to the first good connection
                        break;
                    }
                    catch (Exception ex)
                    {
                        ConnectionException = ex;
                        NetworkTools.CloseConnectionFully(_client);
                        _client = null;
                        _log.LogError(ex, $"{_connectionName} Connect");
                    }
                }
            }
        }

        public async Task ConnectAsync()
        {
            ConnectionException = null;

            if (!IsConnected)
            {
                foreach (var ipEndPoint in _config.IPEndPoints)
                {
                    try
                    {
                        _client = new TcpClient();
                        _client.NoDelay = true;
                        await _client.ConnectAsync(ipEndPoint.Address, ipEndPoint.Port);
                        ConnectionException = null;
                        Connected?.Invoke(null, EventArgs.Empty);
                        _cts = new CancellationTokenSource();
                        Task.Run(() => Read(_cts.Token));

                        // we only want to connect to the first good connection
                        break;
                    }
                    catch (Exception ex)
                    {
                        ConnectionException = ex;
                        if (_client != null)
                        {
                            NetworkTools.CloseConnectionFully(_client);
                            _client = null;
                        }
                        _log.LogError(ex, $"{_connectionName} Connect");
                    }
                }
            }
        }


        public void Disconnect()
        {
            _cts?.Cancel();
            _heartbeatTimer?.Stop();
            if (_client != null)
            {
                NetworkTools.CloseConnectionFully(_client);
                _client = null;

                //Invoke Disconnect only if we have actually disconnected
                Disconnected?.Invoke(null, EventArgs.Empty);
            }
        }

        public bool SendMessage(T message)
        {
            if (!IsConnected)
            {
                _log.LogWarning($"{_connectionName} connection is not connected");
                return false;
            }

            try
            {
                Serializer.SerializeWithLengthPrefix(_client.GetStream(), message, PrefixStyle.Base128);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, $"{_connectionName} unable to send data {message}");
                if (!IsConnected)
                {
                    // the connection could have died while sending the message. if not connected, then tear it down
                    Disconnect();
                }

                return false;
            }

            return true;
        }

        private int NextAsyncSendId()
        {
            return Interlocked.Increment(ref _asyncSendId);
        }

        public async Task<T> SendMessageAsync(T message)
        {
            return await SendMessageAsync(message, continueOnCapturedContext: true);
        }

        public async Task<T> SendMessageAsync(T message, bool continueOnCapturedContext)
        {
            if (!IsConnected)
                throw new IOException($"Failed to send message because protobuff client {_connectionName} is not connected");

            message.AsyncId = NextAsyncSendId();
            message.AsyncSend = true;

            var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            var ct = new CancellationTokenSource(_config.AsyncTimeout);

            _asyncSend.Add(message.AsyncId, tcs);

            T result;

            using (ct.Token.Register(HandleCancelled, new TaskCompletionWrapper<T>(message.AsyncId, tcs), useSynchronizationContext: true))
            {
                Serializer.SerializeWithLengthPrefix(_client.GetStream(), message, PrefixStyle.Base128);
                result = await tcs.Task.ConfigureAwait(continueOnCapturedContext);
            }

            _asyncSend.Remove(message.AsyncId);

            return result;
        }

        private void HandleCancelled(object taskCompletionSource)
        {
            var wrapper = (TaskCompletionWrapper<T>)taskCompletionSource;
            _asyncSend.Remove(wrapper.Id);
            wrapper.TaskCompletionSource.SetCanceled();
        }

        public bool IsConnected
        {
            get
            {
                if (_client == null) return false;

                return _client.Client != null && (_client?.Connected ?? false);
            }
        }

        public ProtobuffClientConfig<T> Config => _config;

        #region IDisposable Support

        private bool _disposedValue; // = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    Disconnect();

                    ClearAsyncTasks();

                    if (_heartbeatTimer != null)
                    {
                        _heartbeatTimer.Elapsed -= SendHeartBeat;
                        _heartbeatTimer.Dispose();
                        _heartbeatTimer = null;
                    }
                }

                _disposedValue = true;
            }
        }

        private void ClearAsyncTasks()
        {
            try
            {
                foreach (var taskComp in _asyncSend.Values)
                {
                    taskComp.SetCanceled();
                }

                _asyncSend.Clear();
            }
            catch (Exception ex)
            {
                _log.LogError(ex, $"{_connectionName} Caught exception while destroying m_asyncSend");
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
