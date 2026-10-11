using Dcs.Messaging;
using Dcs.Messaging.Transport;
using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.Messaging.Sbe
{
    /// <summary>
    /// Simple Binary Encoding transport: each frame is a Simple Open Framing Header followed by an SBE
    /// <c>TransportEnvelope</c> message, exchanged over raw TCP sockets. See <see cref="SbeTransportCodec"/>.
    /// </summary>
    public sealed class SbeMessagingSession : TcpTransportSessionBase
    {
        public SbeMessagingSession(SbeSessionOptions options)
            : base(
                (options ?? throw new ArgumentNullException(nameof(options))).Host,
                options.Port,
                options.Mode == SbeSessionMode.Server,
                options.SessionId)
        {
        }

        private protected override byte[] EncodeFrame(string endpointAddress, IMessage message, string targetSessionId)
        {
            return SbeTransportCodec.EncodeFrame(endpointAddress, message, targetSessionId);
        }

        private protected override Task<IncomingTransportMessage> ReadFrameAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            return SbeTransportCodec.ReadFrameAsync(stream, cancellationToken);
        }
    }
}
