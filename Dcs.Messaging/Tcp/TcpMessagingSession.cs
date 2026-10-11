using Dcs.Messaging;
using Dcs.Messaging.Transport;
using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.Messaging.Tcp
{
    /// <summary>
    /// Raw TCP transport: each frame is a 4-byte big-endian length prefix followed by a protobuf-serialized
    /// <see cref="TransportEnvelope"/>.
    /// </summary>
    public sealed class TcpMessagingSession : TcpTransportSessionBase
    {
        public TcpMessagingSession(TcpSessionOptions options)
            : base(
                (options ?? throw new ArgumentNullException(nameof(options))).Host,
                options.Port,
                options.Mode == TcpSessionMode.Server,
                options.SessionId)
        {
        }

        private protected override byte[] EncodeFrame(string endpointAddress, IMessage message, string targetSessionId)
        {
            var envelope = TransportEnvelopeFactory.FromMessage(endpointAddress, message, targetSessionId);
            return TransportTcpFraming.SerializeLengthPrefixed(envelope);
        }

        private protected override async Task<IncomingTransportMessage> ReadFrameAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            var envelope = await TransportTcpFraming.ReadEnvelopeAsync(stream, cancellationToken).ConfigureAwait(false);
            if (envelope == null)
            {
                return null;
            }

            var message = TransportEnvelopeFactory.ToMessage(envelope);
            return new IncomingTransportMessage(envelope.EndpointAddress, envelope.TargetSessionId, message);
        }
    }
}
