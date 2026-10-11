using Dcs.Messaging;
using Dcs.Messaging.RequestResponse;
using Dcs.Messaging.Serialization;
using System;

namespace Dcs.Messaging.Common
{
    public abstract class MessagingSessionBuilderBase : IMessagingSessionBuilder
    {
        protected MessagingSessionBuilderBase(
            string sessionName,
            IEndpointDetailsProvider endpointDetailsProvider,
            string sessionId,
            IDisposable transport,
            IEndpointProvider endpointProvider)
            : this(sessionName, endpointDetailsProvider, sessionId, transport, endpointProvider, new ProtobufNetBinarySerializer())
        {
        }

        protected MessagingSessionBuilderBase(
            string sessionName,
            IEndpointDetailsProvider endpointDetailsProvider,
            string sessionId,
            IDisposable transport,
            IEndpointProvider endpointProvider,
            IBinarySerializer serializer)
        {
            EndpointDetailsProvider = endpointDetailsProvider;
            EndpointDetailsFactory = new MessageEndpointDetailsFactory();
            MessageFactory = new MessageFactory(sessionId ?? sessionName);
            Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            MessagingTransport = transport;

            MessagingService = new TcpMessagingService(endpointProvider);
            RequestResponder = new TcpRequestResponder(MessagingService, MessageFactory, Serializer);
        }

        public IEndpointDetailsFactory EndpointDetailsFactory { get; }

        public IEndpointDetailsProvider EndpointDetailsProvider { get; }

        public IMessageFactory MessageFactory { get; }

        public IMessagingService MessagingService { get; }

        public IDisposable MessagingTransport { get; }

        public IRequestResponder RequestResponder { get; }

        public IBinarySerializer Serializer { get; }
    }
}
