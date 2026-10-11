using Dcs.Messaging;

namespace Dcs.Messaging.Sbe
{
    internal sealed class SbeEndpoint : IEndpoint
    {
        private readonly IEndpointDetails _endpointDetails;
        private readonly SbeMessagingSession _session;

        public SbeEndpoint(SbeMessagingSession session, IEndpointDetails endpointDetails)
        {
            _session = session;
            _endpointDetails = endpointDetails;
        }

        public IMessageStream MessageStream => _session.GetMessageStream(_endpointDetails);

        public void Send(IMessage message, string sessionId = null)
        {
            _session.Send(_endpointDetails, message, sessionId);
        }
    }
}