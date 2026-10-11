using Dcs.Messaging;
using Dcs.Messaging.Common.Sbe;
using Dcs.Messaging.Sbe;
using Dcs.Messaging.Serialization;

namespace Dcs.Messaging.Common
{
    /// <summary>
    /// Builds a session that uses Simple Binary Encoding end to end: SBE transport envelopes over TCP and an
    /// <see cref="SbeBinarySerializer"/> for application payloads.
    /// </summary>
    public sealed class SbeMessagingSessionBuilder : MessagingSessionBuilderBase
    {
        /// <summary>
        /// Uses <see cref="CommonSbeSchema.CreateSerializer"/>, which covers the sample DTOs.
        /// </summary>
        public SbeMessagingSessionBuilder(
            string sessionName,
            IEndpointDetailsProvider endpointDetailsProvider,
            SbeSessionOptions sessionOptions)
            : this(sessionName, endpointDetailsProvider, sessionOptions, CommonSbeSchema.CreateSerializer())
        {
        }

        /// <summary>
        /// Uses the supplied serializer, which must have a codec registered for every payload type the application sends.
        /// </summary>
        public SbeMessagingSessionBuilder(
            string sessionName,
            IEndpointDetailsProvider endpointDetailsProvider,
            SbeSessionOptions sessionOptions,
            SbeBinarySerializer serializer)
            : this(sessionName, endpointDetailsProvider, sessionOptions, serializer, new SbeMessagingSession(sessionOptions))
        {
        }

        private SbeMessagingSessionBuilder(
            string sessionName,
            IEndpointDetailsProvider endpointDetailsProvider,
            SbeSessionOptions sessionOptions,
            SbeBinarySerializer serializer,
            SbeMessagingSession session)
            : base(
                sessionName,
                endpointDetailsProvider,
                sessionOptions.SessionId,
                session,
                new SbeEndpointProvider(session),
                serializer)
        {
        }
    }
}
