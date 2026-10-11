using Dcs.Messaging;
using System;
using System.Collections.Concurrent;

namespace Dcs.Messaging.Sbe
{
    public sealed class SbeEndpointProvider : IEndpointProvider
    {
        private readonly ConcurrentDictionary<string, IEndpoint> _endpoints;
        private readonly SbeMessagingSession _session;

        public SbeEndpointProvider(SbeMessagingSession session)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _endpoints = new ConcurrentDictionary<string, IEndpoint>(StringComparer.Ordinal);
        }

        public IEndpoint GetEndpoint(IEndpointDetails endpointDetails, bool createIfMissing = false)
        {
            return _endpoints.GetOrAdd(
                endpointDetails.Address,
                _ => new SbeEndpoint(_session, endpointDetails));
        }
    }
}