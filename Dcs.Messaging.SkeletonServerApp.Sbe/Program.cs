using Dcs.Messaging.Common;
using Dcs.Messaging.Sbe;
using Dcs.Messaging.SkeletonServerApp.Services;
using System;

namespace Dcs.Messaging.SkeletonServerApp.Sbe
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            var endpointDetailsProvider = new TestEndpointDetailsProvider();
            var serverId = Guid.NewGuid().ToString();
            var sessionOptions = SbeSessionOptions.CreateServer("127.0.0.1", 5050, serverId);
            var sessionBuilder = new SbeMessagingSessionBuilder("ServerApp", endpointDetailsProvider, sessionOptions);

            using (sessionBuilder.MessagingTransport)
            {
                using (var alertService = new AlertService(serverId, sessionBuilder))
                using (var pricingService = new PricingService(sessionBuilder))
                {
                    pricingService.Start();
                    alertService.Start();
                    Console.WriteLine("[SBE] Server listening on 127.0.0.1:5050 with session {0}", serverId);
                    Console.WriteLine("Press ENTER to stop service...");
                    Console.ReadLine();
                }
            }
        }
    }
}
