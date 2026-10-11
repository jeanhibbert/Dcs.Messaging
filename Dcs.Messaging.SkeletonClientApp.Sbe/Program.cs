using Dcs.Messaging.Common;
using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Sbe;
using Dcs.Messaging;
using System;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;

namespace Dcs.Messaging.SkeletonClientApp.Sbe
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            var clientId = Guid.NewGuid().ToString();
            var endpointDetailsProvider = new TestEndpointDetailsProvider();
            var sessionOptions = SbeSessionOptions.CreateClient("127.0.0.1", 5050, clientId);
            var sessionBuilder = new SbeMessagingSessionBuilder("ClientApp", endpointDetailsProvider, sessionOptions);

            using (sessionBuilder.MessagingTransport)
            using (var alertSubscription = SubscribeToAlerts(sessionBuilder))
            {
                Console.WriteLine("[SBE] Client connected with session {0}", clientId);
                RequestPricing(sessionBuilder, clientId);
                Console.WriteLine("Press ENTER to stop client...");
                Console.ReadLine();
            }
        }

        private static IDisposable SubscribeToAlerts(IMessagingSessionBuilder sessionBuilder)
        {
            var subscriptions = new CompositeDisposable();
            var streamSubscription = new SerialDisposable();
            subscriptions.Add(streamSubscription);
            var endpointDetails = sessionBuilder.EndpointDetailsProvider.GetEndpointDetails(EndpointKeys.TestAlerts);

            subscriptions.Add(sessionBuilder.MessagingService
                .GetMessageStream(endpointDetails)
                .Subscribe(stream =>
                {
                    streamSubscription.Disposable = stream.Messages.Subscribe(message =>
                    {
                        var alert = sessionBuilder.Serializer.Deserialize<TestAlertDto>(message.Payload);
                        Console.WriteLine("Alert #{0} from {1}: {2}", alert.MessageId, alert.ServerName, alert.Message);
                    });
                }));

            return subscriptions;
        }

        private static void RequestPricing(IMessagingSessionBuilder sessionBuilder, string clientId)
        {
            var responseEndpoint = sessionBuilder.EndpointDetailsProvider.GetEndpointDetails(EndpointKeys.TestPricingResponses);
            var requestEndpoint = sessionBuilder.EndpointDetailsProvider.GetEndpointDetails(EndpointKeys.TestPricingRequests);
            var correlationId = Guid.NewGuid().ToString("N");
            var responseReceived = new ManualResetEventSlim(false);
            var streamReady = new ManualResetEventSlim(false);
            Exception requestFailure = null;

            var responseStreamSubscription = new SerialDisposable();
            var messageStreamSubscription = sessionBuilder.MessagingService
                .GetMessageStream(responseEndpoint)
                .Subscribe(stream =>
                {
                    responseStreamSubscription.Disposable = stream.Messages
                        .Where(message => string.Equals(message.Properties.GetString(TransportPropertyKeys.CorrelationId), correlationId, StringComparison.Ordinal))
                        .Take(1)
                        .Subscribe(message =>
                        {
                            if (message.Properties.GetBoolean(TransportPropertyKeys.IsError))
                            {
                                requestFailure = new InvalidOperationException(message.Properties.GetString(TransportPropertyKeys.ErrorMessage));
                            }
                            else
                            {
                                var response = sessionBuilder.Serializer.Deserialize<ForwardPricingDto>(message.Payload);
                                Console.WriteLine(
                                    "Pricing response for {0}/{1}: bid {2:F5}, ask {3:F5}, contra={4}",
                                    response.CurrencyPair.BaseCcy,
                                    response.CurrencyPair.QuoteCcy,
                                    response.BidRate,
                                    response.AskRate,
                                    response.IsContra);
                            }

                            responseReceived.Set();
                        });
                    streamReady.Set();
                });

            if (!streamReady.Wait(TimeSpan.FromSeconds(5)))
            {
                throw new TimeoutException("Timed out waiting to subscribe to the pricing response endpoint.");
            }

            var request = new PricingRequestDto
            {
                CurrencyPair = new CurrencyPairDto
                {
                    BaseCcy = "EUR",
                    QuoteCcy = "USD",
                    DecimalPlaces = 5
                },
                RequestedBy = clientId,
                SpotRate = 1.08452,
                Tenor = "3M"
            };

            var payload = sessionBuilder.Serializer.Serialize(request);
            var message = sessionBuilder.MessageFactory.Create(payload, TimeSpan.FromSeconds(10), false);
            message.Properties.Set(TransportPropertyKeys.CorrelationId, correlationId);
            message.Properties.Set(TransportPropertyKeys.MessageKind, MessageKinds.Request);
            sessionBuilder.MessagingService.Send(message, requestEndpoint);

            if (!responseReceived.Wait(TimeSpan.FromSeconds(10)))
            {
                throw new TimeoutException("Timed out waiting for pricing response from server.");
            }

            responseStreamSubscription.Dispose();
            messageStreamSubscription.Dispose();
            responseReceived.Dispose();
            streamReady.Dispose();

            if (requestFailure != null)
            {
                throw requestFailure;
            }
        }
    }
}
