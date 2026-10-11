using Dcs.Messaging;
using Dcs.Messaging.Common;
using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Sbe;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;

namespace Dcs.Messaging.UnitTests.Sbe;

public class SbeMessagingSessionTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task RequestAndTargetedResponse_RoundTripOverLoopback()
    {
        var port = GetFreePort();
        var endpoints = new TestEndpointDetailsProvider();
        var requestEndpoint = endpoints.GetEndpointDetails(EndpointKeys.TestPricingRequests);
        var responseEndpoint = endpoints.GetEndpointDetails(EndpointKeys.TestPricingResponses);

        var server = new SbeMessagingSessionBuilder("Server", endpoints, SbeSessionOptions.CreateServer("127.0.0.1", port, "server-session"));
        using var serverTransport = server.MessagingTransport;
        var client = new SbeMessagingSessionBuilder("Client", endpoints, SbeSessionOptions.CreateClient("127.0.0.1", port, "client-session"));
        using var clientTransport = client.MessagingTransport;

        var requestReceived = new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var responseReceived = new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var serverStream = await server.MessagingService.GetMessageStream(requestEndpoint);
        using var serverSubscription = serverStream.Messages.Subscribe(request =>
        {
            requestReceived.TrySetResult(request);
            var pricingRequest = server.Serializer.Deserialize<PricingRequestDto>(request.Payload);
            var response = server.MessageFactory.Create(
                server.Serializer.Serialize(new ForwardPricingDto
                {
                    CurrencyPair = pricingRequest.CurrencyPair,
                    BidRate = pricingRequest.SpotRate - 0.0001,
                    AskRate = pricingRequest.SpotRate + 0.0001
                }),
                TimeSpan.Zero,
                false);
            response.Properties.Set(TransportPropertyKeys.CorrelationId, request.Properties.GetString(TransportPropertyKeys.CorrelationId));
            server.MessagingService.Send(response, responseEndpoint, request.SenderSessionId);
        });

        var clientStream = await client.MessagingService.GetMessageStream(responseEndpoint);
        using var clientSubscription = clientStream.Messages.Subscribe(response => responseReceived.TrySetResult(response));

        var message = client.MessageFactory.Create(
            client.Serializer.Serialize(new PricingRequestDto
            {
                CurrencyPair = new CurrencyPairDto { BaseCcy = "EUR", QuoteCcy = "USD", DecimalPlaces = 5 },
                RequestedBy = "client-session",
                SpotRate = 1.08452,
                Tenor = "3M"
            }),
            TimeSpan.FromSeconds(10),
            true);
        message.Properties.Set(TransportPropertyKeys.CorrelationId, "corr-1");
        await client.MessagingService.Send(message, requestEndpoint);

        var receivedRequest = await requestReceived.Task.WaitAsync(Timeout);
        Assert.Equal("client-session", receivedRequest.SenderSessionId);
        Assert.Equal(10000, receivedRequest.Properties.GetInt32(TransportPropertyKeys.TimeToLiveMs));
        Assert.True(receivedRequest.Properties.GetBoolean(TransportPropertyKeys.IsPersistent));

        var receivedResponse = await responseReceived.Task.WaitAsync(Timeout);
        Assert.Equal("server-session", receivedResponse.SenderSessionId);
        Assert.Equal("corr-1", receivedResponse.Properties.GetString(TransportPropertyKeys.CorrelationId));
        var pricing = client.Serializer.Deserialize<ForwardPricingDto>(receivedResponse.Payload);
        Assert.Equal("EUR", pricing.CurrencyPair.BaseCcy);
        Assert.Equal(1.08442, pricing.BidRate, 10);
        Assert.Equal(1.08462, pricing.AskRate, 10);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
