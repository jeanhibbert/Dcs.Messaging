using Dcs.Messaging.Common;
using Dcs.Messaging.Sbe;
using Dcs.Messaging.Serialization;

namespace Dcs.Messaging.UnitTests.Builders;

public class SbeMessagingSessionBuilderTests : IDisposable
{
    private readonly SbeMessagingSessionBuilder _builder;

    public SbeMessagingSessionBuilderTests()
    {
        var options = SbeSessionOptions.CreateServer("127.0.0.1", 15052, "sbe-test-session");
        _builder = new SbeMessagingSessionBuilder("SbeTestApp", new TestEndpointDetailsProvider(), options);
    }

    public void Dispose()
    {
        _builder.MessagingTransport?.Dispose();
    }

    [Fact]
    public void Constructor_SetsMessagingTransportToSbeSession()
    {
        Assert.NotNull(_builder.MessagingTransport);
        Assert.IsType<SbeMessagingSession>(_builder.MessagingTransport);
    }

    [Fact]
    public void Constructor_SetsMessagingService()
    {
        Assert.NotNull(_builder.MessagingService);
    }

    [Fact]
    public void Constructor_SetsSerializerToSbeSerializer()
    {
        Assert.NotNull(_builder.Serializer);
        Assert.IsType<SbeBinarySerializer>(_builder.Serializer);
    }

    [Fact]
    public void Constructor_SetsRequestResponder()
    {
        Assert.NotNull(_builder.RequestResponder);
    }

    [Fact]
    public void Constructor_SetsMessageFactory()
    {
        Assert.NotNull(_builder.MessageFactory);
    }

    [Fact]
    public void Constructor_SetsEndpointDetailsFactory()
    {
        Assert.NotNull(_builder.EndpointDetailsFactory);
    }

    [Fact]
    public void Constructor_SetsEndpointDetailsProvider()
    {
        Assert.NotNull(_builder.EndpointDetailsProvider);
        Assert.IsType<TestEndpointDetailsProvider>(_builder.EndpointDetailsProvider);
    }

    [Fact]
    public void ImplementsIMessagingSessionBuilder()
    {
        Assert.IsAssignableFrom<IMessagingSessionBuilder>(_builder);
    }
}
