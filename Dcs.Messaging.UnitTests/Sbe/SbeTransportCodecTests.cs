using Dcs.Messaging;
using Dcs.Messaging.Sbe;
using Dcs.Messaging.Transport;
using System.Buffers.Binary;
using System.Text;

namespace Dcs.Messaging.UnitTests.Sbe;

public class SbeTransportCodecTests
{
    [Fact]
    public void EncodeFrame_WritesSimpleOpenFramingHeader()
    {
        var frame = SbeTransportCodec.EncodeFrame("ep", CreateMessage(new MessageProperties()), null);

        Assert.Equal((uint)frame.Length, BinaryPrimitives.ReadUInt32BigEndian(frame));
        Assert.Equal(SbeTransportCodec.SofhEncodingType, BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(4)));
    }

    [Fact]
    public void EncodeFrame_WritesLittleEndianSbeMessageHeader()
    {
        var frame = SbeTransportCodec.EncodeFrame("ep", CreateMessage(new MessageProperties()), null);
        var header = frame.AsSpan(SbeTransportCodec.SofhLength);

        Assert.Equal(SbeTransportCodec.TransportEnvelopeBlockLength, BinaryPrimitives.ReadUInt16LittleEndian(header));
        Assert.Equal(SbeTransportCodec.TransportEnvelopeTemplateId, BinaryPrimitives.ReadUInt16LittleEndian(header.Slice(2)));
        Assert.Equal(SbeTransportCodec.SchemaId, BinaryPrimitives.ReadUInt16LittleEndian(header.Slice(4)));
        Assert.Equal(SbeTransportCodec.SchemaVersion, BinaryPrimitives.ReadUInt16LittleEndian(header.Slice(6)));
    }

    [Fact]
    public void RoundTrip_PreservesEnvelopeFields()
    {
        var payload = Encoding.UTF8.GetBytes("payload éè");
        var message = new Message("sender-1", "tag-ü", new MessageProperties(), payload);

        var incoming = RoundTrip("test/endpoint", message, "target-1");

        Assert.Equal("test/endpoint", incoming.EndpointAddress);
        Assert.Equal("target-1", incoming.TargetSessionId);
        Assert.Equal("sender-1", incoming.Message.SenderSessionId);
        Assert.Equal("tag-ü", incoming.Message.Tag);
        Assert.Equal(payload, incoming.Message.Payload);
    }

    [Fact]
    public void RoundTrip_PreservesTypedProperties()
    {
        var properties = new MessageProperties();
        properties.Set("b", true);
        properties.Set("i", -42);
        properties.Set("d", 1.08452);
        properties.Set("s", "correlation");

        var restored = RoundTrip("ep", CreateMessage(properties), null).Message.Properties;

        var values = restored.Keys.Zip(restored.Values).ToDictionary(pair => pair.First, pair => pair.Second);
        Assert.Equal(true, values["b"]);
        Assert.Equal(-42, values["i"]);
        Assert.Equal(1.08452, values["d"]);
        Assert.Equal("correlation", values["s"]);
    }

    [Fact]
    public void RoundTrip_NonPrimitivePropertyValue_IsSentAsInvariantString()
    {
        var properties = new MessageProperties(new Dictionary<string, object> { ["amount"] = 12.5m });

        var restored = RoundTrip("ep", CreateMessage(properties), null).Message.Properties;

        Assert.Equal("12.5", restored.GetString("amount"));
    }

    [Fact]
    public void RoundTrip_FactoryProperties_UseFixedBlockFields()
    {
        var message = new MessageFactory("sender").Create(Array.Empty<byte>(), TimeSpan.FromSeconds(5), true);

        var frame = SbeTransportCodec.EncodeFrame("ep", message, null);
        var restored = SbeTransportCodec.DecodeMessage(frame.AsSpan(SbeTransportCodec.SofhLength)).Message.Properties;

        var groupHeaderOffset = SbeTransportCodec.SofhLength + SbeMessageHeader.EncodedLength + SbeTransportCodec.TransportEnvelopeBlockLength;
        Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(groupHeaderOffset + 2)));
        Assert.Equal(5000, restored.GetInt32(TransportPropertyKeys.TimeToLiveMs));
        Assert.True(restored.GetBoolean(TransportPropertyKeys.IsPersistent));
    }

    [Fact]
    public void RoundTrip_AbsentFactoryProperties_RemainAbsent()
    {
        var restored = RoundTrip("ep", CreateMessage(new MessageProperties()), null).Message.Properties;

        Assert.Empty(restored.Keys);
    }

    [Fact]
    public void RoundTrip_WellKnownKeysWithUnexpectedValues_FallBackToPropertyGroup()
    {
        var properties = new MessageProperties();
        properties.Set(TransportPropertyKeys.TimeToLiveMs, int.MinValue);
        properties.Set(TransportPropertyKeys.IsPersistent, "yes");

        var restored = RoundTrip("ep", CreateMessage(properties), null).Message.Properties;

        Assert.Equal(int.MinValue, restored.GetInt32(TransportPropertyKeys.TimeToLiveMs));
        Assert.Equal("yes", restored.GetString(TransportPropertyKeys.IsPersistent));
    }

    [Fact]
    public void RoundTrip_NullTargetAndEmptyPayload_DecodeAsEmpty()
    {
        var incoming = RoundTrip("ep", new Message("s", null, new MessageProperties(), null), null);

        Assert.Equal(string.Empty, incoming.TargetSessionId);
        Assert.Equal(string.Empty, incoming.Message.Tag);
        Assert.Empty(incoming.Message.Payload);
    }

    [Fact]
    public void DecodeMessage_TruncatedBuffer_ThrowsInvalidDataException()
    {
        var frame = SbeTransportCodec.EncodeFrame("ep", CreateMessage(new MessageProperties()), null);
        var truncated = frame.AsSpan(SbeTransportCodec.SofhLength, frame.Length - SbeTransportCodec.SofhLength - 1).ToArray();

        Assert.Throws<InvalidDataException>(() => SbeTransportCodec.DecodeMessage(truncated));
    }

    [Fact]
    public void DecodeMessage_UnknownTemplate_ThrowsInvalidDataException()
    {
        var frame = SbeTransportCodec.EncodeFrame("ep", CreateMessage(new MessageProperties()), null);
        var body = frame.AsSpan(SbeTransportCodec.SofhLength).ToArray();
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(2), 99);

        Assert.Throws<InvalidDataException>(() => SbeTransportCodec.DecodeMessage(body));
    }

    [Fact]
    public void EncodeFrame_TypicalResponse_IsSmallerThanProtobufFrame()
    {
        var properties = new MessageProperties();
        properties.Set(TransportPropertyKeys.TimeToLiveMs, 10000);
        properties.Set(TransportPropertyKeys.IsPersistent, false);
        properties.Set(TransportPropertyKeys.CorrelationId, Guid.NewGuid().ToString("N"));
        var message = new Message(Guid.NewGuid().ToString(), string.Empty, properties, new byte[32]);

        var sbeFrame = SbeTransportCodec.EncodeFrame("Test/Pricing/Responses", message, Guid.NewGuid().ToString());
        var protobufFrame = TransportTcpFraming.SerializeLengthPrefixed(
            TransportEnvelopeFactory.FromMessage("Test/Pricing/Responses", message, Guid.NewGuid().ToString()));

        Assert.True(sbeFrame.Length < protobufFrame.Length, $"SBE frame {sbeFrame.Length} bytes vs protobuf frame {protobufFrame.Length} bytes");
    }

    private static IncomingTransportMessage RoundTrip(string endpointAddress, IMessage message, string? targetSessionId)
    {
        var frame = SbeTransportCodec.EncodeFrame(endpointAddress, message, targetSessionId!);
        return SbeTransportCodec.DecodeMessage(frame.AsSpan(SbeTransportCodec.SofhLength));
    }

    private static Message CreateMessage(MessageProperties properties)
    {
        return new Message("sender", "tag", properties, new byte[] { 1, 2, 3 });
    }
}
