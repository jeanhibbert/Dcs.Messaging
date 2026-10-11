using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Common.Sbe;
using Dcs.Messaging.Sbe;
using Dcs.Messaging.Serialization;

namespace Dcs.Messaging.UnitTests.Sbe;

public class SbeBinarySerializerTests
{
    private readonly SbeBinarySerializer _serializer = CommonSbeSchema.CreateSerializer();

    [Fact]
    public void ForwardPricingDto_RoundTrips()
    {
        var original = new ForwardPricingDto
        {
            CurrencyPair = new CurrencyPairDto { BaseCcy = "EUR", QuoteCcy = "USD", DecimalPlaces = 5 },
            BidRate = 1.08417,
            AskRate = 1.08487,
            IsContra = true
        };

        var restored = _serializer.Deserialize<ForwardPricingDto>(_serializer.Serialize(original));

        Assert.Equal("EUR", restored.CurrencyPair.BaseCcy);
        Assert.Equal("USD", restored.CurrencyPair.QuoteCcy);
        Assert.Equal(5, restored.CurrencyPair.DecimalPlaces);
        Assert.Equal(original.BidRate, restored.BidRate);
        Assert.Equal(original.AskRate, restored.AskRate);
        Assert.True(restored.IsContra);
    }

    [Fact]
    public void ForwardPricingDto_IsFixedLength()
    {
        var bytes = _serializer.Serialize(new ForwardPricingDto { CurrencyPair = new CurrencyPairDto { BaseCcy = "GBP", QuoteCcy = "JPY" } });

        // 8-byte message header + 27-byte block (bid, ask, currency pair composite, isContra).
        Assert.Equal(35, bytes.Length);
    }

    [Fact]
    public void ForwardPricingDto_NullCurrencyPair_RoundTripsAsNull()
    {
        var restored = _serializer.Deserialize<ForwardPricingDto>(_serializer.Serialize(new ForwardPricingDto { BidRate = 1 }));

        Assert.Null(restored.CurrencyPair);
        Assert.Equal(1, restored.BidRate);
    }

    [Fact]
    public void PricingRequestDto_RoundTrips()
    {
        var original = new PricingRequestDto
        {
            CurrencyPair = new CurrencyPairDto { BaseCcy = "EUR", QuoteCcy = "USD", DecimalPlaces = 5 },
            RequestedBy = "client-é",
            SpotRate = 1.08452,
            Tenor = "3M"
        };

        var restored = _serializer.Deserialize<PricingRequestDto>(_serializer.Serialize(original));

        Assert.Equal("EUR", restored.CurrencyPair.BaseCcy);
        Assert.Equal("USD", restored.CurrencyPair.QuoteCcy);
        Assert.Equal(5, restored.CurrencyPair.DecimalPlaces);
        Assert.Equal("client-é", restored.RequestedBy);
        Assert.Equal(1.08452, restored.SpotRate);
        Assert.Equal("3M", restored.Tenor);
    }

    [Fact]
    public void TestAlertDto_RoundTrips()
    {
        var original = new TestAlertDto { MessageId = 42, ServerName = "server-1", Message = "hello" };

        var restored = _serializer.Deserialize<TestAlertDto>(_serializer.Serialize(original));

        Assert.Equal(42, restored.MessageId);
        Assert.Equal("server-1", restored.ServerName);
        Assert.Equal("hello", restored.Message);
    }

    [Fact]
    public void TestAlertDto_NullStrings_DecodeAsEmpty()
    {
        var restored = _serializer.Deserialize<TestAlertDto>(_serializer.Serialize(new TestAlertDto { MessageId = 1 }));

        Assert.Equal(string.Empty, restored.ServerName);
        Assert.Equal(string.Empty, restored.Message);
    }

    [Fact]
    public void Serialize_UnregisteredType_ThrowsNotSupportedException()
    {
        Assert.Throws<NotSupportedException>(() => _serializer.Serialize(new CurrencyPairDto()));
    }

    [Fact]
    public void Serialize_CurrencyCodeLongerThanThreeCharacters_Throws()
    {
        var dto = new ForwardPricingDto { CurrencyPair = new CurrencyPairDto { BaseCcy = "EURO", QuoteCcy = "USD" } };

        Assert.Throws<ArgumentException>(() => _serializer.Serialize(dto));
    }

    [Fact]
    public void Deserialize_DifferentTemplate_ThrowsInvalidDataException()
    {
        var alertBytes = _serializer.Serialize(new TestAlertDto { MessageId = 1 });

        Assert.Throws<InvalidDataException>(() => _serializer.Deserialize<ForwardPricingDto>(alertBytes));
    }

    [Fact]
    public void Deserialize_TruncatedPayload_ThrowsInvalidDataException()
    {
        var bytes = _serializer.Serialize(new TestAlertDto { MessageId = 1, ServerName = "server", Message = "hello" });

        Assert.Throws<InvalidDataException>(() => _serializer.Deserialize<TestAlertDto>(bytes[..^1]));
    }

    [Fact]
    public void Deserialize_NewerVersionWithExtendedBlock_SkipsUnknownFields()
    {
        var newer = new SbeBinarySerializer(new TestAlertV1Codec());
        var bytes = newer.Serialize(new TestAlertDto { MessageId = 7, ServerName = "server", Message = "hello" });

        var restored = _serializer.Deserialize<TestAlertDto>(bytes);

        Assert.Equal(7, restored.MessageId);
        Assert.Equal("server", restored.ServerName);
        Assert.Equal("hello", restored.Message);
    }

    [Fact]
    public void Constructor_DuplicateCodec_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SbeBinarySerializer(new TestAlertDtoSbeCodec(), new TestAlertDtoSbeCodec()));
    }

    /// <summary>
    /// Simulates schema version 1 of TestAlert, which appends an int64 timestamp to the root block.
    /// </summary>
    private sealed class TestAlertV1Codec : SbeCodec<TestAlertDto>
    {
        public TestAlertV1Codec()
            : base(CommonSbeSchema.SchemaId, CommonSbeSchema.TestAlertTemplateId, sizeof(int) + sizeof(long), version: 1)
        {
        }

        protected override int GetVariableDataLength(TestAlertDto value)
        {
            return SbeWriter.GetVarStringLength(value.ServerName) + SbeWriter.GetVarStringLength(value.Message);
        }

        protected override void Encode(TestAlertDto value, ref SbeWriter writer)
        {
            writer.WriteInt32(value.MessageId);
            writer.WriteInt64(DateTime.UtcNow.Ticks);
            writer.WriteVarString(value.ServerName);
            writer.WriteVarString(value.Message);
        }

        protected override TestAlertDto Decode(ref SbeReader reader, ushort actingVersion)
        {
            throw new NotSupportedException();
        }
    }
}
