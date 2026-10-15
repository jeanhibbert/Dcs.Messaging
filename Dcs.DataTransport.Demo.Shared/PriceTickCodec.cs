using System;
using System.Buffers.Binary;

namespace Dcs.DataTransport.Demo;

/// <summary>
/// Fixed 36-byte little-endian layout: no allocation, no reflection, and no per-field tags or lengths on the wire.
/// </summary>
public sealed class PriceTickCodec : IMessageCodec<PriceTick>
{
    public const int EncodedLength = sizeof(long) + sizeof(long) + sizeof(int) + sizeof(double) + sizeof(double);

    public static readonly PriceTickCodec Instance = new();

    public int MaxEncodedLength => EncodedLength;

    public int Encode(in PriceTick message, Span<byte> destination)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination, message.Sequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(8), message.PublishedAt);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16), message.CurrencyPairId);
        BinaryPrimitives.WriteDoubleLittleEndian(destination.Slice(20), message.Bid);
        BinaryPrimitives.WriteDoubleLittleEndian(destination.Slice(28), message.Ask);
        return EncodedLength;
    }

    public PriceTick Decode(ReadOnlySpan<byte> source)
    {
        return new PriceTick(
            BinaryPrimitives.ReadInt64LittleEndian(source),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(8)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16)),
            BinaryPrimitives.ReadDoubleLittleEndian(source.Slice(20)),
            BinaryPrimitives.ReadDoubleLittleEndian(source.Slice(28)));
    }
}
