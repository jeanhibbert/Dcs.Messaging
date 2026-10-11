using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Sbe;

namespace Dcs.Messaging.Common.Sbe
{
    /// <summary>
    /// <c>CurrencyPair</c> composite: two ISO 4217 <c>char[3]</c> codes and an optional <c>int32</c> precision.
    /// A <c>null</c> pair is written using the SBE null value of every field.
    /// </summary>
    internal static class CurrencyPairSbeComposite
    {
        public const int EncodedLength = CurrencyCodeLength + CurrencyCodeLength + sizeof(int);

        private const int CurrencyCodeLength = 3;
        private const int DecimalPlacesNullValue = int.MinValue;

        public static void Write(ref SbeWriter writer, CurrencyPairDto value)
        {
            writer.WriteFixedString(value?.BaseCcy, CurrencyCodeLength);
            writer.WriteFixedString(value?.QuoteCcy, CurrencyCodeLength);
            writer.WriteInt32(value?.DecimalPlaces ?? DecimalPlacesNullValue);
        }

        public static CurrencyPairDto Read(ref SbeReader reader)
        {
            var baseCcy = reader.ReadFixedString(CurrencyCodeLength);
            var quoteCcy = reader.ReadFixedString(CurrencyCodeLength);
            var decimalPlaces = reader.ReadInt32();

            if (baseCcy == null && quoteCcy == null && decimalPlaces == DecimalPlacesNullValue)
            {
                return null;
            }

            return new CurrencyPairDto
            {
                BaseCcy = baseCcy,
                QuoteCcy = quoteCcy,
                DecimalPlaces = decimalPlaces == DecimalPlacesNullValue ? 0 : decimalPlaces
            };
        }
    }
}
