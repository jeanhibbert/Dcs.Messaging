using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Sbe;

namespace Dcs.Messaging.Common.Sbe
{
    public sealed class PricingRequestDtoSbeCodec : SbeCodec<PricingRequestDto>
    {
        private const ushort EncodedBlockLength = sizeof(double) + CurrencyPairSbeComposite.EncodedLength;

        public PricingRequestDtoSbeCodec()
            : base(CommonSbeSchema.SchemaId, CommonSbeSchema.PricingRequestTemplateId, EncodedBlockLength, CommonSbeSchema.Version)
        {
        }

        protected override int GetVariableDataLength(PricingRequestDto value)
        {
            return SbeWriter.GetVarStringLength(value.RequestedBy) + SbeWriter.GetVarStringLength(value.Tenor);
        }

        protected override void Encode(PricingRequestDto value, ref SbeWriter writer)
        {
            writer.WriteDouble(value.SpotRate);
            CurrencyPairSbeComposite.Write(ref writer, value.CurrencyPair);
            writer.WriteVarString(value.RequestedBy);
            writer.WriteVarString(value.Tenor);
        }

        protected override PricingRequestDto Decode(ref SbeReader reader, ushort actingVersion)
        {
            var dto = new PricingRequestDto
            {
                SpotRate = reader.ReadDouble(),
                CurrencyPair = CurrencyPairSbeComposite.Read(ref reader)
            };
            reader.EndBlock();
            dto.RequestedBy = reader.ReadVarString();
            dto.Tenor = reader.ReadVarString();
            return dto;
        }
    }
}
