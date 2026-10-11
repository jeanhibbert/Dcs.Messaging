using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Sbe;

namespace Dcs.Messaging.Common.Sbe
{
    public sealed class ForwardPricingDtoSbeCodec : SbeCodec<ForwardPricingDto>
    {
        private const ushort EncodedBlockLength = sizeof(double) + sizeof(double) + CurrencyPairSbeComposite.EncodedLength + sizeof(byte);

        public ForwardPricingDtoSbeCodec()
            : base(CommonSbeSchema.SchemaId, CommonSbeSchema.ForwardPricingTemplateId, EncodedBlockLength, CommonSbeSchema.Version)
        {
        }

        protected override int GetVariableDataLength(ForwardPricingDto value)
        {
            return 0;
        }

        protected override void Encode(ForwardPricingDto value, ref SbeWriter writer)
        {
            writer.WriteDouble(value.BidRate);
            writer.WriteDouble(value.AskRate);
            CurrencyPairSbeComposite.Write(ref writer, value.CurrencyPair);
            writer.WriteBoolean(value.IsContra);
        }

        protected override ForwardPricingDto Decode(ref SbeReader reader, ushort actingVersion)
        {
            var dto = new ForwardPricingDto
            {
                BidRate = reader.ReadDouble(),
                AskRate = reader.ReadDouble(),
                CurrencyPair = CurrencyPairSbeComposite.Read(ref reader),
                IsContra = reader.ReadBoolean()
            };
            reader.EndBlock();
            return dto;
        }
    }
}
