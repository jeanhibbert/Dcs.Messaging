using Dcs.Messaging.Serialization;

namespace Dcs.Messaging.Common.Sbe
{
    /// <summary>
    /// Identifiers for the sample DTO schema described in <c>Sbe/dcs-messaging-common.xml</c>.
    /// </summary>
    public static class CommonSbeSchema
    {
        public const ushort SchemaId = 2;
        public const ushort Version = 0;

        public const ushort TestAlertTemplateId = 1;
        public const ushort PricingRequestTemplateId = 2;
        public const ushort ForwardPricingTemplateId = 3;

        /// <summary>
        /// Creates a serializer that can encode every DTO in <c>Dcs.Messaging.Common.Dto</c>.
        /// </summary>
        public static SbeBinarySerializer CreateSerializer()
        {
            return new SbeBinarySerializer(
                new TestAlertDtoSbeCodec(),
                new PricingRequestDtoSbeCodec(),
                new ForwardPricingDtoSbeCodec());
        }
    }
}
