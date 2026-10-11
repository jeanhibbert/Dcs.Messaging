using Dcs.Messaging.Common.Dto;
using Dcs.Messaging.Sbe;

namespace Dcs.Messaging.Common.Sbe
{
    public sealed class TestAlertDtoSbeCodec : SbeCodec<TestAlertDto>
    {
        private const ushort EncodedBlockLength = sizeof(int);

        public TestAlertDtoSbeCodec()
            : base(CommonSbeSchema.SchemaId, CommonSbeSchema.TestAlertTemplateId, EncodedBlockLength, CommonSbeSchema.Version)
        {
        }

        protected override int GetVariableDataLength(TestAlertDto value)
        {
            return SbeWriter.GetVarStringLength(value.ServerName) + SbeWriter.GetVarStringLength(value.Message);
        }

        protected override void Encode(TestAlertDto value, ref SbeWriter writer)
        {
            writer.WriteInt32(value.MessageId);
            writer.WriteVarString(value.ServerName);
            writer.WriteVarString(value.Message);
        }

        protected override TestAlertDto Decode(ref SbeReader reader, ushort actingVersion)
        {
            var dto = new TestAlertDto { MessageId = reader.ReadInt32() };
            reader.EndBlock();
            dto.ServerName = reader.ReadVarString();
            dto.Message = reader.ReadVarString();
            return dto;
        }
    }
}
