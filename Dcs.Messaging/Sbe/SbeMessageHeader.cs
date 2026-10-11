namespace Dcs.Messaging.Sbe
{
    /// <summary>
    /// Standard SBE <c>messageHeader</c> composite: four little-endian <c>uint16</c> fields that precede every message.
    /// </summary>
    public readonly struct SbeMessageHeader
    {
        public const int EncodedLength = 8;

        public SbeMessageHeader(ushort blockLength, ushort templateId, ushort schemaId, ushort version)
        {
            BlockLength = blockLength;
            TemplateId = templateId;
            SchemaId = schemaId;
            Version = version;
        }

        /// <summary>
        /// Length of the root fixed-size block that follows the header. Decoders use the sender's value so
        /// that blocks extended by a newer schema version can be skipped.
        /// </summary>
        public ushort BlockLength { get; }

        public ushort TemplateId { get; }

        public ushort SchemaId { get; }

        public ushort Version { get; }
    }

    /// <summary>
    /// Standard SBE <c>groupSizeEncoding</c> composite that precedes a repeating group.
    /// </summary>
    public readonly struct SbeGroupHeader
    {
        public const int EncodedLength = 4;

        public SbeGroupHeader(ushort blockLength, ushort numInGroup)
        {
            BlockLength = blockLength;
            NumInGroup = numInGroup;
        }

        public ushort BlockLength { get; }

        public ushort NumInGroup { get; }
    }
}
