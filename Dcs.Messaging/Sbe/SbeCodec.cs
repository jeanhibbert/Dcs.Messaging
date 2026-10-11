using System;
using System.IO;

namespace Dcs.Messaging.Sbe
{
    /// <summary>
    /// Non-generic view of an <see cref="SbeCodec{T}"/>, used to register codecs with
    /// <see cref="Serialization.SbeBinarySerializer"/>.
    /// </summary>
    public interface ISbeCodec
    {
        Type MessageType { get; }

        ushort SchemaId { get; }

        ushort TemplateId { get; }
    }

    /// <summary>
    /// Encodes and decodes one SBE message template. Each encoded payload starts with a standard
    /// <see cref="SbeMessageHeader"/>, followed by the fixed-size root block and then variable-length data.
    /// </summary>
    /// <remarks>
    /// Implementations write exactly <see cref="BlockLength"/> bytes of block fields followed by their
    /// variable-length fields, and report the size of the latter via <see cref="GetVariableDataLength"/>
    /// so the output buffer can be allocated once at its exact size.
    /// </remarks>
    public abstract class SbeCodec<T> : ISbeCodec
    {
        protected SbeCodec(ushort schemaId, ushort templateId, ushort blockLength, ushort version = 0)
        {
            SchemaId = schemaId;
            TemplateId = templateId;
            BlockLength = blockLength;
            Version = version;
        }

        public Type MessageType => typeof(T);

        public ushort SchemaId { get; }

        public ushort TemplateId { get; }

        public ushort BlockLength { get; }

        public ushort Version { get; }

        public byte[] Encode(T value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var buffer = new byte[SbeMessageHeader.EncodedLength + BlockLength + GetVariableDataLength(value)];
            var writer = new SbeWriter(buffer);
            writer.WriteMessageHeader(new SbeMessageHeader(BlockLength, TemplateId, SchemaId, Version));
            Encode(value, ref writer);

            if (writer.Position != buffer.Length)
            {
                throw new InvalidOperationException(
                    $"{GetType().Name} wrote {writer.Position} bytes but declared {buffer.Length}; check BlockLength and GetVariableDataLength.");
            }

            return buffer;
        }

        public T Decode(ReadOnlySpan<byte> buffer)
        {
            var reader = new SbeReader(buffer);
            var header = reader.ReadMessageHeader();
            if (header.SchemaId != SchemaId || header.TemplateId != TemplateId)
            {
                throw new InvalidDataException(
                    $"Expected SBE schema {SchemaId} template {TemplateId} for {typeof(T).Name} but received schema {header.SchemaId} template {header.TemplateId}.");
            }

            reader.BeginBlock(header.BlockLength);
            return Decode(ref reader, header.Version);
        }

        protected abstract int GetVariableDataLength(T value);

        protected abstract void Encode(T value, ref SbeWriter writer);

        /// <summary>
        /// Reads the root block fields, calls <see cref="SbeReader.EndBlock"/>, then reads any variable-length fields.
        /// </summary>
        protected abstract T Decode(ref SbeReader reader, ushort actingVersion);
    }
}
