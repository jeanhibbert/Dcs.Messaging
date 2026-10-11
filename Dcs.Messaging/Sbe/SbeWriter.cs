using System;
using System.Buffers.Binary;
using System.Text;

namespace Dcs.Messaging.Sbe
{
    /// <summary>
    /// Sequential little-endian writer over a pre-sized buffer, following SBE field layout rules:
    /// fixed-size block fields first, then repeating groups, then variable-length data.
    /// </summary>
    public ref struct SbeWriter
    {
        private readonly Span<byte> _buffer;
        private int _position;

        public SbeWriter(Span<byte> buffer)
        {
            _buffer = buffer;
            _position = 0;
        }

        public int Position => _position;

        /// <summary>
        /// Maximum UTF-8 length of a <c>varStringEncoding</c> field (65535 is the uint16 null value).
        /// </summary>
        public const int MaxVarStringLength = ushort.MaxValue - 1;

        /// <summary>
        /// Encoded size of a <c>varStringEncoding</c> field (uint16 length + UTF-8 bytes).
        /// </summary>
        public static int GetVarStringLength(string value)
        {
            var byteCount = string.IsNullOrEmpty(value) ? 0 : Encoding.UTF8.GetByteCount(value);
            if (byteCount > MaxVarStringLength)
            {
                throw new ArgumentException($"String of {byteCount} UTF-8 bytes exceeds the SBE varStringEncoding limit of {MaxVarStringLength}.", nameof(value));
            }

            return sizeof(ushort) + byteCount;
        }

        /// <summary>
        /// Encoded size of a <c>varDataEncoding</c> field (uint32 length + raw bytes).
        /// </summary>
        public static int GetVarDataLength(int dataLength)
        {
            return sizeof(uint) + dataLength;
        }

        public void WriteMessageHeader(SbeMessageHeader header)
        {
            WriteUInt16(header.BlockLength);
            WriteUInt16(header.TemplateId);
            WriteUInt16(header.SchemaId);
            WriteUInt16(header.Version);
        }

        public void WriteGroupHeader(SbeGroupHeader header)
        {
            WriteUInt16(header.BlockLength);
            WriteUInt16(header.NumInGroup);
        }

        public void WriteUInt8(byte value)
        {
            _buffer[_position] = value;
            _position += sizeof(byte);
        }

        /// <summary>
        /// Writes an SBE <c>BooleanType</c> enum (uint8: F = 0, T = 1).
        /// </summary>
        public void WriteBoolean(bool value)
        {
            WriteUInt8(value ? (byte)1 : (byte)0);
        }

        public void WriteUInt16(ushort value)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_position), value);
            _position += sizeof(ushort);
        }

        public void WriteInt32(int value)
        {
            BinaryPrimitives.WriteInt32LittleEndian(_buffer.Slice(_position), value);
            _position += sizeof(int);
        }

        public void WriteUInt32(uint value)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(_buffer.Slice(_position), value);
            _position += sizeof(uint);
        }

        public void WriteInt64(long value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(_buffer.Slice(_position), value);
            _position += sizeof(long);
        }

        public void WriteDouble(double value)
        {
            BinaryPrimitives.WriteDoubleLittleEndian(_buffer.Slice(_position), value);
            _position += sizeof(double);
        }

        /// <summary>
        /// Writes a fixed-length ASCII <c>char</c> array, padding with the SBE null character (0).
        /// A <c>null</c> value is encoded as all nulls, which SBE defines as the null value of a char array.
        /// </summary>
        public void WriteFixedString(string value, int length)
        {
            var target = _buffer.Slice(_position, length);
            target.Clear();

            if (!string.IsNullOrEmpty(value))
            {
                if (value.Length > length)
                {
                    throw new ArgumentException($"Value '{value}' exceeds the fixed SBE field length of {length}.", nameof(value));
                }

                Encoding.ASCII.GetBytes(value, target);
            }

            _position += length;
        }

        /// <summary>
        /// Writes a <c>varStringEncoding</c> field. SBE has no null for variable-length data, so <c>null</c> is written as empty.
        /// </summary>
        public void WriteVarString(string value)
        {
            var lengthPosition = _position;
            _position += sizeof(ushort);

            var byteCount = string.IsNullOrEmpty(value) ? 0 : Encoding.UTF8.GetBytes(value, _buffer.Slice(_position));
            if (byteCount > MaxVarStringLength)
            {
                throw new ArgumentException($"String of {byteCount} UTF-8 bytes exceeds the SBE varStringEncoding limit of {MaxVarStringLength}.", nameof(value));
            }

            BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(lengthPosition), (ushort)byteCount);
            _position += byteCount;
        }

        /// <summary>
        /// Writes a <c>varDataEncoding</c> field (uint32 length + raw bytes).
        /// </summary>
        public void WriteVarData(ReadOnlySpan<byte> value)
        {
            WriteUInt32((uint)value.Length);
            value.CopyTo(_buffer.Slice(_position));
            _position += value.Length;
        }
    }
}
