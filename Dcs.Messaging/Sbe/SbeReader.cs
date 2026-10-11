using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Dcs.Messaging.Sbe
{
    /// <summary>
    /// Sequential little-endian reader that mirrors <see cref="SbeWriter"/>. Every read is bounds-checked and
    /// throws <see cref="InvalidDataException"/> on truncated or malformed input.
    /// </summary>
    /// <remarks>
    /// Fixed-size blocks are bracketed with <see cref="BeginBlock"/> / <see cref="EndBlock"/>. <see cref="EndBlock"/>
    /// skips to the end of the block length declared on the wire, so messages produced by a newer schema version
    /// that appends block fields can still be decoded.
    /// </remarks>
    public ref struct SbeReader
    {
        private readonly ReadOnlySpan<byte> _buffer;
        private int _position;
        private int _blockEnd;

        public SbeReader(ReadOnlySpan<byte> buffer)
        {
            _buffer = buffer;
            _position = 0;
            _blockEnd = -1;
        }

        public int Position => _position;

        public int Remaining => _buffer.Length - _position;

        public void BeginBlock(int blockLength)
        {
            if (blockLength < 0 || blockLength > Remaining)
            {
                throw new InvalidDataException($"SBE block length {blockLength} exceeds the {Remaining} bytes remaining.");
            }

            _blockEnd = _position + blockLength;
        }

        public void EndBlock()
        {
            if (_blockEnd < 0)
            {
                throw new InvalidOperationException("EndBlock called without a matching BeginBlock.");
            }

            if (_position > _blockEnd)
            {
                throw new InvalidDataException("SBE block is shorter than the fields expected by this schema version.");
            }

            _position = _blockEnd;
            _blockEnd = -1;
        }

        public SbeMessageHeader ReadMessageHeader()
        {
            return new SbeMessageHeader(ReadUInt16(), ReadUInt16(), ReadUInt16(), ReadUInt16());
        }

        public SbeGroupHeader ReadGroupHeader()
        {
            return new SbeGroupHeader(ReadUInt16(), ReadUInt16());
        }

        public byte ReadUInt8()
        {
            return Take(sizeof(byte))[0];
        }

        public bool ReadBoolean()
        {
            return ReadUInt8() != 0;
        }

        public ushort ReadUInt16()
        {
            return BinaryPrimitives.ReadUInt16LittleEndian(Take(sizeof(ushort)));
        }

        public int ReadInt32()
        {
            return BinaryPrimitives.ReadInt32LittleEndian(Take(sizeof(int)));
        }

        public uint ReadUInt32()
        {
            return BinaryPrimitives.ReadUInt32LittleEndian(Take(sizeof(uint)));
        }

        public long ReadInt64()
        {
            return BinaryPrimitives.ReadInt64LittleEndian(Take(sizeof(long)));
        }

        public double ReadDouble()
        {
            return BinaryPrimitives.ReadDoubleLittleEndian(Take(sizeof(double)));
        }

        /// <summary>
        /// Reads a fixed-length ASCII <c>char</c> array, trimming null padding. All nulls decode as <c>null</c>.
        /// </summary>
        public string ReadFixedString(int length)
        {
            var bytes = Take(length);
            var terminator = bytes.IndexOf((byte)0);
            if (terminator >= 0)
            {
                bytes = bytes.Slice(0, terminator);
            }

            return bytes.IsEmpty ? null : Encoding.ASCII.GetString(bytes);
        }

        public string ReadVarString()
        {
            var bytes = Take(ReadUInt16());
            return bytes.IsEmpty ? string.Empty : Encoding.UTF8.GetString(bytes);
        }

        public byte[] ReadVarData()
        {
            return Take(ReadVarLength()).ToArray();
        }

        private int ReadVarLength()
        {
            var length = ReadUInt32();
            if (length > int.MaxValue)
            {
                throw new InvalidDataException($"SBE variable-length field of {length} bytes is too large.");
            }

            return (int)length;
        }

        private ReadOnlySpan<byte> Take(int count)
        {
            if (count > Remaining)
            {
                throw new InvalidDataException($"SBE buffer underflow: needed {count} bytes at offset {_position} but only {Remaining} remain.");
            }

            var slice = _buffer.Slice(_position, count);
            _position += count;
            return slice;
        }
    }
}
