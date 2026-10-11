using Dcs.Messaging;
using Dcs.Messaging.Transport;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Dcs.Messaging.Sbe
{
    /// <summary>
    /// Wire encoding for <see cref="SbeMessagingSession"/>. Each frame is a FIX Simple Open Framing Header (SOFH)
    /// followed by one SBE <c>TransportEnvelope</c> message, as described by <c>Sbe/Schemas/dcs-messaging-transport.xml</c>:
    /// <code>
    /// SOFH:    uint32 BE frameLength (includes SOFH) | uint16 BE encodingType (0x5BE0)
    /// header:  uint16 blockLength | uint16 templateId | uint16 schemaId | uint16 version
    /// block:   int32 timeToLiveMs (optional) | uint8 isPersistent (optional)
    /// group:   properties { double numericValue | uint8 kind | data key | data stringValue }
    /// data:    endpointAddress | senderSessionId | targetSessionId | tag | payload
    /// </code>
    /// The two properties every <see cref="MessageFactory"/> message carries are fixed root-block fields; any other
    /// property goes in the repeating group with its value in native binary form. The frame is written straight from
    /// <see cref="IMessage"/> without an intermediate envelope object.
    /// </summary>
    internal static class SbeTransportCodec
    {
        public const ushort SchemaId = 1;
        public const ushort SchemaVersion = 0;
        public const ushort TransportEnvelopeTemplateId = 1;
        public const ushort TransportEnvelopeBlockLength = sizeof(int) + sizeof(byte);
        public const ushort PropertyBlockLength = sizeof(double) + sizeof(byte);

        /// <summary>
        /// SOFH encoding type for SBE version 1.0, little-endian.
        /// </summary>
        public const ushort SofhEncodingType = 0x5BE0;
        public const int SofhLength = sizeof(uint) + sizeof(ushort);

        private const int MinimumFrameLength = SofhLength + SbeMessageHeader.EncodedLength;
        private const int Int32NullValue = int.MinValue;
        private const byte BooleanNullValue = byte.MaxValue;

        public static byte[] EncodeFrame(string endpointAddress, IMessage message, string targetSessionId)
        {
            var properties = GetProperties(message.Properties);
            var payload = message.Payload ?? Array.Empty<byte>();
            var frameLength = MinimumFrameLength
                + TransportEnvelopeBlockLength
                + SbeGroupHeader.EncodedLength
                + SbeWriter.GetVarStringLength(endpointAddress)
                + SbeWriter.GetVarStringLength(message.SenderSessionId)
                + SbeWriter.GetVarStringLength(targetSessionId)
                + SbeWriter.GetVarStringLength(message.Tag)
                + SbeWriter.GetVarDataLength(payload.Length);

            var timeToLiveMs = Int32NullValue;
            var isPersistent = BooleanNullValue;
            var groupCount = 0;

            foreach (var property in properties)
            {
                if (TryGetTimeToLive(property, out var ttl))
                {
                    timeToLiveMs = ttl;
                }
                else if (TryGetIsPersistent(property, out var persistent))
                {
                    isPersistent = persistent ? (byte)1 : (byte)0;
                }
                else
                {
                    groupCount++;
                    frameLength += PropertyBlockLength
                        + SbeWriter.GetVarStringLength(property.Key)
                        + SbeWriter.GetVarStringLength(GetStringValue(property.Value));
                }
            }

            if (groupCount > ushort.MaxValue)
            {
                throw new InvalidOperationException($"SBE transport supports at most {ushort.MaxValue} message properties.");
            }

            var frame = new byte[frameLength];
            BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)frameLength);
            BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(sizeof(uint)), SofhEncodingType);

            var writer = new SbeWriter(frame.AsSpan(SofhLength));
            writer.WriteMessageHeader(new SbeMessageHeader(TransportEnvelopeBlockLength, TransportEnvelopeTemplateId, SchemaId, SchemaVersion));
            writer.WriteInt32(timeToLiveMs);
            writer.WriteUInt8(isPersistent);
            writer.WriteGroupHeader(new SbeGroupHeader(PropertyBlockLength, (ushort)groupCount));

            foreach (var property in properties)
            {
                if (!TryGetTimeToLive(property, out _) && !TryGetIsPersistent(property, out _))
                {
                    WriteProperty(ref writer, property.Key, property.Value);
                }
            }

            writer.WriteVarString(endpointAddress);
            writer.WriteVarString(message.SenderSessionId);
            writer.WriteVarString(targetSessionId);
            writer.WriteVarString(message.Tag);
            writer.WriteVarData(payload);

            return frame;
        }

        /// <summary>
        /// Decodes one SBE <c>TransportEnvelope</c> message (the bytes after the SOFH).
        /// </summary>
        public static IncomingTransportMessage DecodeMessage(ReadOnlySpan<byte> buffer)
        {
            var reader = new SbeReader(buffer);
            var header = reader.ReadMessageHeader();
            if (header.SchemaId != SchemaId || header.TemplateId != TransportEnvelopeTemplateId)
            {
                throw new InvalidDataException(
                    $"Unexpected SBE message: schema {header.SchemaId} template {header.TemplateId}; expected schema {SchemaId} template {TransportEnvelopeTemplateId}.");
            }

            var properties = new MessageProperties();

            reader.BeginBlock(header.BlockLength);
            var timeToLiveMs = reader.ReadInt32();
            var isPersistent = reader.ReadUInt8();
            reader.EndBlock();

            if (timeToLiveMs != Int32NullValue)
            {
                properties.Set(TransportPropertyKeys.TimeToLiveMs, timeToLiveMs);
            }

            if (isPersistent != BooleanNullValue)
            {
                properties.Set(TransportPropertyKeys.IsPersistent, isPersistent != 0);
            }

            var group = reader.ReadGroupHeader();
            for (var i = 0; i < group.NumInGroup; i++)
            {
                ReadProperty(ref reader, group.BlockLength, properties);
            }

            var endpointAddress = reader.ReadVarString();
            var senderSessionId = reader.ReadVarString();
            var targetSessionId = reader.ReadVarString();
            var tag = reader.ReadVarString();
            var payload = reader.ReadVarData();

            return new IncomingTransportMessage(endpointAddress, targetSessionId, new Message(senderSessionId, tag, properties, payload));
        }

        public static async Task<IncomingTransportMessage> ReadFrameAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            var sofh = await TransportTcpFraming.ReadExactAsync(stream, SofhLength, cancellationToken).ConfigureAwait(false);
            if (sofh == null)
            {
                return null;
            }

            var frameLength = BinaryPrimitives.ReadUInt32BigEndian(sofh);
            var encodingType = BinaryPrimitives.ReadUInt16BigEndian(sofh.AsSpan(sizeof(uint)));
            if (encodingType != SofhEncodingType || frameLength < MinimumFrameLength || frameLength > int.MaxValue)
            {
                return null;
            }

            var body = await TransportTcpFraming.ReadExactAsync(stream, (int)frameLength - SofhLength, cancellationToken).ConfigureAwait(false);
            if (body == null)
            {
                return null;
            }

            return DecodeMessage(body);
        }

        private static Dictionary<string, object> GetProperties(IMessageProperties properties)
        {
            if (properties is MessageProperties messageProperties)
            {
                return messageProperties.Entries;
            }

            return properties == null
                ? new Dictionary<string, object>()
                : properties.Keys.Zip(properties.Values).ToDictionary(pair => pair.First, pair => pair.Second, StringComparer.Ordinal);
        }

        private static bool TryGetTimeToLive(KeyValuePair<string, object> property, out int value)
        {
            if (property.Value is int intValue
                && intValue != Int32NullValue
                && string.Equals(property.Key, TransportPropertyKeys.TimeToLiveMs, StringComparison.Ordinal))
            {
                value = intValue;
                return true;
            }

            value = 0;
            return false;
        }

        private static bool TryGetIsPersistent(KeyValuePair<string, object> property, out bool value)
        {
            if (property.Value is bool boolValue
                && string.Equals(property.Key, TransportPropertyKeys.IsPersistent, StringComparison.Ordinal))
            {
                value = boolValue;
                return true;
            }

            value = false;
            return false;
        }

        private static string GetStringValue(object value)
        {
            switch (value)
            {
                case bool _:
                case int _:
                case double _:
                    return null;
                case string stringValue:
                    return stringValue;
                default:
                    return Convert.ToString(value, CultureInfo.InvariantCulture);
            }
        }

        private static void WriteProperty(ref SbeWriter writer, string key, object value)
        {
            var numericValue = 0d;
            SbePropertyKind kind;

            switch (value)
            {
                case bool b:
                    kind = SbePropertyKind.Bool;
                    numericValue = b ? 1d : 0d;
                    break;
                case int i:
                    kind = SbePropertyKind.Int;
                    numericValue = i;
                    break;
                case double d:
                    kind = SbePropertyKind.Double;
                    numericValue = d;
                    break;
                default:
                    kind = SbePropertyKind.String;
                    break;
            }

            writer.WriteDouble(numericValue);
            writer.WriteUInt8((byte)kind);
            writer.WriteVarString(key);
            writer.WriteVarString(GetStringValue(value));
        }

        private static void ReadProperty(ref SbeReader reader, int blockLength, MessageProperties properties)
        {
            reader.BeginBlock(blockLength);
            var numericValue = reader.ReadDouble();
            var kind = (SbePropertyKind)reader.ReadUInt8();
            reader.EndBlock();

            var key = reader.ReadVarString();
            var stringValue = reader.ReadVarString();

            switch (kind)
            {
                case SbePropertyKind.Bool:
                    properties.Set(key, numericValue != 0d);
                    break;
                case SbePropertyKind.Int:
                    properties.Set(key, (int)numericValue);
                    break;
                case SbePropertyKind.Double:
                    properties.Set(key, numericValue);
                    break;
                default:
                    properties.Set(key, stringValue);
                    break;
            }
        }

        /// <summary>
        /// How a property entry's value is carried. Every int32 and bool is exactly representable in
        /// <c>numericValue</c>; strings use the <c>stringValue</c> data field.
        /// </summary>
        private enum SbePropertyKind : byte
        {
            Bool = 0,
            Int = 1,
            Double = 2,
            String = 3
        }
    }
}
