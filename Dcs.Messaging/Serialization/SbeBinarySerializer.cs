using Dcs.Messaging.Sbe;
using System;
using System.Collections.Generic;

namespace Dcs.Messaging.Serialization
{
    /// <summary>
    /// <see cref="IBinarySerializer"/> backed by Simple Binary Encoding. Unlike protobuf-net, SBE is schema-first,
    /// so every payload type needs an explicitly registered <see cref="SbeCodec{T}"/>.
    /// </summary>
    public sealed class SbeBinarySerializer : IBinarySerializer
    {
        private readonly Dictionary<Type, ISbeCodec> _codecs;

        public SbeBinarySerializer(params ISbeCodec[] codecs)
            : this((IEnumerable<ISbeCodec>)codecs)
        {
        }

        public SbeBinarySerializer(IEnumerable<ISbeCodec> codecs)
        {
            if (codecs == null)
            {
                throw new ArgumentNullException(nameof(codecs));
            }

            _codecs = new Dictionary<Type, ISbeCodec>();
            foreach (var codec in codecs)
            {
                if (!_codecs.TryAdd(codec.MessageType, codec))
                {
                    throw new ArgumentException($"More than one SBE codec registered for {codec.MessageType.FullName}.", nameof(codecs));
                }
            }
        }

        public T Deserialize<T>(byte[] data)
        {
            return GetCodec<T>().Decode(data);
        }

        public byte[] Serialize<T>(T payload)
        {
            return GetCodec<T>().Encode(payload);
        }

        private SbeCodec<T> GetCodec<T>()
        {
            if (_codecs.TryGetValue(typeof(T), out var codec))
            {
                return (SbeCodec<T>)codec;
            }

            throw new NotSupportedException(
                $"No SBE codec is registered for {typeof(T).FullName}. Implement SbeCodec<{typeof(T).Name}> and pass it to the SbeBinarySerializer constructor.");
        }
    }
}
