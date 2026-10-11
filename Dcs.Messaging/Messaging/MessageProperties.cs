using System;
using System.Collections.Generic;

namespace Dcs.Messaging
{
    public sealed class MessageProperties : IMessageProperties
    {
        private readonly Dictionary<string, object> _values;

        public MessageProperties()
            : this(new Dictionary<string, object>(StringComparer.Ordinal))
        {
        }

        public MessageProperties(IDictionary<string, object> values)
        {
            _values = new Dictionary<string, object>(values, StringComparer.Ordinal);
        }

        public IEnumerable<string> Keys => _values.Keys;

        public IEnumerable<object> Values => _values.Values;

        /// <summary>
        /// Live view of the underlying values for allocation-free enumeration by transport encoders. Do not mutate.
        /// </summary>
        internal Dictionary<string, object> Entries => _values;

        public bool GetBoolean(string key)
        {
            return _values.TryGetValue(key, out var value) && Convert.ToBoolean(value);
        }

        public double GetDouble(string key)
        {
            return _values.TryGetValue(key, out var value) ? Convert.ToDouble(value) : 0d;
        }

        public int GetInt32(string key)
        {
            return _values.TryGetValue(key, out var value) ? Convert.ToInt32(value) : 0;
        }

        public string GetString(string key)
        {
            return _values.TryGetValue(key, out var value) ? Convert.ToString(value) : null;
        }

        public void Set(string key, bool value)
        {
            _values[key] = value;
        }

        public void Set(string key, double value)
        {
            _values[key] = value;
        }

        public void Set(string key, int value)
        {
            _values[key] = value;
        }

        public void Set(string key, string value)
        {
            _values[key] = value;
        }

        public IDictionary<string, object> ToDictionary()
        {
            return new Dictionary<string, object>(_values, StringComparer.Ordinal);
        }
    }
}