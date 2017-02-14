using System;
using System.Collections.Generic;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Options {
    public interface IOptions {
        void SetOption(string name, object value);
        bool HasOption(string name);
        bool RemoveOption(string name);
        T GetOption<T>(string name, T defaultValue = default(T));
    }

    public abstract class OptionsBase : IOptions {
        protected readonly IDictionary<string, object> _options = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        public void SetOption(string name, object value) {
            _options.Add(name, value);
        }

        public bool HasOption(string name) {
            return _options.ContainsKey(name);
        }

        public bool RemoveOption(string name) {
            return _options.Remove(name);
        }

        public T GetOption<T>(string name, T defaultValue = default(T)) {
            if (!_options.ContainsKey(name))
                return defaultValue;

            object data = _options[name];
            if (!(data is T)) {
                try {
                    return data.ToType<T>();
                } catch {
                    throw new ArgumentException($"Option \"{name}\" is not compatible with the requested type \"{typeof(T).FullName}\".");
                }
            }

            return (T)data;
        }
    }
}
