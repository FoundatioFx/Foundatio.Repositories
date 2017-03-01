using System;
using System.Collections.Generic;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Options {
    public interface IOptions {
        void SetOption(string name, object value);
        bool HasOption(string name);
        bool RemoveOption(string name);
        T GetOption<T>(string name, T defaultValue = default(T));
        IEnumerable<KeyValuePair<string, object>> GetAllOptions();
    }

    public abstract class OptionsBase : IOptions {
        protected readonly IDictionary<string, object> _options = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        void IOptions.SetOption(string name, object value) {
            _options[name] = value;
        }

        bool IOptions.HasOption(string name) {
            return _options.ContainsKey(name);
        }

        bool IOptions.RemoveOption(string name) {
            return _options.Remove(name);
        }

        T IOptions.GetOption<T>(string name, T defaultValue) {
            if (!_options.ContainsKey(name))
                return defaultValue;

            object data = _options[name];
            if (!(data is T)) {
                try {
                    return TypeHelper.ToType<T>(data);
                } catch {
                    throw new ArgumentException($"Option \"{name}\" is not compatible with the requested type \"{typeof(T).FullName}\".");
                }
            }

            return (T)data;
        }

        IEnumerable<KeyValuePair<string, object>> IOptions.GetAllOptions() {
            return _options;
        }
    }

    public static class OptionsExtensions {
        public static T BuildOption<T>(this T options, string name, object value) where T : IOptions {
            options.SetOption(name, value);
            return options;
        }

        public static T SafeGetOption<T>(this IOptions options, string name, T defaultValue = default(T)) {
            if (options == null)
                return defaultValue;

            return options.GetOption(name, defaultValue);
        }

        public static bool SafeHasOption(this IOptions options, string name) {
            if (options == null)
                return false;

            return options.HasOption(name);
        }

        public static ICollection<T> SafeGetCollection<T>(this IOptions options, string name) {
            if (options == null)
                return new List<T>();

            return options.GetOption(name, new List<T>());
        }

        public static TOptions AddCollectionOptionValue<TOptions, TValue>(this TOptions options, string name, TValue value) where TOptions : IOptions {
            var setOption = options.SafeGetOption(name, new List<TValue>());
            setOption.Add(value);
            options.SetOption(name, setOption);

            return options;
        }

        public static TOptions AddCollectionOptionValue<TOptions, TValue>(this TOptions options, string name, IEnumerable<TValue> values) where TOptions : IOptions {
            var setOption = options.SafeGetOption(name, new List<TValue>());
            setOption.AddRange(values);
            options.SetOption(name, setOption);

            return options;
        }

        public static ISet<T> SafeGetSet<T>(this IOptions options, string name) {
            if (options == null)
                return new HashSet<T>();

            return options.GetOption(name, new HashSet<T>());
        }

        public static T AddSetOptionValue<T>(this T options, string name, string value) where T : IOptions {
            var setOption = options.SafeGetOption(name, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            setOption.Add(value);
            options.SetOption(name, setOption);

            return options;
        }

        public static T AddSetOptionValue<T>(this T options, string name, IEnumerable<string> values) where T : IOptions {
            var setOption = options.SafeGetOption(name, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            setOption.AddRange(values);
            options.SetOption(name, setOption);

            return options;
        }

        public static T Clone<T>(this T source) where T: IOptions, new() {
            var clone = new T();

            foreach (var kvp in source.GetAllOptions())
                clone.SetOption(kvp.Key, kvp.Value);

            return source;
        }

        public static T Apply<T>(this T target, IOptions source, bool overrideExisting = true) where T : IOptions {
            if (source == null)
                return target;

            foreach (var kvp in source.GetAllOptions()) {
                // TODO: Collection option values should get added to instead of replaced
                if (overrideExisting || !target.HasOption(kvp.Key))
                    target.SetOption(kvp.Key, kvp.Value);
            }

            return target;
        }
    }
}
