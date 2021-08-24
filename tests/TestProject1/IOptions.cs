using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Options {
    public interface IOptionsDictionary : IDictionary<string, object> {
        T Get<T>(string name, T defaultValue = default);
    }

    public class OptionsDictionary : Dictionary<string, object>, IOptionsDictionary {
        public T Get<T>(string name, T defaultValue) {
            if (!ContainsKey(name))
                return defaultValue;

            object data = this[name];
            if (data is not T data1)
                throw new ArgumentException($"Option \"{name}\" is not compatible with the requested type \"{typeof(T).FullName}\".");

            return data1;
        }
    }
}
