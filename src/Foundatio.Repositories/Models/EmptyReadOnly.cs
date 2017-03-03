using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Foundatio.Repositories.Models {
    public static class EmptyReadOnly<T> {
        public static readonly IReadOnlyCollection<T> Collection = new ReadOnlyCollection<T>(new List<T>());
    }

    public static class EmptyReadOnly<TKey, TValue> {
        public static readonly IReadOnlyDictionary<TKey, TValue> Dictionary = new ReadOnlyDictionary<TKey, TValue>(new Dictionary<TKey, TValue>());
    }
}