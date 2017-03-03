using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    internal static class CollectionEqualityExtensions {
        public static bool CollectionEquals<T>(this IEnumerable<T> source, IEnumerable<T> other) {
            var sourceEnumerator = source.GetEnumerator();
            var otherEnumerator = other.GetEnumerator();

            while (sourceEnumerator.MoveNext()) {
                if (!otherEnumerator.MoveNext()) {
                    // counts differ
                    return false;
                }

                if (sourceEnumerator.Current.Equals(otherEnumerator.Current)) {
                    // values aren't equal
                    return false;
                }
            }

            if (otherEnumerator.MoveNext()) {
                // counts differ
                return false;
            }
            return true;
        }
        
        public static int GetCollectionHashCode<T>(this IEnumerable<T> source) {
            var assemblyQualifiedName = typeof(T).AssemblyQualifiedName;
            int hashCode = assemblyQualifiedName == null ? 0 : assemblyQualifiedName.GetHashCode();

            foreach (var item in source) {
                if (item == null)
                    continue;

                unchecked {
                    hashCode = (hashCode * 397) ^ item.GetHashCode();
                }
            }
            return hashCode;
        }
    }
}