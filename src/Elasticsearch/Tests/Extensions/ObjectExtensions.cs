using System;
using System.IO;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Elasticsearch.Tests.Extensions {
    public static class ObjectExtensions {
        public static string ToJson<T>(this T data, Formatting formatting = Formatting.None, JsonSerializerSettings settings = null) {
            JsonSerializer serializer = settings == null ? JsonSerializer.CreateDefault() : JsonSerializer.CreateDefault(settings);
            serializer.Formatting = formatting;

            using (var sw = new StringWriter()) {
                serializer.Serialize(sw, data, typeof(T));
                return sw.ToString();
            }
        }
    }
}
