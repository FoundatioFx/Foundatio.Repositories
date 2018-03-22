using System.Text;
using Elasticsearch.Net;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    internal static class IBodyWithApiCallDetailsExtensions {
        public static T DeserializeRaw<T>(this IBodyWithApiCallDetails call) where T : class, new() {
            if (call?.ApiCall?.ResponseBodyInBytes == null)
                return default;

            var rawResponse = Encoding.UTF8.GetString(call.ApiCall.ResponseBodyInBytes);
            return JsonConvert.DeserializeObject<T>(rawResponse);
        }
    }
}