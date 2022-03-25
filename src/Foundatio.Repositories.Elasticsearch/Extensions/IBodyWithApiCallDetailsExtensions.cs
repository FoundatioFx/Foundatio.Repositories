using System;
using System.Text;
using System.Text.Json;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class IBodyWithApiCallDetailsExtensions {
    public static T DeserializeRaw<T>(this IResponse call) where T : class, new() {
        if (call?.ApiCall?.ResponseBodyInBytes == null)
            return default;

        string rawResponse = Encoding.UTF8.GetString(call.ApiCall.ResponseBodyInBytes);
        return JsonSerializer.Deserialize<T>(rawResponse);
    }
}
