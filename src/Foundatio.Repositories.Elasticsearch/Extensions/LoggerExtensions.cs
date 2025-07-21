using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Elasticsearch.Net;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using Nest;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class LoggerExtensions
{
    [Obsolete("Use LogRequest instead")]
    public static void LogTraceRequest(this ILogger logger, IElasticsearchResponse elasticResponse, LogLevel logLevel = LogLevel.Trace)
    {
        LogRequest(logger, elasticResponse, logLevel);
    }

    public static void LogRequest(this ILogger logger, IElasticsearchResponse elasticResponse, LogLevel logLevel = LogLevel.Trace)
    {
        if (elasticResponse == null || !logger.IsEnabled(logLevel))
            return;

        var apiCall = elasticResponse?.ApiCall;
        if (apiCall?.RequestBodyInBytes != null)
        {
            string body = Encoding.UTF8.GetString(apiCall?.RequestBodyInBytes);
            body = JsonUtility.Normalize(body);

            logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}\r\n{HttpBody}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri.PathAndQuery, body);
        }
        else
        {
            logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri.PathAndQuery);
        }
    }

    public static void LogErrorRequest(this ILogger logger, IElasticsearchResponse elasticResponse, string message, params object[] args)
    {
        LogErrorRequest(logger, null, elasticResponse, message, args);
    }

    public static void LogErrorRequest(this ILogger logger, Exception ex, IElasticsearchResponse elasticResponse, string message, params object[] args)
    {
        if (elasticResponse == null || !logger.IsEnabled(LogLevel.Error))
            return;

        var response = elasticResponse as IResponse;

        AggregateException aggEx = null;
        if (ex != null && response?.OriginalException != null)
            aggEx = new AggregateException(ex, response.OriginalException);

        logger.LogError(aggEx ?? response?.OriginalException, elasticResponse.GetErrorMessage(message), args);
    }
}

internal class JsonUtility
{
    public static string Normalize(string jsonStr)
    {
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(jsonStr));
        if (!JsonDocument.TryParseValue(ref reader, out var doc))
            return jsonStr;

        return Normalize(doc.RootElement);
    }

    public static string Normalize(JsonElement element)
    {
        var ms = new MemoryStream();
        var opts = new JsonWriterOptions
        {
            Indented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
        using (var writer = new Utf8JsonWriter(ms, opts))
        {
            Write(element, writer);
        }

        var bytes = ms.ToArray();
        var str = Encoding.UTF8.GetString(bytes);
        return str;
    }

    private static void Write(JsonElement element, Utf8JsonWriter writer)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();

                foreach (var x in element.EnumerateObject().OrderBy(prop => prop.Name))
                {
                    writer.WritePropertyName(x.Name);
                    Write(x.Value, writer);
                }

                writer.WriteEndObject();
                break;

            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var x in element.EnumerateArray())
                {
                    Write(x, writer);
                }
                writer.WriteEndArray();
                break;

            case JsonValueKind.Number:
                writer.WriteNumberValue(element.GetDouble());
                break;

            case JsonValueKind.String:
                writer.WriteStringValue(element.GetString());
                break;

            case JsonValueKind.Null:
                writer.WriteNullValue();
                break;

            case JsonValueKind.True:
                writer.WriteBooleanValue(true);
                break;

            case JsonValueKind.False:
                writer.WriteBooleanValue(false);
                break;

            default:
                throw new NotImplementedException($"Kind: {element.ValueKind}");
        }
    }
}
