using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class LoggerExtensions
{
    public static void LogRequest(this ILogger logger, ElasticsearchResponse elasticResponse, LogLevel logLevel = LogLevel.Trace)
    {
        if (elasticResponse == null || !logger.IsEnabled(logLevel))
            return;

        var apiCall = elasticResponse.ApiCallDetails;
        if (apiCall?.RequestBodyInBytes != null)
        {
            string body = Encoding.UTF8.GetString(apiCall.RequestBodyInBytes);
            body = JsonUtility.Normalize(body);

            logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}\r\n{HttpBody}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri?.PathAndQuery, body);
        }
        else if (apiCall != null)
        {
            logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}", apiCall?.HttpStatusCode, apiCall?.HttpMethod, apiCall?.Uri?.PathAndQuery);
        }
    }

    internal static void LogRequest(this ILogger logger, ElasticsearchStringResponse elasticResponse, LogLevel logLevel = LogLevel.Trace)
    {
        if (elasticResponse is null || !logger.IsEnabled(logLevel))
            return;

        var apiCall = elasticResponse.ApiCallDetails;
        if (apiCall?.RequestBodyInBytes is not null)
        {
            string body = JsonUtility.Normalize(Encoding.UTF8.GetString(apiCall.RequestBodyInBytes));
            logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}\r\n{HttpBody}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri?.PathAndQuery, body);
        }
        else if (apiCall is not null)
        {
            logger.Log(logLevel, "[{HttpStatusCode}] {HttpMethod} {HttpPathAndQuery}", apiCall.HttpStatusCode, apiCall.HttpMethod, apiCall.Uri?.PathAndQuery);
        }
    }

    internal static void LogErrorRequest(this ILogger logger, ElasticsearchStringResponse? elasticResponse, string message, params object?[] args)
    {
        if (!logger.IsEnabled(LogLevel.Error))
            return;

        if (elasticResponse is null)
        {
            logger.LogError(message, args);
            return;
        }

        elasticResponse.TryGetOriginalException(out var originalException);
        var allArgs = new object?[args.Length + 1];
        args.CopyTo(allArgs, 0);
        allArgs[^1] = String.IsNullOrWhiteSpace(elasticResponse.Body)
            ? elasticResponse.DebugInformation
            : elasticResponse.Body;
        logger.LogError(originalException, message + ": {ElasticError}", allArgs);
    }

    public static void LogErrorRequest(this ILogger logger, ElasticsearchResponse? elasticResponse, string message, params object?[] args)
    {
        LogErrorRequest(logger, null, elasticResponse, message, args);
    }

    public static void LogErrorRequest(this ILogger logger, Exception? ex, ElasticsearchResponse? elasticResponse, string message, params object?[] args)
    {
        if (!logger.IsEnabled(LogLevel.Error))
            return;

        if (elasticResponse is null)
        {
            logger.LogError(ex, message, args);
            return;
        }

        var originalException = elasticResponse.ApiCallDetails?.OriginalException;

        Exception? logException = ex switch
        {
            null => originalException,
            _ when originalException is null => ex,
            _ => new AggregateException(ex, originalException)
        };

        var allArgs = new object[args.Length + 1];
        args.CopyTo(allArgs, 0);
        allArgs[^1] = elasticResponse.GetErrorMessage();
        logger.LogError(logException, message + ": {ElasticError}", allArgs);
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
        using var ms = new MemoryStream();
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
                throw new NotSupportedException($"Unsupported JsonValueKind: {element.ValueKind}");
        }
    }
}
