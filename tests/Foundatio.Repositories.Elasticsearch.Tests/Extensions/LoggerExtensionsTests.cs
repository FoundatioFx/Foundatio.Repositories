using System;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests.Extensions;

public sealed class LoggerExtensionsTests
{
    [Fact]
    public void LogErrorRequest_WithOperationExceptionAndNoTransportException_LogsOperationException()
    {
        var logger = new RecordingLogger();
        var operationException = new InvalidOperationException("delete failed");

        logger.LogErrorRequest(operationException, new DeleteIndexResponse(), "Failed to delete index");

        Assert.Same(operationException, logger.Exception);
        Assert.Contains("Failed to delete index", logger.Message);
    }

    [Fact]
    public void LogErrorRequest_WithOperationExceptionAndNoResponse_LogsOperationException()
    {
        var logger = new RecordingLogger();
        var operationException = new InvalidOperationException("request failed before a response");

        logger.LogErrorRequest(operationException, null, "Failed before receiving a response");

        Assert.Same(operationException, logger.Exception);
        Assert.Contains("Failed before receiving a response", logger.Message);
    }

    [Fact]
    public void LogErrorRequest_WithNoRawResponse_StillLogsMessage()
    {
        var logger = new RecordingLogger();

        logger.LogErrorRequest((ElasticsearchStringResponse?)null, "Failed before receiving a raw response");

        Assert.Null(logger.Exception);
        Assert.Contains("Failed before receiving a raw response", logger.Message);
    }

    private sealed class RecordingLogger : ILogger
    {
        public Exception? Exception { get; private set; }
        public string Message { get; private set; } = String.Empty;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Exception = exception;
            Message = formatter(state, exception);
        }
    }
}
