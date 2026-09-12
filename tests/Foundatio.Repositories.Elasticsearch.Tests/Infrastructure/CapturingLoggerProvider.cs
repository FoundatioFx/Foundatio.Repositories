using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;

/// <summary>
/// Collects formatted log messages so tests can assert on what was logged.
/// </summary>
public sealed class CapturingLoggerProvider : ILoggerProvider
{
    private readonly ICollection<string> _messages;

    public CapturingLoggerProvider(ICollection<string> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);

        _messages = messages;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new CapturingLogger(_messages);
    }

    public void Dispose()
    {
    }

    private sealed class CapturingLogger : ILogger
    {
        private readonly ICollection<string> _messages;

        public CapturingLogger(ICollection<string> messages)
        {
            _messages = messages;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);

            lock (_messages)
            {
                _messages.Add(formatter(state, exception));
            }
        }
    }
}
