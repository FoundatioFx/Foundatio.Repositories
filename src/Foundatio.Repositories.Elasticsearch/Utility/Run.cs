﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Utility {
    internal static class Run {
        public static async Task<T> WithRetriesAsync<T>(Func<Task<T>> action, int maxAttempts = 5, TimeSpan? retryInterval = null, CancellationToken cancellationToken = default(CancellationToken), ILogger logger = null) {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            int attempts = 1;
            var startTime = SystemClock.UtcNow;
            do {
                if (attempts > 1)
                    logger?.LogInformation($"Retrying {attempts.ToOrdinal()} attempt after {SystemClock.UtcNow.Subtract(startTime).TotalMilliseconds}ms...");

                try {
                    return await action().AnyContext();
                } catch (Exception ex) {
                    if (attempts >= maxAttempts)
                        throw;

                    logger?.LogError(ex, $"Retry error: {ex.Message}");
                    await SystemClock.SleepAsync(retryInterval ?? TimeSpan.FromMilliseconds(attempts * 100), cancellationToken).AnyContext();
                }

                attempts++;
            } while (attempts <= maxAttempts && !cancellationToken.IsCancellationRequested);

            throw new TaskCanceledException("Should not get here.");
        }
    }
}
