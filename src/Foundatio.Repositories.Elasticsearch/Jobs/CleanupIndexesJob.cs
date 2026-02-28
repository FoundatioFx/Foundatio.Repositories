using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Exceptionless.DateTimeExtensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

public class CleanupIndexesJob : IJob
{
    protected readonly ElasticsearchClient _client;
    protected readonly ILogger _logger;
    protected readonly ILockProvider _lockProvider;
    protected readonly TimeProvider _timeProvider;
    private static readonly CultureInfo _enUS = new("en-US");
    private readonly ICollection<IndexMaxAge> _indexes = new List<IndexMaxAge>();

    public CleanupIndexesJob(ElasticsearchClient client, ILockProvider lockProvider, TimeProvider timeProvider, ILoggerFactory loggerFactory)
    {
        _client = client;
        _lockProvider = lockProvider;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
    }

    protected void AddIndex(TimeSpan maxAge, ExtractDateFunc getAge)
    {
        _indexes.Add(new IndexMaxAge(maxAge, getAge));
    }

    protected void AddIndex(string prefix, TimeSpan maxAge)
    {
        _indexes.Add(new IndexMaxAge(maxAge, idx =>
        {
            if (DateTime.TryParseExact(idx, "'" + prefix + "-'yyyy.MM.dd", _enUS, DateTimeStyles.None, out var result))
                return result;

            return null;
        }));
    }

    public virtual async Task<JobResult> RunAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting index cleanup...");

        var sw = Stopwatch.StartNew();
        var result = await _client.Indices.GetAsync(Indices.All,
            d => d.RequestConfiguration(r => r.RequestTimeout(TimeSpan.FromMinutes(5))), cancellationToken).AnyContext();
        sw.Stop();

        if (result.IsValidResponse)
        {
            _logger.LogRequest(result);
            _logger.LogInformation("Retrieved list of {IndexCount} indexes in {Duration:g}", result.Indices?.Count, sw.Elapsed.ToWords(true));
        }
        else
        {
            _logger.LogErrorRequest(result, "Failed to retrieve list of indexes");
        }

        var indexes = new List<IndexDate>();
        if (result.IsValidResponse && result.Indices != null)
            indexes = result.Indices?.Keys.Select(r => GetIndexDate(r.ToString())).Where(r => r != null).ToList();

        if (indexes == null || indexes.Count == 0)
            return JobResult.Success;

        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var indexesToDelete = indexes.Where(r => r.Date < now.Subtract(r.MaxAge)).ToList();

        if (indexesToDelete.Count == 0)
        {
            _logger.LogInformation("No indexes selected for deletion.");
            return JobResult.Success;
        }

        // log that we are seeing indexes that should have been deleted already
        var oldIndexes = indexes.Where(s => s.Date < now.Subtract(s.MaxAge).AddDays(-1)).ToList();
        if (oldIndexes.Count > 0)
            _logger.LogError("Found old indexes that should have been deleted: {Indexes}", String.Join(", ", oldIndexes));

        _logger.LogInformation("Selected {IndexCount} indexes for deletion", indexesToDelete.Count);

        bool shouldContinue = true;
        foreach (var oldIndex in indexesToDelete)
        {
            if (!shouldContinue)
            {
                _logger.LogInformation("Stopped deleted snapshots.");
                break;
            }

            _logger.LogInformation("Acquiring lock to delete index {OldIndex}", oldIndex.Index);
            try
            {
                await _lockProvider.TryUsingAsync("es-delete-index", async t =>
                {
                    _logger.LogInformation("Got lock to delete index {OldIndex}", oldIndex.Index);
                    sw.Restart();
                    var response = await _client.Indices.DeleteAsync(Indices.Index(oldIndex.Index), t).AnyContext();
                    sw.Stop();
                    _logger.LogRequest(response);

                    if (response.IsValidResponse)
                        await OnIndexDeleted(oldIndex.Index, sw.Elapsed).AnyContext();
                    else
                        shouldContinue = await OnIndexDeleteFailure(oldIndex.Index, sw.Elapsed, response, null).AnyContext();
                }, TimeSpan.FromMinutes(30), cancellationToken).AnyContext();
            }
            catch (Exception ex)
            {
                sw.Stop();
                shouldContinue = await OnIndexDeleteFailure(oldIndex.Index, sw.Elapsed, null, ex).AnyContext();
            }
        }

        await OnCompleted(indexesToDelete.Select(i => i.Index).ToList(), sw.Elapsed).AnyContext();

        return JobResult.Success;
    }

    public virtual Task OnIndexDeleted(string indexName, TimeSpan duration)
    {
        _logger.LogInformation("Completed delete index {IndexName} in {Duration:g}", indexName, duration.ToWords(true));
        return Task.CompletedTask;
    }

    public virtual Task<bool> OnIndexDeleteFailure(string indexName, TimeSpan duration, DeleteIndexResponse response, Exception ex)
    {
        _logger.LogErrorRequest(ex, response, "Failed to delete index {IndexName} after {Duration:g}", indexName, duration);
        return Task.FromResult(true);
    }

    public virtual Task OnCompleted(IReadOnlyCollection<string> deletedIndexes, TimeSpan duration)
    {
        _logger.LogInformation("Finished cleaning up {IndexCount} in {Duration:g}.", deletedIndexes.Count, duration);
        return Task.CompletedTask;
    }

    private IndexDate GetIndexDate(string name)
    {
        if (_indexes.Count == 0)
        {
            AddIndex("logstash", TimeSpan.FromDays(7));
            AddIndex(".marvel", TimeSpan.FromDays(7));
        }

        foreach (var index in _indexes)
        {
            var date = index.GetDate(name);
            if (date == null)
                continue;

            return new IndexDate { Index = name, Date = date.Value, MaxAge = index.MaxAge };
        }

        return null;
    }

    private class IndexMaxAge
    {
        public IndexMaxAge(TimeSpan maxAge, ExtractDateFunc getAge)
        {
            MaxAge = maxAge;
            GetDate = getAge;
        }

        public TimeSpan MaxAge { get; }
        public ExtractDateFunc GetDate { get; }
    }

    private class IndexDate
    {
        public string Index { get; set; }
        public DateTime Date { get; set; }
        public TimeSpan MaxAge { get; set; }
    }
}
