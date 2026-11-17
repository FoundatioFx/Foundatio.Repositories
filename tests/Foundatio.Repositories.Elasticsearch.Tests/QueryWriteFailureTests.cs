using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Repositories.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class QueryWriteFailureTests
{
    [Theory]
    [InlineData(false, "BeforeQuery")]
    [InlineData(false, "SystemFilter")]
    [InlineData(false, "ConfigureQuery")]
    [InlineData(true, "BeforeQuery")]
    [InlineData(true, "SystemFilter")]
    [InlineData(true, "ConfigureQuery")]
    public async Task QueryWrite_WithRepositoryFilter_PreservesSelection(bool patch, string filterSource)
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":false,"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = patch
            ? new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask))
            : new TestConfiguration(("_delete_by_query", 200, QueryResponse(deleted: 1, timedOut: false, itemFailure: false)));
        using var repository = new TestRepository(configuration) { ConfigureSelectionFilter = filterSource is "ConfigureQuery" };
        var query = new RepositoryQuery<TestDocument>();
        int beforeQueries = 0;
        if (filterSource is "SystemFilter")
        {
            var filter = new RepositoryQuery<TestDocument>();
            filter.FieldEquals(document => document.Count, 42);
            query.SystemFilter(filter);
        }
        repository.BeforeQuery.AddHandler((_, args) =>
        {
            beforeQueries++;
            if (filterSource is "BeforeQuery")
                args.Query.FieldEquals("count", 42);
            return Task.CompletedTask;
        });

        // Act
        long changed = patch
            ? await repository.PatchAllAsync(query, new ScriptPatch("ctx._source.count++"), new CommandOptions().UseUpdateByQuery().Retry(0))
            : await repository.RemoveAllAsync(query, new CommandOptions().Retry(0));

        // Assert
        Assert.Equal(1, changed);
        Assert.Equal(1, beforeQueries);
        using var request = JsonDocument.Parse(configuration.Invoker.RequestBodies[0]);
        Assert.Contains("\"count\"", request.RootElement.GetProperty("query").GetRawText());
        Assert.Contains("42", request.RootElement.GetProperty("query").GetRawText());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task QueryWrite_WithReplacementQuery_UsesEffectiveQueryForCompletionEffects(bool patch, bool partialFailure)
    {
        // Arrange
        string completedTask = $$$"""
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":2,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":{{{partialFailure.ToString().ToLowerInvariant()}}},"total":2,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = patch
            ? new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask))
            : new TestConfiguration(("_delete_by_query", 200, QueryResponse(deleted: 1, timedOut: partialFailure, itemFailure: false)));
        using var repository = new TestRepository(configuration, cacheEnabled: !patch)
        {
            ReplacementQuery = new RepositoryQuery<TestDocument>().Id("effective")
        };
        var query = new RepositoryQuery<TestDocument>().Id("original");
        if (!patch)
        {
            await repository.CacheDocumentAsync("original");
            await repository.CacheDocumentAsync("effective");
            Assert.NotNull(await repository.GetCachedDocumentAsync("original"));
            Assert.NotNull(await repository.GetCachedDocumentAsync("effective"));
        }

        // Act
        async Task<long> WriteAsync() => patch
            ? await repository.PatchAllAsync(query, new ScriptPatch("ctx._source.count++"), new CommandOptions().UseUpdateByQuery().Retry(0))
            : await repository.RemoveAllAsync(query, new CommandOptions().Cache(false).Retry(0));
        if (partialFailure)
            await Assert.ThrowsAsync<DocumentException>(WriteAsync);
        else
            Assert.Equal(1, await WriteAsync());

        // Assert
        using var request = JsonDocument.Parse(configuration.Invoker.RequestBodies[0]);
        string effectiveQuery = request.RootElement.GetProperty("query").GetRawText();
        Assert.Contains("\"effective\"", effectiveQuery);
        Assert.DoesNotContain("\"original\"", effectiveQuery);
        Assert.Equal(new[] { "effective" }, repository.QueryNotificationIds);
        Assert.Equal(1, repository.QueryNotifications);
        if (!patch)
        {
            Assert.Null(await repository.GetCachedDocumentAsync("effective"));
            Assert.NotNull(await repository.GetCachedDocumentAsync("original"));
            Assert.Equal(new[] { "effective" }, repository.QueryInvalidationIds);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("parent-route")]
    public async Task PatchAsync_WithPipeline_TargetsTheRequestedRoutingShard(string? routing)
    {
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(0, false, false, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true);

        Assert.True(await repository.PatchAsync(new Id("one", routing), new ScriptPatch("ctx._source.count++"), options => options.Retry(0)));

        Assert.Contains($"routing={routing ?? "one"}", Assert.Single(configuration.Invoker.RequestPaths));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WithUnstructuredServiceUnavailableWhileReading_RetriesReadBeforeWriting(bool throwOnErrors)
    {
        // Arrange
        const string document = """{"docs":[{"_index":"query-tests","_id":"one","found":true,"_source":{"id":"one","count":0}}]}""";
        const string saved = """{"took":1,"errors":false,"items":[{"index":{"_index":"query-tests","_id":"one","status":200,"result":"updated","_seq_no":1,"_primary_term":1}}]}""";
        using var configuration = new TestConfiguration(("_mget", 503, "upstream connection terminated"), ("_mget", 200, document), ("_bulk", 200, saved)) { ThrowOnErrors = throwOnErrors };
        using var repository = new TestRepository(configuration);

        // Act
        Assert.True(await repository.PatchAsync("one", new ActionPatch<TestDocument>(document => document.Count++), o => o.Retry(1)));

        // Assert
        Assert.Equal(3, configuration.Invoker.Requests);
    }

    [Theory]
    [InlineData("ActionPatch", false)]
    [InlineData("ActionPatch", true)]
    [InlineData("JsonPatch", false)]
    [InlineData("JsonPatch", true)]
    [InlineData("PartialPatch", false)]
    [InlineData("PartialPatch", true)]
    public async Task PatchAsync_WithIfVersionAndUnavailableReadVersion_ThrowsVersioningNotSupportedBeforeWriting(string patchType, bool sentinelMetadata)
    {
        // Arrange
        string metadata = sentinelMetadata ? "\"_seq_no\":-2,\"_primary_term\":0," : String.Empty;
        string document = $$$"""{"docs":[{"_index":"query-tests","_id":"one",{{{metadata}}}"found":true,"_source":{"id":"one","count":0}}]}""";
        using var configuration = new TestConfiguration(("_mget", 200, document));
        using var repository = new TestRepository(configuration, usePipeline: patchType is "PartialPatch");
        IPatchOperation operation = patchType switch
        {
            "ActionPatch" => new ActionPatch<TestDocument>(document => document.Count++),
            "JsonPatch" => new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "count", Value = 1 })),
            _ => new PartialPatch(new { Count = 1 })
        };

        // Act
        var exception = await Assert.ThrowsAsync<DocumentVersioningNotSupportedException>(() =>
            repository.PatchAsync("one", operation, o => o.IfVersion("1:0").Retry(1)));

        // Assert
        Assert.Contains("query-tests", exception.Message);
        Assert.Equal(1, configuration.Invoker.Requests);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RemoveAsync_WithMissingUnversionedDocuments_DoesNotRaiseRemovalEvents(bool bulk)
    {
        // Arrange
        const string singleResponse = """{"_index":"query-tests","_id":"one","result":"not_found","_version":2,"_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}""";
        const string bulkResponse = """{"took":1,"errors":false,"items":[{"delete":{"_index":"query-tests","_id":"one","status":404,"result":"not_found"}},{"delete":{"_index":"query-tests","_id":"two","status":404,"result":"not_found"}}]}""";
        using var configuration = new TestConfiguration(bulk ? ("_bulk", 200, bulkResponse) : ("_doc/one", 404, singleResponse));
        using var repository = new TestRepository(configuration);
        int removed = 0;
        repository.DocumentsRemoved.AddHandler((_, args) =>
        {
            removed += args.Documents.Count;
            return Task.CompletedTask;
        });
        TestDocument[] documents = bulk ? [new TestDocument { Id = "one" }, new TestDocument { Id = "two" }] : [new TestDocument { Id = "one" }];

        // Act
        await repository.RemoveAsync(documents, o => o.Retry(0));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(0, removed);
    }

    [Theory]
    [InlineData(200, true, false)]
    [InlineData(200, false, true)]
    [InlineData(500, false, true)]
    public async Task RemoveAllAsync_WithPartialQueryFailure_NotifiesCompletedDeletesAndThrows(int status, bool timedOut, bool itemFailure)
    {
        // Arrange
        string response = QueryResponse(deleted: 1, timedOut, itemFailure);
        using var configuration = new TestConfiguration(("_delete_by_query", status, response));
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.RemoveAllAsync(q => q, o => o.Retry(0)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task PatchAsync_WithPipelineAndQueryFailure_ThrowsInsteadOfReportingNoop(bool timedOut, bool itemFailure)
    {
        // Arrange
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(deleted: 0, timedOut, itemFailure)));
        using var repository = new TestRepository(configuration, usePipeline: true);

        // Act & Assert
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(0)));
        Assert.Equal(1, configuration.Invoker.Requests);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task PatchAsync_WithPipelineAndPartiallyAppliedQuery_NotifiesBeforeThrowing(bool timedOut, bool itemFailure)
    {
        // Arrange
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(deleted: 0, timedOut, itemFailure, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true);
        int changed = 0;
        var updatedIds = new List<string>();
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            changed++;
            return Task.CompletedTask;
        });

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"),
            o => o.Retry(1).UpdateCallback(updatedIds.AddRange)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, updatedIds);
    }

    [Theory]
    [InlineData("""{}""")]
    [InlineData("""{"updated":0,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":0,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":0,"deleted":0}""")]
    [InlineData("""{"total":-1,"updated":0,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":-1,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":0,"deleted":-1,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":0,"deleted":0,"version_conflicts":-1}""")]
    [InlineData("""{"updated":1,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"timed_out":true,"total":1,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"total":0,"updated":1,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":2,"deleted":0,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":0,"deleted":2,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":1,"deleted":1,"version_conflicts":0}""")]
    [InlineData("""{"total":1,"updated":1,"deleted":0,"version_conflicts":1}""")]
    [InlineData("""{"total":9223372036854775807,"updated":9223372036854775807,"deleted":1,"version_conflicts":0}""")]
    public async Task PatchAsync_WithPipelineAndInvalidCompletionCounters_InvalidatesAndNotifiesUnknownOutcome(string response)
    {
        using var configuration = new TestConfiguration(("_update_by_query", 200, response));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        int changed = 0;
        var updatedIds = new List<string>();
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one",
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1).UpdateCallback(updatedIds.AddRange)));

        Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
        Assert.Empty(updatedIds);
    }

    [Theory]
    [InlineData("<html>upstream response</html>")]
    [InlineData("")]
    public async Task PatchAsync_WithPipelineAndUnreadableSuccessfulResponse_InvalidatesAndNotifiesUnknownOutcome(string response)
    {
        using var configuration = new TestConfiguration(("_update_by_query", 200, response));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one",
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1)));

        Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Theory]
    [InlineData("""{"total":2,"deleted":0,"version_conflicts":1}""")]
    [InlineData("""{"total":2,"updated":-1,"deleted":0,"version_conflicts":1}""")]
    [InlineData("""{"total":0,"updated":0,"deleted":0,"version_conflicts":1}""")]
    [InlineData("""{"total":1,"updated":0,"deleted":0,"version_conflicts":2}""")]
    public async Task PatchAsync_WithPipelineAndUnknownAppliedCount_DoesNotRetryReportedConflicts(string response)
    {
        using var configuration = new TestConfiguration(
            ("_update_by_query", 200, response),
            ("_refresh", 200, """{"_shards":{"total":1,"successful":1,"failed":0}}"""),
            ("_update_by_query", 200, QueryResponse(0, false, false, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        var failure = await Record.ExceptionAsync(async () =>
        {
            _ = await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"),
                options => options.Consistency(Consistency.Eventual).Retry(1));
        });

        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        var exception = Assert.IsType<DocumentException>(failure);
        Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Theory]
    [InlineData("0.5")]
    [InlineData("9223372036854775808")]
    public async Task PatchAsync_WithPipelineAndUnreadableCompletionCounter_NotifiesOnlyTheDispatchedId(string updated)
    {
        string response = $$"""
            {"took":1,"timed_out":false,"total":1,"updated":{{updated}},"deleted":0,"version_conflicts":0,"noops":0,"failures":[]}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, response));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync(new Ids("one", "two"),
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1)));

        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        Assert.IsType<UnexpectedTransportException>(exception.InnerException);
        Exception? cause = exception.InnerException;
        while (cause is not null && cause is not JsonException)
            cause = cause.InnerException;
        Assert.IsAssignableFrom<JsonException>(cause);
        Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndRequestJsonFailure_ConservativelyNotifiesOnlyTheAttemptedId()
    {
        using var configuration = new SerializationFailureConfiguration();
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        var updatedIds = new List<string>();
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);
        var parameter = new FailingScriptParameter();
        var patch = new ScriptPatch("ctx._source.count++") { Params = new Dictionary<string, object> { ["value"] = parameter } };

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync(new Ids("one", "two"), patch,
            options => options.Consistency(Consistency.Eventual).Retry(1).UpdateCallback(updatedIds.AddRange)));

        Assert.Equal(0, configuration.HttpRequests);
        Assert.Equal(1, parameter.SerializationAttempts);
        Assert.IsType<UnexpectedTransportException>(exception.InnerException);
        Assert.Contains("injected request JSON failure", exception.ToString(), StringComparison.Ordinal);
        Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
        Assert.Empty(updatedIds);
    }

    [Theory]
    [InlineData(408)]
    [InlineData(500)]
    [InlineData(503)]
    [InlineData(504)]
    public async Task PatchAsync_WithPipelineAndUnknownHttpFailure_InvalidatesWithoutRetrying(int status)
    {
        string response = status is 503 or 504
            ? "<html>injected gateway failure</html>"
            : $$"""{"error":{"type":"exception","reason":"injected request failure"},"status":{{status}}}""";
        using var configuration = new TestConfiguration(("_update_by_query", status, response));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one",
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1)));

        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndNativeTimeout_InvalidatesWithoutRetrying()
    {
        var failure = new TaskCanceledException("injected request timeout", new TimeoutException("injected timeout cause"));
        using var configuration = new NativeTransportFailureConfiguration(failure);
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one",
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1)));

        Assert.Equal(1, configuration.HttpRequests);
        Assert.Same(failure.InnerException, exception.GetBaseException());
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndNativeTransportException_NotifiesOnlyTheAttemptedId()
    {
        var failure = new IOException("injected response stream failure");
        using var configuration = new NativeTransportFailureConfiguration(failure);
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync(new Ids("one", "two"),
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1)));

        Assert.Equal(1, configuration.HttpRequests);
        Assert.IsType<UnexpectedTransportException>(exception.InnerException);
        Assert.Same(failure, exception.GetBaseException());
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndCancellationAfterDispatch_NotifiesOnlyTheAttemptedId()
    {
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(0, false, false, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        int completedRequests = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);
        configuration.RequestCompleted = details =>
        {
            if (details.Uri?.AbsolutePath.Contains("_update_by_query", StringComparison.Ordinal) is not true)
                return;

            Assert.Equal(200, details.HttpStatusCode);
            completedRequests++;
            repository.Dispose();
            // The native completion callback propagates cancellation directly, after a response was received.
            throw new OperationCanceledException("injected cancellation after the pipeline write completed");
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.PatchAsync(new Ids("one", "two"),
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1)));

        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        Assert.Equal(1, completedRequests);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndCancellationBetweenIds_NotifiesOnlyTheCompletedId()
    {
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(0, false, false, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        int completedRequests = 0;
        var updatedIds = new List<string>();
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);
        configuration.RequestCompleted = details =>
        {
            if (details.Uri?.AbsolutePath.Contains("_update_by_query", StringComparison.Ordinal) is not true)
                return;

            Assert.Equal(200, details.HttpStatusCode);
            completedRequests++;
            repository.Dispose();
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.PatchAsync(new Ids("one", "two"),
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1).UpdateCallback(updatedIds.AddRange)));

        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        Assert.Equal(1, completedRequests);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(1, changed);
        Assert.Equal(new[] { "one" }, updatedIds);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndCancellationBeforeDispatch_PreservesCacheWithoutNotifying()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);
        repository.Dispose();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.PatchAsync(new Ids("one", "two"),
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(1)));

        Assert.Empty(configuration.Invoker.RequestPaths);
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
        Assert.Equal(0, changed);
        Assert.Empty(repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task PatchAsync_WithPipelineAndNativeConnectionFailure_PreservesCacheWithoutNotifying(int retries)
    {
        var failure = new HttpRequestException(HttpRequestError.ConnectionError, "injected connection failure before the write was sent");
        using var configuration = new NativeTransportFailureConfiguration(failure);
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAsync("one",
            new ScriptPatch("ctx._source.count++"), options => options.Consistency(Consistency.Eventual).Retry(retries)));

        Assert.Equal(retries + 1, configuration.HttpRequests);
        Assert.Same(failure, exception.GetBaseException());
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.Equal(0, changed);
        Assert.Empty(repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    public async Task PatchAsync_WithPipelineAndKnownUnmodifiedOutcome_PreservesCacheWithoutNotifying(bool noop, int retries)
    {
        int count = noop ? 1 : 0;
        string response = $$"""
            {"took":1,"timed_out":false,"total":{{count}},"updated":0,"deleted":0,"version_conflicts":0,"noops":{{count}},"failures":[]}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, response));
        using var repository = new TestRepository(configuration, usePipeline: true, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        if (noop)
            Assert.False(await repository.PatchAsync("one", new ScriptPatch("ctx.op = 'noop'"),
                options => options.Consistency(Consistency.Eventual).Retry(retries)));
        else
            await Assert.ThrowsAsync<DocumentNotFoundException>(() => repository.PatchAsync("one", new ScriptPatch("ctx.op = 'noop'"),
                options => options.Consistency(Consistency.Eventual).Retry(retries)));

        Assert.Contains("_update_by_query", Assert.Single(configuration.Invoker.RequestPaths), StringComparison.Ordinal);
        Assert.NotNull(await repository.GetCachedDocumentAsync("one"));
        Assert.Equal(0, changed);
        Assert.Empty(repository.DocumentNotificationIds);
        Assert.Equal(0, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    public async Task SynchronousQueryWrite_WhenExternallyCancelled_NotifiesAppliedChangesAndThrowsWithoutRetrying(bool patch, int affected)
    {
        // Arrange
        string response = $$"""
            {"took":1,"timed_out":false,"total":2,"updated":{{(patch ? affected : 0)}},"deleted":{{(patch ? 0 : affected)}},"version_conflicts":0,"noops":0,"canceled":"by user request","failures":[]}
            """;
        using var configuration = new TestConfiguration((patch ? "_update_by_query" : "_delete_by_query", 200, response));
        using var repository = new TestRepository(configuration, usePipeline: patch);
        int changed = 0;
        if (patch)
        {
            repository.DocumentsChanged.AddHandler((_, _) =>
            {
                changed++;
                return Task.CompletedTask;
            });
        }

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(async () =>
        {
            if (patch)
                await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(1));
            else
                await repository.RemoveAllAsync(q => q, o => o.Retry(1));
        });

        // Assert
        Assert.Contains("cancel", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("by user request", exception.Message);
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(patch ? affected : 0, changed);
        Assert.Equal(patch ? 0 : affected, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndVersionConflict_RequestsConflictCountsAndRetries()
    {
        // Arrange
        const string conflict = """{"took":1,"timed_out":false,"total":1,"updated":0,"deleted":0,"version_conflicts":1,"failures":[]}""";
        using var configuration = new TestConfiguration(
            ("_update_by_query", 200, conflict),
            ("_refresh", 200, """{"_shards":{"total":1,"successful":1,"failed":0}}"""),
            ("_update_by_query", 200, QueryResponse(0, false, false, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Retry(1)));

        // Assert
        Assert.Equal(3, configuration.Invoker.Requests);
        Assert.All(configuration.Invoker.RequestBodies, body =>
        {
            if (String.IsNullOrEmpty(body))
                return;

            using var document = JsonDocument.Parse(body);
            Assert.Equal("proceed", document.RootElement.GetProperty("conflicts").GetString());
        });
    }

    [Fact]
    public async Task PatchAsync_WithPipelineAndNoConflict_DoesNotRefreshWithEventualConsistency()
    {
        // Arrange
        using var configuration = new TestConfiguration(("_update_by_query", 200, QueryResponse(0, false, false, updated: 1)));
        using var repository = new TestRepository(configuration, usePipeline: true);

        // Act
        Assert.True(await repository.PatchAsync("one", new ScriptPatch("ctx._source.count++"), o => o.Consistency(Consistency.Eventual).Retry(1)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
    }

    [Theory]
    [InlineData(400, 0)]
    [InlineData(502, 1)]
    [InlineData(503, 1)]
    [InlineData(504, 1)]
    public async Task PatchAllAsync_WithFailedTaskSubmission_NotifiesOnlyWhenOutcomeMayBeUnknown(int status, int expectedNotifications)
    {
        // Arrange
        string response = status is 503 ? "upstream connect error or disconnect/reset before headers" : $$"""{"error":{"type":"injected_failure","reason":"injected"},"status":{{status}}}""";
        using var configuration = new TestConfiguration(("_update_by_query", status, response));
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Equal(expectedNotifications, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAllAsync_WithUnexpectedTaskSubmissionContentType_NotifiesUnknownOutcomeAndThrowsWithoutResubmitting()
    {
        // Arrange
        using var configuration = new TestConfiguration(("_update_by_query", 200, "<html>upstream response unavailable</html>"));
        using var repository = new TestRepository(configuration);
        int changed = 0;
        repository.DocumentsChanged.AddHandler((_, _) =>
        {
            changed++;
            return Task.CompletedTask;
        });

        // Act
        await Assert.ThrowsAsync<DocumentException>(() =>
            repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1)));

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Single(configuration.Invoker.RequestPaths, path => path.Contains("_update_by_query", StringComparison.Ordinal));
        Assert.Equal(1, changed);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAllAsync_WhenDisposedDuringUnexpectedTaskSubmissionResponse_NotifiesUnknownOutcomeAndThrowsCancelled()
    {
        // Arrange
        using var configuration = new TestConfiguration(("_update_by_query", 200, "<html>upstream response unavailable</html>"));
        using var repository = new TestRepository(configuration);
        configuration.Invoker.BeforeResponseRead = path =>
        {
            if (path.Contains("_update_by_query", StringComparison.Ordinal))
                repository.Dispose();
        };
        int changed = 0;
        repository.DocumentsChanged.AddHandler((_, _) =>
        {
            changed++;
            return Task.CompletedTask;
        });

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            _ = await repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1));
        });

        // Assert
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Single(configuration.Invoker.RequestPaths, path => path.Contains("_update_by_query", StringComparison.Ordinal));
        Assert.IsAssignableFrom<OperationCanceledException>(exception);
        Assert.Equal(1, changed);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAllAsync_WithMalformedTaskResponse_NotifiesUnknownOutcomeAndPreservesCause(bool duringSubmission)
    {
        // Arrange
        using var configuration = duringSubmission
            ? new TestConfiguration(("_update_by_query", 200, """{"task":"node:1","unexpected": """))
            : new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, """{"completed":true,"task": """));
        using var repository = new TestRepository(configuration);
        int changed = 0;
        repository.DocumentsChanged.AddHandler((_, _) =>
        {
            changed++;
            return Task.CompletedTask;
        });

        // Act
        var failure = await Record.ExceptionAsync(async () =>
        {
            _ = await repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1));
        });

        // Assert
        Assert.Equal(duringSubmission ? 1 : 2, configuration.Invoker.Requests);
        Assert.Single(configuration.Invoker.RequestPaths, path => path.Contains("_update_by_query", StringComparison.Ordinal));
        var exception = Assert.IsType<DocumentException>(failure);
        Assert.IsType<UnexpectedTransportException>(exception.InnerException);
        Assert.IsAssignableFrom<JsonException>(exception.GetBaseException());
        Assert.Equal(1, changed);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData("{}", 0)]
    [InlineData("{}", 1)]
    [InlineData("null", 1)]
    [InlineData("\"unrecognized\"", 1)]
    [InlineData("{\"total\":0,\"updated\":0,\"created\":0,\"deleted\":0,\"version_conflicts\":0}", 1)]
    public async Task PatchAllAsync_WithCompletedResponseCounters_UsesTheAuthoritativeResult(string taskStatus, int updated)
    {
        // Arrange
        string completedTask = $$$"""
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{{{taskStatus}}}},"response":{"took":1,"timed_out":false,"total":1,"updated":{{{updated}}},"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        // Act
        long patched = await repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1));

        // Assert
        Assert.Equal(updated, patched);
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Single(configuration.Invoker.RequestPaths, path => path.Contains("_update_by_query", StringComparison.Ordinal));
        Assert.Equal(updated is 0 ? 0 : 1, changed);
        Assert.Equal(updated is 0 ? 0 : 1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData("{}", "{}")]
    [InlineData("null", "null")]
    [InlineData("\"unrecognized\"", "{}")]
    [InlineData("{}", "{\"updated\":1}")]
    [InlineData("{\"total\":1,\"created\":0,\"deleted\":0,\"version_conflicts\":0}", "null")]
    [InlineData("{}", "{\"total\":1,\"updated\":-1,\"created\":0,\"deleted\":0,\"version_conflicts\":0}")]
    [InlineData("{}", "{\"total\":1,\"updated\":0.5,\"created\":0,\"deleted\":0,\"version_conflicts\":0}")]
    public async Task PatchAllAsync_WithUnreadableCompletedCounters_NotifiesUnknownOutcomeAndThrows(string taskStatus, string taskResponse)
    {
        // Arrange
        string completedTask = $$$"""
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{{{taskStatus}}}},"response":{{{taskResponse}}}}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() =>
            repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1)));

        // Assert
        Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Single(configuration.Invoker.RequestPaths, path => path.Contains("_update_by_query", StringComparison.Ordinal));
        Assert.Equal(1, changed);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAllAsync_WithOverflowingCompletedCounters_NotifiesUnknownOutcomeAndPreservesCause()
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":9223372036854775807,"updated":1,"created":9223372036854775807,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":false,"total":9223372036854775807,"updated":1,"created":9223372036854775807,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);
        int changed = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changed++);

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() =>
            repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1)));

        // Assert
        Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.IsType<OverflowException>(exception.InnerException);
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Single(configuration.Invoker.RequestPaths, path => path.Contains("_update_by_query", StringComparison.Ordinal));
        Assert.Equal(1, changed);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAllAsync_WhenDisposedDuringTaskResponseRead_NotifiesUnknownOutcomeAndThrowsCancelled(bool duringSubmission)
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":false,"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);
        configuration.Invoker.BeforeResponseRead = path =>
        {
            if (path.Contains(duringSubmission ? "_update_by_query" : "_tasks", StringComparison.Ordinal))
                repository.Dispose();
        };
        int changed = 0;
        repository.DocumentsChanged.AddHandler((_, _) =>
        {
            changed++;
            return Task.CompletedTask;
        });

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            _ = await repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1));
        });

        // Assert
        Assert.Equal(duringSubmission ? 1 : 2, configuration.Invoker.Requests);
        Assert.Single(configuration.Invoker.RequestPaths, path => path.Contains("_update_by_query", StringComparison.Ordinal));
        Assert.IsAssignableFrom<OperationCanceledException>(exception);
        Assert.Equal(1, changed);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAsync_WhenDisposedDuringRetryResponseRead_NotifiesEarlierSuccessAndThrowsCancelled()
    {
        // Arrange
        const string partialSuccess = """
            {"took":1,"errors":true,"items":[{"update":{"_index":"query-tests","_id":"one","status":200,"result":"updated","_seq_no":1,"_primary_term":1}},{"update":{"_index":"query-tests","_id":"two","status":429,"error":{"type":"es_rejected_execution_exception","reason":"retry"}}}]}
            """;
        const string retryResponse = """
            {"_index":"query-tests","_id":"two","result":"updated","_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}
            """;
        using var configuration = new TestConfiguration(("_bulk", 200, partialSuccess), ("_update/two", 200, retryResponse));
        using var repository = new TestRepository(configuration, cacheEnabled: true);
        await repository.CacheDocumentAsync("one");
        await repository.CacheDocumentAsync("two");
        configuration.Invoker.BeforeResponseRead = path =>
        {
            if (path.Contains("_update/two", StringComparison.Ordinal))
                repository.Dispose();
        };
        var updatedIds = new List<string>();
        int changed = 0;
        repository.DocumentsChanged.AddHandler((_, _) =>
        {
            changed++;
            return Task.CompletedTask;
        });

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            _ = await repository.PatchAsync(new Ids("one", "two"), new ScriptPatch("ctx._source.count++"), o => o.Retry(1).UpdateCallback(updatedIds.AddRange));
        });

        // Assert
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.IsAssignableFrom<OperationCanceledException>(exception);
        Assert.Equal(new[] { "one" }, updatedIds);
        Assert.Equal(new[] { "one" }, repository.DocumentNotificationIds);
        Assert.Equal(1, changed);
        Assert.Null(await repository.GetCachedDocumentAsync("one"));
        Assert.NotNull(await repository.GetCachedDocumentAsync("two"));
    }

    [Fact]
    public async Task PatchAllAsync_WithTimedOutTask_NotifiesAndThrowsInsteadOfReportingSuccess()
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":2,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":true,"total":2,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(0)));

        // Assert
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task PatchAllAsync_WithExternallyCancelledTask_NotifiesAndThrowsWithoutResubmitting(int updated)
    {
        // Arrange
        string completedTask = $$$"""
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":2,"updated":{{{updated}}},"created":0,"deleted":0,"version_conflicts":0,"canceled":"by user request"}},"response":{"took":1,"timed_out":false,"total":2,"updated":{{{updated}}},"created":0,"deleted":0,"version_conflicts":0,"canceled":"by user request","failures":[]}}
            """;
        using var configuration = new TestConfiguration(("_update_by_query", 200, """{"task":"node:1"}"""), ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() =>
            repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1)));

        // Assert
        Assert.Contains("cancel", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("by user request", exception.Message);
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Fact]
    public async Task PatchAllAsync_WithThrowExceptionsEnabledAndTransientTaskRead_RetriesPollingWithoutResubmitting()
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":false,"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        using var configuration = new TestConfiguration(
            ("_update_by_query", 200, """{"task":"node:1"}"""),
            ("_tasks", 503, """{"error":{"type":"unavailable_shards_exception","reason":"temporary outage"},"status":503}"""),
            ("_tasks", 200, completedTask))
        { ThrowOnErrors = true };
        using var repository = new TestRepository(configuration);

        // Act
        long patched = await repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(0));

        // Assert
        Assert.Equal(1, patched);
        Assert.Equal(3, configuration.Invoker.Requests);
        Assert.Equal(1, repository.QueryNotifications);
        Assert.True(configuration.Client.ElasticsearchClientSettings.ThrowExceptions);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAllAsync_WithMissingTask_NotifiesUnknownOutcomeAndThrows(bool throwOnErrors)
    {
        // Arrange
        using var configuration = new TestConfiguration(
            ("_update_by_query", 200, """{"task":"node:1"}"""),
            ("_tasks", 404, """{"error":{"type":"resource_not_found_exception","reason":"task is no longer available"},"status":404}"""))
        { ThrowOnErrors = throwOnErrors };
        using var repository = new TestRepository(configuration);

        // Act
        var exception = await Assert.ThrowsAsync<DocumentException>(() =>
            repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"), o => o.UseUpdateByQuery().Retry(1)));

        // Assert
        Assert.Contains("unknown", exception.Message);
        Assert.Equal(2, configuration.Invoker.Requests);
        Assert.Equal(1, repository.QueryNotifications);
    }

    [Theory]
    [InlineData(429, 0)]
    [InlineData(429, 1)]
    [InlineData(503, 0)]
    [InlineData(503, 1)]
    public async Task PatchAllAsync_WithTransientTaskSubmission_RespectsRetryBudget(int status, int retries)
    {
        // Arrange
        const string completedTask = """
            {"completed":true,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/update/byquery","status":{"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0}},"response":{"took":1,"timed_out":false,"total":1,"updated":1,"created":0,"deleted":0,"version_conflicts":0,"failures":[]}}
            """;
        string errorType = status is 503 ? "unavailable_shards_exception" : "es_rejected_execution_exception";
        using var configuration = new TestConfiguration(
            ("_update_by_query", status, $$"""{"error":{"type":"{{errorType}}","reason":"injected"},"status":{{status}}}"""),
            ("_update_by_query", 200, """{"task":"node:1"}"""),
            ("_tasks", 200, completedTask));
        using var repository = new TestRepository(configuration);

        // Act
        if (retries is 0)
            await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"),
                o => o.UseUpdateByQuery().Retry(retries)));
        else
            Assert.Equal(1, await repository.PatchAllAsync(q => q, new ScriptPatch("ctx._source.count++"),
                o => o.UseUpdateByQuery().Retry(retries)));

        // Assert
        Assert.Equal(retries is 0 ? 1 : 3, configuration.Invoker.Requests);
        Assert.Equal(retries is 0 ? 0 : 1, repository.QueryNotifications);
    }

    private static string QueryResponse(int deleted, bool timedOut, bool itemFailure, int updated = 0)
    {
        string failures = itemFailure
            ? """[{"index":"query-tests","id":"two","status":500,"cause":{"type":"exception","reason":"injected failure"}}]"""
            : "[]";
        return $$"""{"took":1,"timed_out":{{timedOut.ToString().ToLowerInvariant()}},"total":2,"updated":{{updated}},"deleted":{{deleted}},"version_conflicts":0,"failures":{{failures}}}""";
    }

    private sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public int Count { get; set; }
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(ElasticConfiguration configuration, bool usePipeline = false, bool cacheEnabled = false) : base(new TestIndex(configuration))
        {
            if (!cacheEnabled)
                DisableCache();
            DefaultPipeline = usePipeline ? "test-pipeline" : null;
        }

        public int QueryNotifications { get; private set; }
        public List<string> QueryNotificationIds { get; } = [];
        public List<string?> DocumentNotificationIds { get; } = [];
        public List<string> QueryInvalidationIds { get; } = [];
        public bool ConfigureSelectionFilter { get; init; }
        public IRepositoryQuery<TestDocument>? ReplacementQuery { get; init; }

        public Task CacheDocumentAsync(string id) =>
            AddDocumentsToCacheAsync(new TestDocument { Id = id }, new CommandOptions().Cache(), false);

        public Task<FindHit<TestDocument>?> GetCachedDocumentAsync(string id) => GetCachedFindHit(id);

        protected override IRepositoryQuery ConfigureQuery(IRepositoryQuery<TestDocument> query)
        {
            var configured = base.ConfigureQuery(ReplacementQuery ?? query);
            if (ConfigureSelectionFilter)
                configured.FieldEquals("count", 42);
            return configured;
        }

        protected override Task SendQueryNotificationsAsync(ChangeType changeType, IRepositoryQuery query, ICommandOptions options)
        {
            QueryNotifications++;
            QueryNotificationIds.AddRange(query.GetIds());
            return Task.CompletedTask;
        }

        protected override Task PublishChangeTypeMessageAsync(ChangeType changeType, string? id, IDictionary<string, object?>? data = null, TimeSpan? delay = null)
        {
            DocumentNotificationIds.Add(id);
            return Task.CompletedTask;
        }

        protected override Task InvalidateCacheByQueryAsync(IRepositoryQuery<TestDocument> query)
        {
            QueryInvalidationIds.AddRange(query.GetIds());
            return base.InvalidateCacheByQueryAsync(query);
        }
    }

    private sealed class TestIndex(ElasticConfiguration configuration) : Index<TestDocument>(configuration, "query-tests")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class SerializationFailureConfiguration : ElasticConfiguration
    {
        public int HttpRequests { get; private set; }

        protected override IRequestInvoker CreateRequestInvoker() =>
            new HttpRequestInvoker((handler, _) => new UnexpectedHttpRequestHandler(handler, () => HttpRequests++));
    }

    private sealed class NativeTransportFailureConfiguration(Exception failure) : ElasticConfiguration
    {
        public int HttpRequests { get; private set; }

        protected override IRequestInvoker CreateRequestInvoker() =>
            new HttpRequestInvoker((handler, _) => new FailingHttpRequestHandler(handler, failure, () => HttpRequests++));
    }

    private sealed class FailingHttpRequestHandler(HttpMessageHandler inner, Exception failure, Action requestSent) : DelegatingHandler(inner)
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            requestSent();
            throw failure;
        }
    }

    private sealed class UnexpectedHttpRequestHandler(HttpMessageHandler inner, Action requestSent) : DelegatingHandler(inner)
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            requestSent();
            throw new InvalidOperationException("The request should fail during JSON serialization before HTTP dispatch.");
        }
    }

    private sealed class FailingScriptParameter
    {
        internal int SerializationAttempts { get; private set; }

        public string Value
        {
            get
            {
                SerializationAttempts++;
                throw new JsonException("injected request JSON failure");
            }
        }
    }

    private sealed class TestConfiguration(params (string Path, int Status, string Body)[] responses) : ElasticConfiguration
    {
        public CannedResponseInvoker Invoker { get; } = new(responses);
        public bool ThrowOnErrors { get; init; }
        public Action<ApiCallDetails>? RequestCompleted { get; set; }

        protected override IRequestInvoker CreateRequestInvoker() => Invoker;

        protected override void ConfigureSettings(ElasticsearchClientSettings settings)
        {
            base.ConfigureSettings(settings);
            settings.ThrowExceptions(ThrowOnErrors);
            settings.OnRequestCompleted(details => RequestCompleted?.Invoke(details));
        }
    }

    private sealed class CannedResponseInvoker(params (string Path, int Status, string Body)[] responses) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _responseFactoryOwner = new();
        private readonly Queue<(string Path, int Status, string Body)> _responses = new(responses);
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _responseFactoryOwner.ResponseFactory;
        public int Requests { get; private set; }
        public List<string> RequestBodies { get; } = [];
        public List<string> RequestPaths { get; } = [];
        public Action<string>? BeforeResponseRead { get; set; }

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            return RequestCoreAsync<TResponse>(false, endpoint, boundConfiguration, postData).GetAwaiter().GetResult();
        }

        private async Task<TResponse> RequestCoreAsync<TResponse>(bool isAsync, Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken = default)
            where TResponse : TransportResponse, new()
        {
            int status;
            string body;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                status = 200;
                body = """{"query-tests":{"mappings":{"properties":{"id":{"type":"keyword"}}}}}""";
            }
            else
            {
                Requests++;
                RequestPaths.Add(endpoint.PathAndQuery);
                using var requestStream = new MemoryStream();
                postData?.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                RequestBodies.Add(Encoding.UTF8.GetString(requestStream.ToArray()));
                var response = _responses.Dequeue();
                Assert.Contains(response.Path, endpoint.PathAndQuery, StringComparison.Ordinal);
                status = response.Status;
                body = response.Body;
            }

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var stream = new MemoryStream(bytes);
            BeforeResponseRead?.Invoke(endpoint.PathAndQuery);
            string contentType = body.StartsWith('{') ? "application/json" : body.StartsWith('<') ? "text/html" : "text/plain";
            return isAsync
                ? await ResponseFactory.CreateAsync<TResponse>(endpoint, boundConfiguration, postData, null, status, _headers, stream, contentType, bytes.Length, null, null, cancellationToken)
                : ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, status, _headers, stream, contentType, bytes.Length, null, null);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return RequestCoreAsync<TResponse>(true, endpoint, boundConfiguration, postData, cancellationToken);
        }

        public void Dispose() => ((IDisposable)_responseFactoryOwner).Dispose();
    }
}
