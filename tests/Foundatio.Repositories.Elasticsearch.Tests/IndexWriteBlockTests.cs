using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

/// <summary>
/// Covers the write block that lets a reindex reconcile a copy against a source that cannot change underneath it.
/// </summary>
/// <remarks>
/// The block is applied through a raw transport call because the typed client exposes no equivalent, and its
/// response is validated by hand. That makes these tests the only thing standing between a hand-rolled request and
/// a silently ineffective block, so they assert the observable effect - a rejected write - rather than the shape of
/// the response.
/// </remarks>
public sealed class IndexWriteBlockTests : ElasticRepositoryTestBase
{
    public IndexWriteBlockTests(ITestOutputHelper output) : base(output)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    private async Task<VersionedEmployeeIndex> CreateSeededIndexAsync()
    {
        var index = new VersionedEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();
        await index.ConfigureAsync();

        IEmployeeRepository repository = new EmployeeRepository(_configuration);
        await repository.AddAsync(EmployeeGenerator.GenerateEmployees(3), o => o.ImmediateConsistency());

        return index;
    }

    private Task<bool> TryWriteAsync(string index)
    {
        return TryWriteAsync(index, "probe-" + Guid.NewGuid().ToString("N"));
    }

    private async Task<bool> TryWriteAsync(string index, string id)
    {
        var response = await _client.IndexAsync(EmployeeGenerator.Default, i => i.Index(index).Id(id), TestCancellationToken);

        return response.IsValidResponse;
    }

    [Fact]
    public async Task ApplyAsync_WhileHeld_RejectsWritesButAllowsReads()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        Assert.True(await TryWriteAsync(index.VersionedName));

        var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken);
        await using (block)
        {
            Assert.False(await TryWriteAsync(index.VersionedName));

            // Reads must keep working, because the copy still has to scroll the source while it is blocked.
            var search = await _client.SearchAsync<Employee>(d => d.Indices(index.VersionedName), TestCancellationToken);
            Assert.True(search.IsValidResponse);
            Assert.NotEmpty(search.Documents);
        }
    }

    [Fact]
    public async Task DisposeAsync_RestoresWrites()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken);
        Assert.False(await TryWriteAsync(index.VersionedName));

        await block.DisposeAsync();

        Assert.True(await TryWriteAsync(index.VersionedName));
    }

    /// <summary>
    /// The block must not survive a failure, because it persists on the index.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_WhenCallerThrows_StillRestoresWrites()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        var thrown = await Record.ExceptionAsync(async () =>
        {
            await using var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken);
            Assert.False(await TryWriteAsync(index.VersionedName));

            throw new InvalidOperationException("the reconcile failed");
        });

        Assert.IsType<InvalidOperationException>(thrown);
        Assert.True(await TryWriteAsync(index.VersionedName));
    }

    [Fact]
    public async Task DisposeAsync_WhenCallerIsCancelled_StillRestoresWrites()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        using var cancellation = new CancellationTokenSource();
        var thrown = await Record.ExceptionAsync(async () =>
        {
            await using var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), cancellation.Token);
            Assert.False(await TryWriteAsync(index.VersionedName));

            await cancellation.CancelAsync();
            cancellation.Token.ThrowIfCancellationRequested();
        });

        Assert.IsAssignableFrom<OperationCanceledException>(thrown);
        Assert.True(await TryWriteAsync(index.VersionedName));
    }

    [Fact]
    public async Task DisposeAsync_WhenReleaseFails_DoesNotMaskTheCallersException()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        var thrown = await Record.ExceptionAsync(async () =>
        {
            var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken);
            try
            {
                throw new InvalidOperationException("the reconcile failed");
            }
            finally
            {
                // Deleting the index makes the release request fail with a 404. A throwing dispose would replace
                // the caller's exception with that failure, hiding why the migration actually failed.
                await index.DeleteAsync();
                await block.DisposeAsync();
            }
        });

        Assert.IsType<InvalidOperationException>(thrown);
        Assert.Equal("the reconcile failed", thrown.Message);
    }

    [Fact]
    public async Task ReleaseAsync_WhenReleaseFails_Throws()
    {
        var index = await CreateSeededIndexAsync();

        var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken);
        await index.DeleteAsync();

        var thrown = await Record.ExceptionAsync(() => block.ReleaseAsync());

        Assert.IsType<RepositoryException>(thrown);
        Assert.Contains("must be unblocked manually", thrown.Message);
    }

    [Fact]
    public async Task ReleaseAsync_ThenDisposeAsync_RestoresWritesOnlyOnce()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        await using (var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken))
        {
            Assert.False(await TryWriteAsync(index.VersionedName));
            await block.ReleaseAsync();
            Assert.True(await TryWriteAsync(index.VersionedName));
        }

        Assert.True(await TryWriteAsync(index.VersionedName));
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsSafe()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken);

        await block.DisposeAsync();
        await block.DisposeAsync();

        Assert.True(await TryWriteAsync(index.VersionedName));
    }

    /// <summary>
    /// A block the caller established for their own reasons is not ours to lift.
    /// </summary>
    /// <remarks>
    /// Releasing it would silently re-open an index the operator had deliberately closed to writes, which is a
    /// worse outcome than leaving a block in place that they already know about.
    /// </remarks>
    private Task<StringResponse> SetWriteBlockSettingAsync(string index, string value)
    {
        return _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT,
            $"/{index}/_settings", PostData.String($"{{\"index.blocks.write\":{value}}}"), TestCancellationToken);
    }

    [Fact]
    public async Task DisposeAsync_WhenIndexWasAlreadyBlocked_LeavesTheBlockInPlace()
    {
        var index = await CreateSeededIndexAsync();
        await using AsyncDisposableAction _ = new(async () =>
        {
            await SetWriteBlockSettingAsync(index.VersionedName, "null");
            await index.DeleteAsync();
        });

        var preBlock = await SetWriteBlockSettingAsync(index.VersionedName, "true");
        Assert.True(preBlock.ApiCallDetails.HasSuccessfulStatusCode);
        Assert.False(await TryWriteAsync(index.VersionedName));

        var block = await IndexWriteBlock.ApplyAsync(_client, index.VersionedName, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken);
        await block.DisposeAsync();

        Assert.False(await TryWriteAsync(index.VersionedName));
    }

    [Fact]
    public async Task ApplyAsync_WhenIndexDoesNotExist_Throws()
    {
        var exception = await Record.ExceptionAsync(async () =>
            await IndexWriteBlock.ApplyAsync(_client, "index-that-does-not-exist", Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken));

        Assert.IsType<RepositoryException>(exception);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public Task ApplyAsync_WithInvalidIndex_Throws(string? index)
    {
        return Assert.ThrowsAnyAsync<ArgumentException>(async () =>
            await IndexWriteBlock.ApplyAsync(_client, index!, Log.CreateLogger<IndexWriteBlockTests>(), TestCancellationToken));
    }
}
