using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DeleteByQueryRetryIntegrationTests : ElasticRepositoryTestBase
{
    private readonly IdentityWithNoCachingRepository _repository;

    public DeleteByQueryRetryIntegrationTests(ITestOutputHelper output) : base(output)
    {
        _repository = new IdentityWithNoCachingRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Theory]
    [InlineData(0, 7, 3)]
    [InlineData(1, 10, 0)]
    public async Task RespectsRetryBudgetForKnownVersionConflictsAsync(int retries, long expectedDeleted, long expectedRemaining)
    {
        // Freeze search visibility so a subsequent real-time update reliably conflicts with the
        // search snapshot. No concurrent writer, scheduler delay, or probabilistic conflict is needed.
        try
        {
            var settings = await _client.Indices.PutSettingsAsync(_configuration.Identities.Name,
                s => s.Settings(index => index.RefreshInterval(new Duration("-1"))), TestCancellationToken);
            Assert.True(settings.IsValidResponse, settings.DebugInformation);

            var identities = IdentityGenerator.GenerateIdentities(10);
            await _repository.AddAsync(identities, o => o.ImmediateConsistency());
            string[] patchedIds = identities.Take(3).Select(identity => identity.Id).ToArray();
            foreach (string id in patchedIds)
                await _repository.PatchAsync(id, new ScriptPatch("ctx._source.conflictMarker = true;"),
                    o => o.Consistency(Consistency.Eventual).Notifications(false));

            // Prove the conflict setup against Elasticsearch before exercising the repository.
            // This probe targets only the updated documents and must not remove any of them.
            var probe = await _client.DeleteByQueryAsync(new DeleteByQueryRequest(_configuration.Identities.Name)
            {
                Conflicts = Conflicts.Proceed,
                Refresh = false,
                Query = new IdsQuery { Values = patchedIds }
            }, TestCancellationToken);
            Assert.True(probe.IsValidResponse, probe.DebugInformation);
            Assert.Equal(0, probe.Deleted);
            Assert.Equal(3, probe.VersionConflicts);
            _messageBus.ResetMessagesSent();

            // The first pass deletes seven documents and conflicts on three. Immediate consistency
            // refreshes after that pass, so one retry can then delete the three remaining versions.
            long deleted = await _repository.RemoveAllAsync(o => o.ImmediateConsistency().Retry(retries));

            Assert.Equal(expectedDeleted, deleted);
            Assert.Equal(expectedRemaining, await _repository.CountAsync());
            Assert.Equal(2, _messageBus.MessagesSent);
        }
        finally
        {
            // Cleanup must survive test cancellation but still have its own bounded lifetime.
            using var cleanupCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var restored = await _client.Indices.PutSettingsAsync(_configuration.Identities.Name,
                s => s.Settings(index => index.RefreshInterval(new Duration("1s"))), cleanupCancellation.Token);
            Assert.True(restored.IsValidResponse, restored.DebugInformation);
        }
    }

    public override ValueTask DisposeAsync()
    {
        _repository.Dispose();
        return base.DisposeAsync();
    }
}
