using System;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed partial class ReindexRecoveryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Sequential_RetiredSourceRejectsStaleWritesEvenWhenPostCutoverCallbackFails(bool failCallback)
        => WithIndexesAsync(async work =>
        {
            await PutDocumentAsync(work.OldIndex, "original");
            bool promoted = false;
            var migration = Reindexer().ReindexAsync(work, (progress, _) =>
            {
                if (progress is 99)
                {
                    promoted = true;
                    if (failCallback)
                        throw new InvalidOperationException("observer failed after promotion");
                }
                return Task.CompletedTask;
            }, TestCancellationToken);
            if (failCallback)
                await Assert.ThrowsAsync<InvalidOperationException>(() => migration);
            else
                await migration;

            Assert.True(promoted);
            var staleWrite = await _client.Transport.RequestAsync<StringResponse>(HttpMethod.PUT,
                $"/{work.OldIndex}/_doc/stale", PostData.String("{}"), TestCancellationToken);
            Assert.Equal(403, staleWrite.ApiCallDetails.HttpStatusCode);
            await PutDocumentAsync(work.Alias, "current");
            Assert.Equal(["current", "original"], await IdsAsync(work.NewIndex));

            var ownership = await ReindexSafetyState.ReadAsync(_client,
                ReindexSafetyState.GetId("block", work.OldIndex), TestCancellationToken);
            Assert.Equal("cutover", ownership?.Phase);
            await Assert.ThrowsAsync<ReindexCompletionUnknownException>(() =>
                IndexWriteBlock.RecoverAsync(_client, work, _logger, TestCancellationToken));
        });
}
