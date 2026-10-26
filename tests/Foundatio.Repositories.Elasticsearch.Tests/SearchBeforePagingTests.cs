using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class SearchBeforePagingTests(ITestOutputHelper output) : ElasticRepositoryTestBase(output)
{
    [Theory]
    [InlineData(SearchAfterPagingMode.Live, false, false)]
    [InlineData(SearchAfterPagingMode.Live, false, true)]
    [InlineData(SearchAfterPagingMode.Live, true, false)]
    [InlineData(SearchAfterPagingMode.Live, true, true)]
    [InlineData(SearchAfterPagingMode.PointInTime, false, false)]
    [InlineData(SearchAfterPagingMode.PointInTime, false, true)]
    [InlineData(SearchAfterPagingMode.PointInTime, true, false)]
    [InlineData(SearchAfterPagingMode.PointInTime, true, true)]
    public async Task FindAsync_WithMissingOrMultivaluedSort_ReversesEveryPage(SearchAfterPagingMode mode, bool multivalued, bool descending)
    {
        // Arrange
        using var index = new PagingIndex(_configuration, $"reverse-paging-{Guid.NewGuid():N}");
        using var repository = new PagingRepository(index);
        string? pitId = null;
        try
        {
            var documents = new[]
            {
                new PagingDocument { Id = "a", Values = multivalued ? ["a", "z"] : ["a"] },
                new PagingDocument { Id = "b", Values = multivalued ? ["b", "y"] : ["b"] },
                new PagingDocument { Id = "c", Values = ["c"] },
                new PagingDocument { Id = "d" }
            };
            await repository.AddAsync(documents, o => o.ImmediateConsistency());
            string[] expectedIds = descending && !multivalued ? ["c", "b", "a", "d"] : ["a", "b", "c", "d"];
            var query = descending
                ? new RepositoryQuery<PagingDocument>().SortDescending(d => d.Values)
                : new RepositoryQuery<PagingDocument>().SortAscending(d => d.Values);
            var page = await repository.FindAsync(query, new CommandOptions<PagingDocument>().PageLimit(1).SearchAfterPaging(mode));
            pitId = page.GetPointInTimeId();
            Assert.Equal(expectedIds[0], Assert.Single(page.Documents).Id);

            // Act / Assert: traverse forward, then back through the same sort tuple and query.
            foreach (string expected in expectedIds.Skip(1))
            {
                page = await repository.FindAsync(query, new CommandOptions<PagingDocument>().PageLimit(1)
                    .SearchAfterPaging(mode).PointInTimeId(pitId).SearchAfterToken(page.GetSearchAfterToken(), _serializer));
                pitId = page.GetPointInTimeId();
                Assert.Equal(expected, Assert.Single(page.Documents).Id);
            }

            foreach (string expected in expectedIds.Take(3).Reverse())
            {
                page = await repository.FindAsync(query, new CommandOptions<PagingDocument>().PageLimit(1)
                    .SearchAfterPaging(mode).PointInTimeId(pitId).SearchBeforeToken(page.GetSearchBeforeToken(), _serializer));
                pitId = page.GetPointInTimeId();
                Assert.Equal(expected, Assert.Single(page.Documents).Id);
            }
        }
        finally
        {
            if (pitId is not null)
                await repository.ClosePointInTimeAsync(pitId);
            await index.DeleteAsync();
        }
    }

    private sealed class PagingDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string[]? Values { get; set; }
    }

    private sealed class PagingIndex(IElasticConfiguration configuration, string name) : Index<PagingDocument>(configuration, name)
    {
        public override void ConfigureIndexMapping(TypeMappingDescriptor<PagingDocument> map)
        {
            map.Properties(p => p.Keyword(d => d.Id).Keyword(d => d.Values));
        }
    }

    private sealed class PagingRepository(IIndex index) : ElasticRepositoryBase<PagingDocument>(index);
}
