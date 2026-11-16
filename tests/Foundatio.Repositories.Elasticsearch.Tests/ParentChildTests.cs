using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ParentChildTests : ElasticRepositoryTestBase
{
    private readonly IParentRepository _parentRepository;
    private readonly IChildRepository _childRepository;

    public ParentChildTests(ITestOutputHelper output) : base(output)
    {
        _parentRepository = new ParentRepository(_configuration);
        _childRepository = new ChildRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task Add()
    {
        var parent = ParentGenerator.Default;
        parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
        Assert.NotNull(parent);
        Assert.NotNull(parent.Id);

        var child = ChildGenerator.Default;
        child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
        Assert.NotNull(child);
        Assert.NotNull(child.Id);

        child = await _childRepository.GetByIdAsync(new Id(child.Id, parent.Id));
        Assert.NotNull(child);
        Assert.NotNull(child.Id);

        child = await _childRepository.GetByIdAsync(child.Id);
        Assert.NotNull(child);
        Assert.NotNull(child.Id);
    }

    [Fact]
    public async Task GetByIds()
    {
        var parent1 = ParentGenerator.Generate();
        parent1 = await _parentRepository.AddAsync(parent1, o => o.ImmediateConsistency());
        Assert.NotNull(parent1);
        Assert.NotNull(parent1.Id);

        var child1 = ChildGenerator.Generate(parentId: parent1.Id);
        child1 = await _childRepository.AddAsync(child1, o => o.ImmediateConsistency());
        Assert.NotNull(child1);
        Assert.NotNull(child1.Id);

        var parent2 = ParentGenerator.Generate();
        parent2 = await _parentRepository.AddAsync(parent2, o => o.ImmediateConsistency());
        Assert.NotNull(parent2);
        Assert.NotNull(parent2.Id);

        var child2 = ChildGenerator.Generate(parentId: parent2.Id);
        child2 = await _childRepository.AddAsync(child2, o => o.ImmediateConsistency());
        Assert.NotNull(child2);
        Assert.NotNull(child2.Id);

        var ids = new Ids(child1.Id, child2.Id);

        var results = await _childRepository.GetByIdsAsync(ids);
        Assert.NotNull(results);
        Assert.Equal(2, results.Count);

        var idsWithRouting = new Ids(new Id(child1.Id, parent1.Id), new Id(child2.Id, parent2.Id));

        var resultsWithRouting = await _childRepository.GetByIdsAsync(idsWithRouting);
        Assert.NotNull(resultsWithRouting);
        Assert.Equal(2, resultsWithRouting.Count);
    }

    [Fact]
    public async Task SaveWithCache_WhenDifferentRoutesShareOneShard_ReturnsTheFinalPersistedChild()
    {
        // ParentChildIndex explicitly configures one primary shard, so both parent routes select the same shard.
        var firstParent = await _parentRepository.AddAsync(ParentGenerator.Generate(), options => options.ImmediateConsistency());
        var secondParent = await _parentRepository.AddAsync(ParentGenerator.Generate(), options => options.ImmediateConsistency());
        const string childId = "same-child-id";
        var first = new Child { Id = childId, ParentId = firstParent.Id, ChildProperty = "first source" };
        var second = new Child { Id = childId, ParentId = secondParent.Id, ChildProperty = "second source" };

        await _childRepository.SaveAsync(new[] { first, second }, options => options.SkipVersionCheck().Cache().ImmediateConsistency().Retry(0));

        foreach (string routing in new[] { firstParent.Id, secondParent.Id })
        {
            var id = new Id(childId, routing);
            var persisted = await _childRepository.GetByIdAsync(id, options => options.Cache(false));
            Assert.NotNull(persisted);
            Assert.Equal(secondParent.Id, persisted.ParentId);
            Assert.Equal("second source", persisted.ChildProperty);
            var cached = await _childRepository.GetByIdAsync(id, options => options.Cache());
            Assert.NotNull(cached);
            Assert.Equal(persisted.ParentId, cached.ParentId);
            Assert.Equal(persisted.ChildProperty, cached.ChildProperty);
        }
    }

    [Fact]
    public async Task DeletedParentWillFilterChild()
    {
        var parent = ParentGenerator.Default;
        parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
        Assert.NotNull(parent);
        Assert.NotNull(parent.Id);

        var child = ChildGenerator.Default;
        child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
        Assert.NotNull(child);
        Assert.NotNull(child.Id);

        parent.IsDeleted = true;
        await _parentRepository.SaveAsync(parent, o => o.ImmediateConsistency());
        Assert.Equal(0, await _childRepository.CountAsync());

        parent.IsDeleted = false;
        await _parentRepository.SaveAsync(parent, o => o.ImmediateConsistency());
        Assert.Equal(1, await _childRepository.CountAsync());
    }

    [Fact]
    public async Task CanQueryByParent()
    {
        var parent = ParentGenerator.Default;
        parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
        Assert.NotNull(parent);
        Assert.NotNull(parent.Id);

        await _parentRepository.AddAsync(ParentGenerator.Generate(), o => o.ImmediateConsistency());

        var child = ChildGenerator.Default;
        child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
        Assert.NotNull(child);
        Assert.NotNull(child.Id);

        var childResults = await _childRepository.FindAsync(q => q.ParentQuery(p => p.Id(parent.Id)), o => o.QueryLogLevel(LogLevel.Warning));
        Assert.Equal(1, childResults.Total);
    }

    [Fact]
    public async Task CanQueryByChild()
    {
        var parent = ParentGenerator.Default;
        parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
        Assert.NotNull(parent);
        Assert.NotNull(parent.Id);

        var child = ChildGenerator.Default;
        child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
        Assert.NotNull(child);
        Assert.NotNull(child.Id);

        await _childRepository.AddAsync(ChildGenerator.Generate(parentId: parent.Id), o => o.ImmediateConsistency());
        Assert.Equal(2, await _childRepository.CountAsync());

        var parentResults = await _parentRepository.FindAsync(q => q.ChildQuery(new RepositoryQuery<Child>().FilterExpression("id:" + child.Id)));
        Assert.Equal(1, parentResults.Total);
    }

    [Fact]
    public async Task CanDeleteParentChild()
    {
        var parent = ParentGenerator.Default;
        parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
        Assert.NotNull(parent);
        Assert.NotNull(parent.Id);

        var child = ChildGenerator.Default;
        child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
        Assert.NotNull(child);
        Assert.NotNull(child.Id);

        await _childRepository.RemoveAsync(child.Id, o => o.ImmediateConsistency());
        var result = await _childRepository.GetByIdAsync(child.Id);
        Assert.Null(result);
    }
}
