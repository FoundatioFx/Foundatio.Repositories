using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Utility;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class ParentChildTests : ElasticRepositoryTestBase {
        private readonly ParentRepository _parentRepository;
        private readonly ChildRepository _childRepository;

        public ParentChildTests(ITestOutputHelper output) : base(output) {
            _parentRepository = new ParentRepository(_configuration);
            _childRepository = new ChildRepository(_configuration);

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task Add() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
            Assert.NotNull(parent?.Id);

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
            Assert.NotNull(child?.Id);

            child = await _childRepository.GetByIdAsync(new Id(child.Id, parent.Id));
            Assert.NotNull(child?.Id);

            child = await _childRepository.GetByIdAsync(child.Id);
            Assert.NotNull(child?.Id);
        }

        [Fact]
        public async Task GetByIds() {
            var parent1 = ParentGenerator.Generate();
            parent1 = await _parentRepository.AddAsync(parent1, o => o.ImmediateConsistency());
            Assert.NotNull(parent1?.Id);

            var child1 = ChildGenerator.Generate(parentId: parent1.Id);
            child1 = await _childRepository.AddAsync(child1, o => o.ImmediateConsistency());
            Assert.NotNull(child1?.Id);

            var parent2 = ParentGenerator.Generate();
            parent2 = await _parentRepository.AddAsync(parent2, o => o.ImmediateConsistency());
            Assert.NotNull(parent2?.Id);

            var child2 = ChildGenerator.Generate(parentId: parent2.Id);
            child2 = await _childRepository.AddAsync(child2, o => o.ImmediateConsistency());
            Assert.NotNull(child2?.Id);

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
        public async Task DeletedParentWillFilterChild() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
            Assert.NotNull(parent?.Id);

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
            Assert.NotNull(child?.Id);

            parent.IsDeleted = true;
            await _parentRepository.SaveAsync(parent, o => o.ImmediateConsistency());
            Assert.Equal(0, await _childRepository.CountBySearchAsync(null));

            parent.IsDeleted = false;
            await _parentRepository.SaveAsync(parent, o => o.ImmediateConsistency());
            Assert.Equal(1, await _childRepository.CountBySearchAsync(null));
        }

        [Fact]
        public async Task CanQueryByParent() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent);
            Assert.NotNull(parent?.Id);

            await _parentRepository.AddAsync(ParentGenerator.Generate());

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
            Assert.NotNull(child?.Id);

            var childResults = await _childRepository.QueryAsync(q => q.ParentQuery(p => p.Id(parent.Id)));
            Assert.Equal(1, childResults.Total);
        }

        [Fact]
        public async Task CanQueryByChild() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
            Assert.NotNull(parent?.Id);

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
            Assert.NotNull(child?.Id);

            await _childRepository.AddAsync(ChildGenerator.Generate(parentId: parent.Id), o => o.ImmediateConsistency());
            Assert.Equal(2, await _childRepository.CountAsync());

            var parentResults = await _parentRepository.QueryAsync(q => q.ChildQuery(typeof(Child), c => c.FilterExpression("id:" + child.Id)));
            Assert.Equal(1, parentResults.Total);
        }

        [Fact]
        public async Task CanDeleteParentChild() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent, o => o.ImmediateConsistency());
            Assert.NotNull(parent?.Id);

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child, o => o.ImmediateConsistency());
            Assert.NotNull(child?.Id);

            await _childRepository.RemoveAsync(child.Id, o => o.ImmediateConsistency());
            var result = await _childRepository.GetByIdAsync(child.Id);
            Assert.Null(result);
        }
    }
}
