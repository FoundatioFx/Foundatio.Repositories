using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
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
