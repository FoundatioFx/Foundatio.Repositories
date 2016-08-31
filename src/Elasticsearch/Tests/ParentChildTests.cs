using System;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Queries;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class ParentChildTests : ElasticRepositoryTestBase {
        private readonly ParentRepository _parentRepository;
        private readonly ChildRepository _childRepository;

        public ParentChildTests(ITestOutputHelper output) : base(output) {
            _parentRepository = new ParentRepository(_configuration, _cache, Log.CreateLogger<ParentRepository>());
            _childRepository = new ChildRepository(_configuration, _cache, Log.CreateLogger<ChildRepository>());

            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task Add() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent);
            Assert.NotNull(parent?.Id);

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child);
            Assert.NotNull(child?.Id);

            await _client.RefreshAsync();
            child = await _childRepository.GetByIdAsync(child.Id);
            Assert.NotNull(child?.Id);
        }

        [Fact]
        public async Task DeletedParentWillFilterChild() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent);
            Assert.NotNull(parent?.Id);

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child);
            Assert.NotNull(child?.Id);

            parent.IsDeleted = true;
            await _parentRepository.SaveAsync(parent);

            await _client.RefreshAsync();
            Assert.Equal(0, await _childRepository.CountBySearchAsync(null));

            parent.IsDeleted = false;
            await _parentRepository.SaveAsync(parent);

            await _client.RefreshAsync();
            Assert.Equal(1, await _childRepository.CountBySearchAsync(null));
        }

        [Fact]
        public async Task CanQueryByParent() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent);
            Assert.NotNull(parent?.Id);

            await _parentRepository.AddAsync(ParentGenerator.Generate());

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child);
            Assert.NotNull(child?.Id);

            await _client.RefreshAsync();
            var childResults = await _childRepository.QueryAsync(new MyAppQuery().WithParentQuery(q => q.WithId(parent.Id)));
            Assert.Equal(1, childResults.Total);
        }

        [Fact]
        public async Task CanQueryByChild() {
            var parent = ParentGenerator.Default;
            parent = await _parentRepository.AddAsync(parent);
            Assert.NotNull(parent?.Id);

            var child = ChildGenerator.Default;
            child = await _childRepository.AddAsync(child);
            Assert.NotNull(child?.Id);

            await _childRepository.AddAsync(ChildGenerator.Generate(parentId: parent.Id));
            await _client.RefreshAsync();
            Assert.Equal(2, await _childRepository.CountAsync());

            await _client.RefreshAsync();
            var parentResults = await _parentRepository.QueryAsync(new MyAppQuery().WithChildQuery(q => q.WithType("child").WithFilter("id:" + child.Id)));
            Assert.Equal(1, parentResults.Total);
        }

        // TODO: Test parent that doesn't support soft deletes
    }
}
