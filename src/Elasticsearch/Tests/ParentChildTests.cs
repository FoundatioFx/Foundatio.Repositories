using System;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
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

        // TODO: Test parent that doesn't support soft deletes
    }
}
