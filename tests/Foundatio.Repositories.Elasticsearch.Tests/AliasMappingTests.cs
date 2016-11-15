using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class AliasMappingVisitorTests : ElasticRepositoryTestBase {
        public AliasMappingVisitorTests(ITestOutputHelper output) : base(output) {
        }

        [Fact]
        public void VerifySimpleAlias() {
            var visitor = new AliasMappingVisitor(_client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(new TypeMappingDescriptor<Employee>().Properties(p => p
                .Keyword(f => f.Name(e => e.Id).Alias("employee_id"))));

            var map = visitor.RootAliasMap;
            Assert.Equal(1, map.Count);
            Assert.True(map.ContainsKey("id"));
            Assert.Equal("employee_id", map["id"].Name);
            Assert.False(map["id"].HasChildMappings);
        }

        [Fact]
        public void VerifyNestedAlias() {
            var visitor = new AliasMappingVisitor(_client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(new TypeMappingDescriptor<Employee>().Properties(p => p
                .Keyword(f => f.Name(e => e.Id).Alias("employee_id"))
                .Object<object>(o => o.Name("Data").Properties(p1 => p1
                    .Keyword(f => f.Name("Profile_URL").Alias("url"))))));

            var map = visitor.RootAliasMap;
            Assert.Equal(2, map.Count);
            Assert.True(map.ContainsKey("id"));
            Assert.Equal("employee_id", map["id"].Name);
            Assert.False(map["id"].HasChildMappings);

            Assert.True(map.ContainsKey("Data"));
            Assert.Null(map["Data"].Name);
            Assert.True(map["Data"].HasChildMappings);
            Assert.Equal(1, map["Data"].ChildMap.Count);
            Assert.True(map["Data"].ChildMap.ContainsKey("Profile_URL"));
            Assert.Equal("url", map["Data"].ChildMap["Profile_URL"].Name);
            Assert.False(map["Data"].ChildMap["Profile_URL"].HasChildMappings);
        }
    }
}