using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class AggregationQueryTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;

        public AggregationQueryTests(ITestOutputHelper output) : base(output) {
            _employeeRepository = new EmployeeRepository(_configuration);

            RemoveDataAsync().GetAwaiter().GetResult();
            CreateDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task GetNumberAggregations() {
            const string aggregations = "min:age max:age avg:age sum:age percentiles:age";
            var result = await _employeeRepository.GetCountByQueryAsync(new MyAppQuery().WithAggregations(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(5, result.Aggregations.Count);
            Assert.Equal(19, result.Aggregations["min_age"].Value);
            Assert.Equal(60, result.Aggregations["max_age"].Value);
            Assert.Equal(34.7, result.Aggregations["avg_age"].Value);
            Assert.Equal(347, result.Aggregations["sum_age"].Value);
            Assert.Equal(19.27, result.Aggregations["percentiles_age"].Data["1"]);
            Assert.Equal(20.35, result.Aggregations["percentiles_age"].Data["5"]);
            Assert.Equal(26d, result.Aggregations["percentiles_age"].Data["25"]);
            Assert.Equal(30.5, result.Aggregations["percentiles_age"].Data["50"]);
            Assert.Equal(42.5, result.Aggregations["percentiles_age"].Data["75"]);
            Assert.Equal(55.94999999999999, result.Aggregations["percentiles_age"].Data["95"]);
            Assert.Equal(59.19, result.Aggregations["percentiles_age"].Data["99"]);
        }

        [Fact]
        public async Task GetNumberAggregationsWithFilter() {
            const string aggregations = "min:age max:age avg:age sum:age";
            var result = await _employeeRepository.GetCountByQueryAsync(new MyAppQuery().WithFilter("age: <40").WithAggregations(aggregations));
            Assert.Equal(7, result.Total);
            Assert.Equal(5, result.Aggregations.Count);
            Assert.Equal(19, result.Aggregations["min_age"].Value);
            Assert.Equal(35, result.Aggregations["max_age"].Value);
            Assert.Equal(Math.Round(27.2857142857143, 5), Math.Round(result.Aggregations["avg_age"].Value.GetValueOrDefault(), 5));
            Assert.Equal(191, result.Aggregations["sum_age"].Value);
            Assert.Equal(191, result.Aggregations["percentiles_age"].Value);
        }

        [Fact]
        public async Task GetCardinalityAggregations() {
            const string aggregations = "cardinality:location";
            var result = await _employeeRepository.GetCountByQueryAsync(new MyAppQuery().WithAggregations(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(2, result.Aggregations["cardinality_location"].Value);
        }

        [Fact]
        public async Task GetMissingAggregations() {
            const string aggregations = "missing:companyName";
            var result = await _employeeRepository.GetCountByQueryAsync(new MyAppQuery().WithAggregations(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(10, result.Aggregations["missing_companyName"].Value);
        }

        [Fact]
        public async Task GetDateAggregations() {
            const string aggregations = "date:updatedUtc";
            var result = await _employeeRepository.GetCountByQueryAsync(new MyAppQuery().WithAggregations(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(1, result.Aggregations["date_updatedUtc"].Buckets.Count);
        }

        [Fact]
        public async Task GetGeoGridAggregations() {
            const string aggregations = "geogrid:location";
            var result = await _employeeRepository.GetCountByQueryAsync(new MyAppQuery().WithAggregations(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);

            var bucket = result.Aggregations["geogrid_location"].Buckets.Single();
            Assert.Equal("s", bucket.Key);
            Assert.Equal(10, bucket.Total);
            Assert.Equal(Math.Round(14.9999999860302, 5), Math.Round(bucket.Aggregations["avg_lat"].Value.GetValueOrDefault(), 5));
            Assert.Equal(Math.Round(14.9999999860302, 5), Math.Round(bucket.Aggregations["avg_lon"].Value.GetValueOrDefault(), 5));
        }

        [Fact]
        public async Task GetTermAggregations() {
            const string aggregations = "terms:age";
            var result = await _employeeRepository.GetCountByQueryAsync(new MyAppQuery().WithAggregations(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(10, result.Aggregations["terms_age"].Buckets.Count);
            Assert.Equal(1, result.Aggregations["terms_age"].Buckets.First(f => f.Key == "19.0").Total);
        }

        public async Task CreateDataAsync() {
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(age: 19, yearsEmployed: 1,  location: "10,10", createdUtc: DateTime.Today.SubtractYears(1)),
                EmployeeGenerator.Generate(age: 22, yearsEmployed: 2,  location: "10,10", createdUtc: DateTime.Today.SubtractYears(2)),
                EmployeeGenerator.Generate(age: 25, yearsEmployed: 3,  location: "10,10", createdUtc: DateTime.Today.SubtractYears(3)),
                EmployeeGenerator.Generate(age: 29, yearsEmployed: 4,  location: "10,10", createdUtc: DateTime.Today.SubtractYears(4)),
                EmployeeGenerator.Generate(age: 30, yearsEmployed: 5,  location: "10,10", createdUtc: DateTime.Today.SubtractYears(5)),
                EmployeeGenerator.Generate(age: 31, yearsEmployed: 6,  location: "20,20", createdUtc: DateTime.Today.SubtractYears(6)),
                EmployeeGenerator.Generate(age: 35, yearsEmployed: 7,  location: "20,20", createdUtc: DateTime.Today.SubtractYears(7)),
                EmployeeGenerator.Generate(age: 45, yearsEmployed: 8,  location: "20,20", createdUtc: DateTime.Today.SubtractYears(8)),
                EmployeeGenerator.Generate(age: 51, yearsEmployed: 9,  location: "20,20", createdUtc: DateTime.Today.SubtractYears(9)),
                EmployeeGenerator.Generate(age: 60, yearsEmployed: 10, location: "20,20", createdUtc: DateTime.Today.SubtractYears(10))
            });
            await _client.RefreshAsync(Indices.AllIndices);
        }
    }
}