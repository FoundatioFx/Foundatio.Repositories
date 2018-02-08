using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {

    public sealed class AggregationQueryTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;

        public AggregationQueryTests(ITestOutputHelper output) : base(output) {
            _employeeRepository = new EmployeeRepository(_configuration);
            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task GetNumberAggregationsAsync() {
            await CreateDataAsync();

            const string aggregations = "min:age max:age avg:age sum:age percentiles:age min:createdUtc max:createdUtc";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(7, result.Aggregations.Count);
            Assert.Equal(19, result.Aggregations.Min("min_age").Value);
            Assert.Equal(60, result.Aggregations.Max("max_age").Value);
            Assert.Equal(34.7, result.Aggregations.Average("avg_age").Value);
            Assert.Equal(347, result.Aggregations.Sum("sum_age").Value);
            var percentiles = result.Aggregations.Percentiles("percentiles_age");
            Assert.Equal(SystemClock.UtcNow.Date.SubtractYears(10), result.Aggregations.Min<DateTime>("min_createdUtc").Value);
            Assert.Equal(SystemClock.UtcNow.Date.SubtractYears(1), result.Aggregations.Max<DateTime>("max_createdUtc").Value);
            Assert.Equal(19.27, percentiles.GetPercentile(1).Value);
            Assert.Equal(20.35, percentiles.GetPercentile(5).Value);
            Assert.Equal(26d, percentiles.GetPercentile(25).Value);
            Assert.Equal(30.5, percentiles.GetPercentile(50).Value);
            Assert.Equal(42.5, percentiles.GetPercentile(75).Value);
            Assert.Equal(55.94999999999999, percentiles.GetPercentile(95).Value);
            Assert.Equal(59.19, percentiles.GetPercentile(99).Value);
        }

        [Fact]
        public async Task GetNumberAggregationsWithFilterAsync() {
            await CreateDataAsync();

            const string aggregations = "min:age max:age avg:age sum:age";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.FilterExpression("age: <40").AggregationsExpression(aggregations));
            Assert.Equal(7, result.Total);
            Assert.Equal(4, result.Aggregations.Count);
            Assert.Equal(19, result.Aggregations.Min("min_age").Value);
            Assert.Equal(35, result.Aggregations.Min("max_age").Value);
            Assert.Equal(Math.Round(27.2857142857143, 5), Math.Round(result.Aggregations.Average("avg_age").Value.GetValueOrDefault(), 5));
            Assert.Equal(191, result.Aggregations.Sum("sum_age").Value);
        }

        [Fact]
        public async Task GetAliasedNumberAggregationsWithFilterAsync() {
            await CreateDataAsync();

            const string aggregations = "min:aliasedage max:aliasedage avg:aliasedage sum:aliasedage";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.FilterExpression("aliasedage: <40").AggregationsExpression(aggregations));
            Assert.Equal(7, result.Total);
            Assert.Equal(4, result.Aggregations.Count);
            Assert.Equal(19, result.Aggregations.Min("min_aliasedage").Value);
            Assert.Equal(35, result.Aggregations.Min("max_aliasedage").Value);
            Assert.Equal(Math.Round(27.2857142857143, 5), Math.Round(result.Aggregations.Average("avg_aliasedage").Value.GetValueOrDefault(), 5));
            Assert.Equal(191, result.Aggregations.Sum("sum_aliasedage").Value);
        }

        [Fact]
        public async Task GetNestedAliasedNumberAggregationsWithFilterAsync() {
            await _employeeRepository.AddAsync(new Employee {
                Name = "Blake",
                Age = 30,
                Data = new Dictionary<string, object> { { "@user_meta", new { twitter_id = "blaken", twitter_followers = 1000 } } }
            }, o => o.ImmediateConsistency());

            const string aggregations = "min:followers max:followers avg:followers sum:followers cardinality:twitter";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(1, result.Total);
            Assert.Equal(5, result.Aggregations.Count);
            Assert.Equal(1000, result.Aggregations.Min("min_followers").Value);
            Assert.Equal(1000, result.Aggregations.Min("max_followers").Value);
            Assert.Equal(1000, result.Aggregations.Average("avg_followers").Value.GetValueOrDefault());
            Assert.Equal(1000, result.Aggregations.Sum("sum_followers").Value);
            Assert.Equal(1, result.Aggregations.Cardinality("cardinality_twitter").Value);
        }

        [Fact]
        public async Task GetNestedAggregationsAsync() {
            var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
            var employees = new List<Employee> {
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1))
            };
            employees[0].Id = "employee1";
            employees[0].Id = "employee2";
            employees[0].PeerReviews = new PeerReview[] { new PeerReview { ReviewerEmployeeId = employees[1].Id, Rating = 4 } };
            employees[1].PeerReviews = new PeerReview[] { new PeerReview { ReviewerEmployeeId = employees[0].Id, Rating = 5 } };

            await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

            var nestedAggQuery = _client.Search<Employee>(d => d.Index("employees").Aggregations(a => a
               .Nested("nested_reviewRating", h => h.Path("peerReviews")
                   .Aggregations(a1 => a1.Terms("terms_rating", t => t.Field("peerReviews.rating").Meta(m => m.Add("@field_type", "integer")))))
                ));

            var result = nestedAggQuery.Aggregations.ToAggregations();
            Assert.Equal(1, result.Count);
            Assert.Equal(2, ((result["nested_reviewRating"] as Foundatio.Repositories.Models.SingleBucketAggregate).Aggregations["terms_rating"] as Foundatio.Repositories.Models.BucketAggregate).Items.Count);

            var nestedAggQueryWithFilter = _client.Search<Employee>(d => d.Index("employees").Aggregations(a => a
               .Nested("nested_reviewRating", h => h.Path("peerReviews")
                    .Aggregations(a1 => a1
                        .Filter("user_" + employees[0].Id, f => f.Filter(q => q.Term(t => t.Field("peerReviews.reviewerEmployeeId").Value(employees[0].Id)))
                            .Aggregations(a2 => a2.Terms("terms_rating", t => t.Field("peerReviews.rating").Meta(m => m.Add("@field_type", "integer")))))
                ))));

            result = nestedAggQueryWithFilter.Aggregations.ToAggregations();
            Assert.Equal(1, result.Count);

            var filteredAgg = ((result["nested_reviewRating"] as Foundatio.Repositories.Models.SingleBucketAggregate).Aggregations["user_" + employees[0].Id] as Foundatio.Repositories.Models.SingleBucketAggregate);
            Assert.NotNull(filteredAgg);
            Assert.Equal(1, filteredAgg.Aggregations.Terms("terms_rating").Buckets.Count);
            Assert.Equal("5", filteredAgg.Aggregations.Terms("terms_rating").Buckets.First().Key );
            Assert.Equal(1, filteredAgg.Aggregations.Terms("terms_rating").Buckets.First().Total);
        }

        [Fact]
        public async Task GetAliasedNumberAggregationThatCausesMappingAsync() {
            await _employeeRepository.AddAsync(new Employee {
                Name = "Blake",
                Age = 30,
                NextReview = DateTimeOffset.UtcNow,
                Data = new Dictionary<string, object> { { "@user_meta", new { twitter_id = "blaken", twitter_followers = 1000 } } }
            }, o => o.ImmediateConsistency());

            var thisWillTriggerMappingRefresh = await _employeeRepository.GetCountByQueryAsync(q => q.FilterExpression("fieldDoestExist:true"));
            Assert.Equal(0, thisWillTriggerMappingRefresh.Total);

            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression("cardinality:twitter"));
            Assert.Equal(1, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(1, result.Aggregations.Cardinality("cardinality_twitter").Value);
        }

        [Fact]
        public async Task GetCardinalityAggregationsAsync() {
            await CreateDataAsync();

            const string aggregations = "cardinality:location";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(2, result.Aggregations.Cardinality("cardinality_location").Value);
        }

        [Fact]
        public async Task GetMissingAggregationsAsync() {
            await CreateDataAsync();

            const string aggregations = "missing:companyName";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(10, result.Aggregations.Missing("missing_companyName").Total);
        }

        [Fact]
        public async Task GetDateUtcAggregationsAsync() {
            var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
                EmployeeGenerator.Generate(nextReview: utcToday)
            }, o => o.ImmediateConsistency());

            const string aggregations = "min:nextReview max:nextReview date:nextReview";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(3, result.Total);
            Assert.Equal(3, result.Aggregations.Count);

            AssertEqual(utcToday.UtcDateTime.SubtractDays(2), result.Aggregations.Min<DateTime>("min_nextReview")?.Value);
            AssertEqual(utcToday.UtcDateTime, result.Aggregations.Max<DateTime>("max_nextReview")?.Value);

            var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
            Assert.Equal(3, dateHistogramAgg.Buckets.Count);
            var oldestDate = utcToday.UtcDateTime.Date.SubtractDays(2);
            foreach (var bucket in dateHistogramAgg.Buckets) {
                AssertEqual(oldestDate, bucket.Date);
                Assert.Equal(1, bucket.Total);
                oldestDate = oldestDate.AddDays(1);
            }
        }

        [Fact]
        public async Task GetDateUtcAggregationsWithOffsetsAsync() {
            var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
                EmployeeGenerator.Generate(nextReview: utcToday)
            }, o => o.ImmediateConsistency());

            const string aggregations = "min:nextReview^1h max:nextReview^1h date:nextReview^1h";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(3, result.Total);
            Assert.Equal(3, result.Aggregations.Count);

            AssertEqual(DateTime.SpecifyKind(utcToday.UtcDateTime.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified), result.Aggregations.Min<DateTime>("min_nextReview")?.Value);
            AssertEqual(DateTime.SpecifyKind(utcToday.UtcDateTime.SubtractHours(1), DateTimeKind.Unspecified), result.Aggregations.Max<DateTime>("max_nextReview")?.Value);

            var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
            Assert.Equal(3, dateHistogramAgg.Buckets.Count);
            var oldestDate = DateTime.SpecifyKind(utcToday.UtcDateTime.Date.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified);
            foreach (var bucket in dateHistogramAgg.Buckets) {
                AssertEqual(oldestDate, bucket.Date);
                Assert.Equal(1, bucket.Total);
                oldestDate = oldestDate.AddDays(1);
            }
        }

        [Fact]
        public async Task GetDateUtcAggregationsWithNegativeOffsetAsync() {
            var utcToday = SystemClock.OffsetUtcNow.UtcDateTime.Date;
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
                EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
                EmployeeGenerator.Generate(nextReview: utcToday)
            }, o => o.ImmediateConsistency());

            const double offsetInMinutes = 600;
            string aggregations = $"min:nextReview^-{offsetInMinutes}m max:nextReview^-{offsetInMinutes}m date:nextReview^-{offsetInMinutes}m";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(3, result.Total);
            Assert.Equal(3, result.Aggregations.Count);

            AssertEqual(DateTime.SpecifyKind(utcToday.SubtractDays(2).AddMinutes(offsetInMinutes), DateTimeKind.Unspecified), result.Aggregations.Min<DateTime>("min_nextReview")?.Value);
            AssertEqual(DateTime.SpecifyKind(utcToday.AddMinutes(offsetInMinutes), DateTimeKind.Unspecified), result.Aggregations.Max<DateTime>("max_nextReview")?.Value);

            var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
            Assert.Equal(3, dateHistogramAgg.Buckets.Count);
            var oldestDate = DateTime.SpecifyKind(utcToday.SubtractDays(3).AddMinutes(offsetInMinutes), DateTimeKind.Unspecified); // it's minus 3 days because the offset puts it a day back.
            foreach (var bucket in dateHistogramAgg.Buckets) {
                AssertEqual(oldestDate, bucket.Date);
                Assert.Equal(1, bucket.Total);
                oldestDate = oldestDate.AddDays(1);
            }
        }

        public static IEnumerable<object[]> DatesToCheck => new List<object[]> {
            new object[] { new DateTime(2016, 2, 29, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2016, 8, 31, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2016, 9, 1, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2016, 9, 1, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2017, 3, 1, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2017, 4, 10, 18, 43, 39, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2017, 4, 10, 23, 0, 0, 0, DateTimeKind.Utc) },
            new object[] { new DateTime(2017, 12, 31, 11, 59, 59, DateTimeKind.Utc).EndOfDay() },
            new object[] { SystemClock.UtcNow }
        }.ToArray();

        [Theory]
        [MemberData(nameof(DatesToCheck))]
        public async Task GetDateOffsetAggregationsAsync(DateTime utcNow) {
            using (TestSystemClock.Install()) {
                SystemClock.Test.SetFixedTime(utcNow);

                var today = SystemClock.OffsetNow.Floor(TimeSpan.FromMilliseconds(1));

                await _employeeRepository.AddAsync(new List<Employee> {
                    EmployeeGenerator.Generate(nextReview: today.SubtractDays(2)),
                    EmployeeGenerator.Generate(nextReview: today.SubtractDays(1)),
                    EmployeeGenerator.Generate(nextReview: today)
                }, o => o.ImmediateConsistency());

                const string aggregations = "min:nextReview max:nextReview date:nextReview";
                var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
                Assert.Equal(3, result.Total);
                Assert.Equal(3, result.Aggregations.Count);

                // Dates are always returned in utc.
                AssertEqual(DateTime.SpecifyKind(today.UtcDateTime.SubtractDays(2), DateTimeKind.Utc), result.Aggregations.Min<DateTime>("min_nextReview")?.Value);
                AssertEqual(DateTime.SpecifyKind(today.UtcDateTime, DateTimeKind.Utc), result.Aggregations.Max<DateTime>("max_nextReview")?.Value);

                var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
                Assert.Equal(3, dateHistogramAgg.Buckets.Count);
                var oldestDate = DateTime.SpecifyKind(today.UtcDateTime.Date.SubtractDays(2), DateTimeKind.Utc);
                foreach (var bucket in dateHistogramAgg.Buckets) {
                    AssertEqual(oldestDate, bucket.Date);
                    Assert.Equal(1, bucket.Total);
                    oldestDate = oldestDate.AddDays(1);
                }
            }
        }

        [Fact(Skip = "Need to fix it, its flakey")]
        public async Task GetDateOffsetAggregationsWithOffsetsAsync() {
            var today = SystemClock.OffsetNow.Floor(TimeSpan.FromMilliseconds(1));
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(nextReview: today.SubtractDays(2)),
                EmployeeGenerator.Generate(nextReview: today.SubtractDays(1)),
                EmployeeGenerator.Generate(nextReview: today)
            }, o => o.ImmediateConsistency());

            const string aggregations = "min:nextReview^1h max:nextReview^1h date:nextReview^1h";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(3, result.Total);
            Assert.Equal(3, result.Aggregations.Count);

            // Dates are always returned in utc.
            AssertEqual(DateTime.SpecifyKind(today.UtcDateTime.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified), result.Aggregations.Min<DateTime>("min_nextReview")?.Value);
            AssertEqual(DateTime.SpecifyKind(today.UtcDateTime.SubtractHours(1), DateTimeKind.Unspecified), result.Aggregations.Max<DateTime>("max_nextReview")?.Value);

            var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
            Assert.Equal(3, dateHistogramAgg.Buckets.Count);
            var oldestDate = DateTime.SpecifyKind(today.UtcDateTime.Date.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified);
            foreach (var bucket in dateHistogramAgg.Buckets) {
                AssertEqual(oldestDate, bucket.Date);
                Assert.Equal(1, bucket.Total);
                oldestDate = oldestDate.AddDays(1);
            }
        }

        private void AssertEqual(DateTime expected, DateTime? actual) {
            Assert.NotNull(actual);
            Assert.Equal(expected, actual);
            Assert.Equal(expected.Kind, actual.Value.Kind);
        }

        [Fact]
        public async Task GetGeoGridAggregationsAsync() {
            await CreateDataAsync();

            const string aggregations = "geogrid:location";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);

            var bucket = result.Aggregations.GeoHash("geogrid_location").Buckets.Single();
            Assert.Equal("s", bucket.Key);
            Assert.Equal(10, bucket.Total);
            Assert.Equal(Math.Round(14.9999999860302, 5), Math.Round(bucket.Aggregations.Average("avg_lat").Value.GetValueOrDefault(), 5));
            Assert.Equal(Math.Round(14.9999999860302, 5), Math.Round(bucket.Aggregations.Average("avg_lon").Value.GetValueOrDefault(), 5));
        }

        [Fact]
        public async Task GetTermAggregationsAsync() {
            await CreateDataAsync();

            const string aggregations = "terms:age";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(10, result.Aggregations.Terms<int>("terms_age").Buckets.Count);
            Assert.Equal(1, result.Aggregations.Terms<int>("terms_age").Buckets.First(f => f.Key == 19).Total);

            var json = JsonConvert.SerializeObject(result);
            var roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
            Assert.Equal(10, roundTripped.Total);
            Assert.Equal(1, roundTripped.Aggregations.Count);
            Assert.Equal(10, roundTripped.Aggregations.Terms<int>("terms_age").Buckets.Count);
            Assert.Equal(1, roundTripped.Aggregations.Terms<int>("terms_age").Buckets.First(f => f.Key == 19).Total);
        }

        [Fact]
        public async Task GetTermAggregationsWithTopHitsAsync() {
            await CreateDataAsync();

            const string aggregations = "terms:(age tophits:_)";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(10, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(10, result.Aggregations.Terms<int>("terms_age").Buckets.Count);
            var bucket = result.Aggregations.Terms<int>("terms_age").Buckets.First(f => f.Key == 19);
            Assert.Equal(1, bucket.Total);
            
            var tophits = bucket.Aggregations.TopHits();
            Assert.NotNull(tophits);
            var employees = tophits.Documents<Employee>();
            Assert.Equal(1, employees.Count);
            Assert.Equal(19, employees.First().Age);
            Assert.Equal(1, employees.First().YearsEmployed);

            var json = JsonConvert.SerializeObject(result);
            var roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
            Assert.Equal(10, roundTripped.Total);
            Assert.Equal(1, roundTripped.Aggregations.Count);
            Assert.Equal(10, roundTripped.Aggregations.Terms<int>("terms_age").Buckets.Count);
            bucket = roundTripped.Aggregations.Terms<int>("terms_age").Buckets.First(f => f.Key == 19);
            Assert.Equal(1, bucket.Total);
            
            // TODO: Do we need to be able to roundtrip this? I think we need to for caching purposes.

            // tophits = bucket.Aggregations.TopHits();
            // Assert.NotNull(tophits);
            // employees = tophits.Documents<Employee>();
            // Assert.Equal(1, employees.Count);
            // Assert.Equal(19, employees.First().Age);
            // Assert.Equal(1, employees.First().YearsEmployed);
        }

        [Fact]
        public async Task CanGetDupesAsync() {
            await CreateDataAsync();
            await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 45, yearsEmployed: 12, location: "15,20", createdUtc: DateTime.UtcNow.Date.SubtractYears(12), updatedUtc: DateTime.UtcNow.Date.SubtractYears(12)), o => o.ImmediateConsistency());

            const string aggregations = "terms:(age^2 tophits:(_~100))";
            var result = await _employeeRepository.GetCountByQueryAsync(q => q.AggregationsExpression(aggregations));
            Assert.Equal(11, result.Total);
            Assert.Equal(1, result.Aggregations.Count);
            Assert.Equal(1, result.Aggregations.Terms<int>("terms_age").Buckets.Count);
            var bucket = result.Aggregations.Terms<int>("terms_age").Buckets.First(f => f.Key == 45);
            Assert.Equal(2, bucket.Total);
            
            var tophits = bucket.Aggregations.TopHits();
            Assert.NotNull(tophits);
            var employees = tophits.Documents<Employee>();
            Assert.Equal(2, employees.Count);
            Assert.Equal(45, employees.First().Age);
            Assert.Equal(8, employees.First().YearsEmployed);
        }

        internal async Task CreateDataAsync() {
            var utcToday = SystemClock.UtcNow.Date;
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(age: 19, yearsEmployed: 1,  location: "10,10", createdUtc: utcToday.SubtractYears(1), updatedUtc: utcToday.SubtractYears(1)),
                EmployeeGenerator.Generate(age: 22, yearsEmployed: 2,  location: "10,10", createdUtc: utcToday.SubtractYears(2), updatedUtc: utcToday.SubtractYears(2)),
                EmployeeGenerator.Generate(age: 25, yearsEmployed: 3,  location: "10,10", createdUtc: utcToday.SubtractYears(3), updatedUtc: utcToday.SubtractYears(3)),
                EmployeeGenerator.Generate(age: 29, yearsEmployed: 4,  location: "10,10", createdUtc: utcToday.SubtractYears(4), updatedUtc: utcToday.SubtractYears(4)),
                EmployeeGenerator.Generate(age: 30, yearsEmployed: 5,  location: "10,10", createdUtc: utcToday.SubtractYears(5), updatedUtc: utcToday.SubtractYears(5)),
                EmployeeGenerator.Generate(age: 31, yearsEmployed: 6,  location: "20,20", createdUtc: utcToday.SubtractYears(6), updatedUtc: utcToday.SubtractYears(6)),
                EmployeeGenerator.Generate(age: 35, yearsEmployed: 7,  location: "20,20", createdUtc: utcToday.SubtractYears(7), updatedUtc: utcToday.SubtractYears(7)),
                EmployeeGenerator.Generate(age: 45, yearsEmployed: 8,  location: "20,20", createdUtc: utcToday.SubtractYears(8), updatedUtc: utcToday.SubtractYears(8)),
                EmployeeGenerator.Generate(age: 51, yearsEmployed: 9,  location: "20,20", createdUtc: utcToday.SubtractYears(9), updatedUtc: utcToday.SubtractYears(9)),
                EmployeeGenerator.Generate(age: 60, yearsEmployed: 10, location: "20,20", createdUtc: utcToday.SubtractYears(10), updatedUtc: utcToday.SubtractYears(10))
            }, o => o.ImmediateConsistency());
        }
    }
}