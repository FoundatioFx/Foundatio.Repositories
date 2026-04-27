using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Core.Search;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Microsoft.Extensions.Time.Testing;
using Newtonsoft.Json;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class AggregationQueryTests : ElasticRepositoryTestBase
{
    private readonly IEmployeeRepository _employeeRepository;

    public AggregationQueryTests(ITestOutputHelper output) : base(output)
    {
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task GetNumberAggregationsAsync()
    {
        await CreateDataAsync();

        const string aggregations = "min:age max:age avg:age sum:age percentiles:age min:createdUtc max:createdUtc";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(10, result.Total);
        Assert.Equal(7, result.Aggregations.Count);
        var minAge = result.Aggregations.Min("min_age");
        Assert.NotNull(minAge);
        Assert.Equal(19, minAge.Value);
        var maxAge = result.Aggregations.Max("max_age");
        Assert.NotNull(maxAge);
        Assert.Equal(60, maxAge.Value);
        var avgAge = result.Aggregations.Average("avg_age");
        Assert.NotNull(avgAge);
        Assert.Equal(34.7, avgAge.Value);
        var sumAge = result.Aggregations.Sum("sum_age");
        Assert.NotNull(sumAge);
        Assert.Equal(347, sumAge.Value);
        var percentiles = result.Aggregations.Percentiles("percentiles_age");
        Assert.NotNull(percentiles);
        var minCreatedUtc = result.Aggregations.Min<DateTime>("min_createdUtc");
        Assert.NotNull(minCreatedUtc);
        Assert.Equal(DateTime.UtcNow.Date.SubtractYears(10), minCreatedUtc.Value);
        var maxCreatedUtc = result.Aggregations.Max<DateTime>("max_createdUtc");
        Assert.NotNull(maxCreatedUtc);
        Assert.Equal(DateTime.UtcNow.Date.SubtractYears(1), maxCreatedUtc.Value);
        var p1 = percentiles.GetPercentile(1);
        Assert.NotNull(p1);
        Assert.Equal(19.27, p1.Value);
        var p5 = percentiles.GetPercentile(5);
        Assert.NotNull(p5);
        Assert.Equal(20.35, p5.Value);
        var p25 = percentiles.GetPercentile(25);
        Assert.NotNull(p25);
        Assert.Equal(26d, p25.Value);
        var p50 = percentiles.GetPercentile(50);
        Assert.NotNull(p50);
        Assert.Equal(30.5, p50.Value);
        var p75 = percentiles.GetPercentile(75);
        Assert.NotNull(p75);
        Assert.Equal(42.5, p75.Value);
        var p95 = percentiles.GetPercentile(95);
        Assert.NotNull(p95);
        Assert.NotNull(p95.Value);
        Assert.Equal(55.95d, Math.Round((double)p95.Value, 2));
        var p99 = percentiles.GetPercentile(99);
        Assert.NotNull(p99);
        Assert.Equal(59.19, p99.Value);
    }

    [Fact]
    public async Task GetNumberAggregationsWithFilterAsync()
    {
        await CreateDataAsync();

        const string aggregations = "min:age max:age avg:age sum:age";
        var result = await _employeeRepository.CountAsync(q => q.FilterExpression("age: <40").AggregationsExpression(aggregations));
        Assert.Equal(7, result.Total);
        Assert.Equal(4, result.Aggregations.Count);
        var minAge = result.Aggregations.Min("min_age");
        Assert.NotNull(minAge);
        Assert.Equal(19, minAge.Value);
        var maxAge = result.Aggregations.Min("max_age");
        Assert.NotNull(maxAge);
        Assert.Equal(35, maxAge.Value);
        var avgAge = result.Aggregations.Average("avg_age");
        Assert.NotNull(avgAge);
        Assert.Equal(Math.Round(27.2857142857143, 5), Math.Round(avgAge.Value.GetValueOrDefault(), 5));
        var sumAge = result.Aggregations.Sum("sum_age");
        Assert.NotNull(sumAge);
        Assert.Equal(191, sumAge.Value);
    }

    [Fact]
    public async Task GetAliasedNumberAggregationsWithFilterAsync()
    {
        await CreateDataAsync();

        const string aggregations = "min:aliasedage max:aliasedage avg:aliasedage sum:aliasedage";
        var result = await _employeeRepository.CountAsync(q => q.FilterExpression("aliasedage: <40").AggregationsExpression(aggregations));
        Assert.Equal(7, result.Total);
        Assert.Equal(4, result.Aggregations.Count);
        var minAliasedAge = result.Aggregations.Min("min_aliasedage");
        Assert.NotNull(minAliasedAge);
        Assert.Equal(19, minAliasedAge.Value);
        var maxAliasedAge = result.Aggregations.Min("max_aliasedage");
        Assert.NotNull(maxAliasedAge);
        Assert.Equal(35, maxAliasedAge.Value);
        var avgAliasedAge = result.Aggregations.Average("avg_aliasedage");
        Assert.NotNull(avgAliasedAge);
        Assert.Equal(Math.Round(27.2857142857143, 5), Math.Round(avgAliasedAge.Value.GetValueOrDefault(), 5));
        var sumAliasedAge = result.Aggregations.Sum("sum_aliasedage");
        Assert.NotNull(sumAliasedAge);
        Assert.Equal(191, sumAliasedAge.Value);
    }

    [Fact]
    public async Task GetNestedAliasedNumberAggregationsWithFilterAsync()
    {
        await _employeeRepository.AddAsync(new Employee
        {
            Name = "Blake",
            Age = 30,
            Data = new Dictionary<string, object?> { { "@user_meta", new { twitter_id = "blaken", twitter_followers = 1000 } } }
        }, o => o.ImmediateConsistency());

        const string aggregations = "min:followers max:followers avg:followers sum:followers cardinality:twitter";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(1, result.Total);
        Assert.Equal(5, result.Aggregations.Count);
        var minFollowers = result.Aggregations.Min("min_followers");
        Assert.NotNull(minFollowers);
        Assert.Equal(1000, minFollowers.Value);
        var maxFollowers = result.Aggregations.Min("max_followers");
        Assert.NotNull(maxFollowers);
        Assert.Equal(1000, maxFollowers.Value);
        var avgFollowers = result.Aggregations.Average("avg_followers");
        Assert.NotNull(avgFollowers);
        Assert.Equal(1000, avgFollowers.Value.GetValueOrDefault());
        var sumFollowers = result.Aggregations.Sum("sum_followers");
        Assert.NotNull(sumFollowers);
        Assert.Equal(1000, sumFollowers.Value);
        var cardinalityTwitter = result.Aggregations.Cardinality("cardinality_twitter");
        Assert.NotNull(cardinalityTwitter);
        Assert.Equal(1, cardinalityTwitter.Value);
    }

    [Fact]
    public async Task GetNestedAggregationsAsync_WithPeerReviews_ReturnsNestedAndFilteredBuckets()
    {
        // Arrange
        var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        var employees = new List<Employee> {
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1))
        };
        employees[0].Id = "employee1";
        employees[1].Id = "employee2";
        employees[0].PeerReviews = new PeerReview[] { new PeerReview { ReviewerEmployeeId = employees[1].Id, Rating = 4 } };
        employees[1].PeerReviews = new PeerReview[] { new PeerReview { ReviewerEmployeeId = employees[0].Id, Rating = 5 } };

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var nestedAggQuery = await _client.SearchAsync<Employee>(d => d.Indices("employees").Aggregations(a => a
           .Add("nested_reviewRating", agg => agg
               .Nested(h => h.Path("peerReviews"))
               .Aggregations(a1 => a1.Add("terms_rating", t => t.Terms(t1 => t1.Field("peerReviews.rating")).Meta(m => m.Add("@field_type", "integer")))))
            ), cancellationToken: TestCancellationToken);

        // Assert
        var aggs = nestedAggQuery.Aggregations;
        Assert.NotNull(aggs);
        var result = aggs.ToAggregations(_serializer);
        Assert.NotNull(result);
        Assert.Single(result);
        Assert.Equal(2, ((Foundatio.Repositories.Models.BucketAggregate)((Foundatio.Repositories.Models.SingleBucketAggregate)result["nested_reviewRating"]).Aggregations["terms_rating"]).Items.Count);

        // Act (with filter)
        var nestedAggQueryWithFilter = await _client.SearchAsync<Employee>(d => d.Indices("employees").Aggregations(a => a
           .Add("nested_reviewRating", agg => agg
               .Nested(h => h.Path("peerReviews"))
               .Aggregations(a1 => a1
                    .Add($"user_{employees[0].Id}", f => f
                        .Filter(q => q.Term(t => t.Field("peerReviews.reviewerEmployeeId").Value(employees[0].Id)))
                        .Aggregations(a2 => a2.Add("terms_rating", t => t.Terms(t1 => t1.Field("peerReviews.rating")).Meta(m => m.Add("@field_type", "integer")))))
            ))), cancellationToken: TestCancellationToken);

        // Assert (with filter)
        var aggsWithFilter = nestedAggQueryWithFilter.Aggregations;
        Assert.NotNull(aggsWithFilter);
        result = aggsWithFilter.ToAggregations(_serializer);
        Assert.NotNull(result);
        Assert.Single(result);

        var nestedAgg = (Foundatio.Repositories.Models.SingleBucketAggregate)result["nested_reviewRating"];
        var filteredAgg = (Foundatio.Repositories.Models.SingleBucketAggregate)nestedAgg.Aggregations[$"user_{employees[0].Id}"];
        Assert.NotNull(filteredAgg);
        var termsRating = filteredAgg.Aggregations.Terms("terms_rating");
        Assert.NotNull(termsRating);
        Assert.Single(termsRating.Buckets);
        Assert.Equal("5", termsRating.Buckets.First().Key);
        Assert.Equal(1, termsRating.Buckets.First().Total);
    }

    [Fact]
    public async Task GetAliasedNumberAggregationThatCausesMappingAsync()
    {
        await _employeeRepository.AddAsync(new Employee
        {
            Name = "Blake",
            Age = 30,
            NextReview = DateTimeOffset.UtcNow,
            Data = new Dictionary<string, object?> { { "@user_meta", new { twitter_id = "blaken", twitter_followers = 1000 } } }
        }, o => o.ImmediateConsistency());

        var thisWillTriggerMappingRefresh = await _employeeRepository.CountAsync(q => q.FilterExpression("fieldDoestExist:true"));
        Assert.Equal(0, thisWillTriggerMappingRefresh.Total);

        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression("cardinality:twitter"));
        Assert.Equal(1, result.Total);
        Assert.Single(result.Aggregations);
        var cardinalityTwitter = result.Aggregations.Cardinality("cardinality_twitter");
        Assert.NotNull(cardinalityTwitter);
        Assert.Equal(1, cardinalityTwitter.Value);
    }

    [Fact]
    public async Task GetCardinalityAggregationsAsync()
    {
        await CreateDataAsync();

        const string aggregations = "cardinality:location";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(10, result.Total);
        Assert.Single(result.Aggregations);
        var cardinalityLocation = result.Aggregations.Cardinality("cardinality_location");
        Assert.NotNull(cardinalityLocation);
        Assert.Equal(2, cardinalityLocation.Value);
    }

    [Fact]
    public async Task GetMissingAggregationsAsync()
    {
        await CreateDataAsync();

        const string aggregations = "missing:companyName";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(10, result.Total);
        Assert.Single(result.Aggregations);
        var missingCompanyName = result.Aggregations.Missing("missing_companyName");
        Assert.NotNull(missingCompanyName);
        Assert.Equal(10, missingCompanyName.Total);
    }

    [Fact]
    public async Task GetDateUtcAggregationsAsync()
    {
        var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5)).UtcDateTime;
        await _employeeRepository.AddAsync(new List<Employee> {
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
            EmployeeGenerator.Generate(nextReview: utcToday)
        }, o => o.ImmediateConsistency());

        const string aggregations = "min:nextReview max:nextReview date:nextReview";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(3, result.Total);
        Assert.Equal(3, result.Aggregations.Count);

        var minNextReview = result.Aggregations.Min<DateTime>("min_nextReview");
        Assert.NotNull(minNextReview);
        AssertEqual(utcToday.SubtractDays(2), minNextReview.Value);
        var maxNextReview = result.Aggregations.Max<DateTime>("max_nextReview");
        Assert.NotNull(maxNextReview);
        AssertEqual(utcToday, maxNextReview.Value);

        var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
        Assert.NotNull(dateHistogramAgg);
        Assert.Equal(3, dateHistogramAgg.Buckets.Count);
        var oldestDate = utcToday.Date.SubtractDays(2);
        foreach (var bucket in dateHistogramAgg.Buckets)
        {
            AssertEqual(oldestDate, bucket.Date);
            Assert.Equal(1, bucket.Total);
            oldestDate = oldestDate.AddDays(1);
        }
    }

    [Fact]
    public async Task GetDateUtcAggregationsWithOffsetsAsync()
    {
        var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5)).UtcDateTime;
        await _employeeRepository.AddAsync(new List<Employee> {
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
            EmployeeGenerator.Generate(nextReview: utcToday)
        }, o => o.ImmediateConsistency());

        const string aggregations = "min:nextReview^1h max:nextReview^1h date:nextReview^1h";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(3, result.Total);
        Assert.Equal(3, result.Aggregations.Count);

        var minNextReview = result.Aggregations.Min<DateTime>("min_nextReview");
        Assert.NotNull(minNextReview);
        AssertEqual(DateTime.SpecifyKind(utcToday.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified), minNextReview.Value);
        var maxNextReview = result.Aggregations.Max<DateTime>("max_nextReview");
        Assert.NotNull(maxNextReview);
        AssertEqual(DateTime.SpecifyKind(utcToday.SubtractHours(1), DateTimeKind.Unspecified), maxNextReview.Value);

        var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
        Assert.NotNull(dateHistogramAgg);
        Assert.Equal(3, dateHistogramAgg.Buckets.Count);
        var oldestDate = DateTime.SpecifyKind(utcToday.Date.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified);
        foreach (var bucket in dateHistogramAgg.Buckets)
        {
            AssertEqual(oldestDate, bucket.Date);
            Assert.Equal(1, bucket.Total);
            oldestDate = oldestDate.AddDays(1);
        }
    }

    [Fact]
    public async Task GetDateUtcAggregationsWithNegativeOffsetAsync()
    {
        var utcToday = DateTimeOffset.UtcNow.UtcDateTime.Date;
        await _employeeRepository.AddAsync(new List<Employee> {
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
            EmployeeGenerator.Generate(nextReview: utcToday)
        }, o => o.ImmediateConsistency());

        const double offsetInMinutes = 600;
        string aggregations = $"min:nextReview^-{offsetInMinutes}m max:nextReview^-{offsetInMinutes}m date:nextReview^-{offsetInMinutes}m";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(3, result.Total);
        Assert.Equal(3, result.Aggregations.Count);

        var minNextReview = result.Aggregations.Min<DateTime>("min_nextReview");
        Assert.NotNull(minNextReview);
        AssertEqual(DateTime.SpecifyKind(utcToday.SubtractDays(2).AddMinutes(offsetInMinutes), DateTimeKind.Unspecified), minNextReview.Value);
        var maxNextReview = result.Aggregations.Max<DateTime>("max_nextReview");
        Assert.NotNull(maxNextReview);
        AssertEqual(DateTime.SpecifyKind(utcToday.AddMinutes(offsetInMinutes), DateTimeKind.Unspecified), maxNextReview.Value);

        var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
        Assert.NotNull(dateHistogramAgg);
        Assert.Equal(3, dateHistogramAgg.Buckets.Count);
        var oldestDate = DateTime.SpecifyKind(utcToday.SubtractDays(3).AddMinutes(offsetInMinutes), DateTimeKind.Unspecified); // it's minus 3 days because the offset puts it a day back.
        foreach (var bucket in dateHistogramAgg.Buckets)
        {
            AssertEqual(oldestDate, bucket.Date);
            Assert.Equal(1, bucket.Total);
            oldestDate = oldestDate.AddDays(1);
        }
    }

    public static IEnumerable<object[]> DatesToCheck => new List<object[]> {
        new object[] { new DateTime(2016, 2, 29, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2016, 8, 31, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2016, 9, 1, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2017, 3, 1, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2017, 4, 10, 18, 43, 39, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2017, 4, 10, 23, 0, 0, 0, DateTimeKind.Utc) },
        new object[] { new DateTime(2017, 12, 31, 11, 59, 59, DateTimeKind.Utc).EndOfDay() },
        new object[] { DateTime.UtcNow }
    }.ToArray();

    [Theory]
    [MemberData(nameof(DatesToCheck))]
    public async Task GetDateOffsetAggregationsAsync(DateTime utcNow)
    {
        _employeeRepository.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));
        var today = utcNow.Floor(TimeSpan.FromMilliseconds(1));

        await _employeeRepository.AddAsync(new List<Employee>
        {
            EmployeeGenerator.Generate(nextReview: today.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: today.SubtractDays(1)),
            EmployeeGenerator.Generate(nextReview: today)
        }, o => o.ImmediateConsistency());

        const string aggregations = "min:nextReview max:nextReview date:nextReview";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(3, result.Total);
        Assert.Equal(3, result.Aggregations.Count);

        // Dates are always returned in utc.
        var minNextReview = result.Aggregations.Min<DateTime>("min_nextReview");
        Assert.NotNull(minNextReview);
        Assert.Equal(DateTime.SpecifyKind(today.SubtractDays(2), DateTimeKind.Utc), minNextReview.Value);
        var maxNextReview = result.Aggregations.Max<DateTime>("max_nextReview");
        Assert.NotNull(maxNextReview);
        Assert.Equal(DateTime.SpecifyKind(today, DateTimeKind.Utc), maxNextReview.Value);

        var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
        Assert.NotNull(dateHistogramAgg);
        Assert.Equal(3, dateHistogramAgg.Buckets.Count);
        var oldestDate = DateTime.SpecifyKind(today.Date.SubtractDays(2), DateTimeKind.Utc);
        foreach (var bucket in dateHistogramAgg.Buckets)
        {
            Assert.Equal(oldestDate, bucket.Date);
            Assert.Equal(1, bucket.Total);
            oldestDate = oldestDate.AddDays(1);
        }
    }

    [Fact]
    public async Task GetDateOffsetAggregationsWithOffsetsAsync()
    {
        var today = new DateTimeOffset(DateTimeOffset.UtcNow.Date, TimeSpan.Zero);
        await _employeeRepository.AddAsync(new List<Employee> {
            EmployeeGenerator.Generate(nextReview: today.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: today.SubtractDays(1)),
            EmployeeGenerator.Generate(nextReview: today)
        }, o => o.ImmediateConsistency());

        const string aggregations = "min:nextReview^1h max:nextReview^1h date:nextReview^1h";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(3, result.Total);
        Assert.Equal(3, result.Aggregations.Count);

        // Dates are always returned in utc.
        var minNextReview = result.Aggregations.Min<DateTime>("min_nextReview");
        Assert.NotNull(minNextReview);
        AssertEqual(DateTime.SpecifyKind(today.UtcDateTime.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified), minNextReview.Value);
        var maxNextReview = result.Aggregations.Max<DateTime>("max_nextReview");
        Assert.NotNull(maxNextReview);
        AssertEqual(DateTime.SpecifyKind(today.UtcDateTime.SubtractHours(1), DateTimeKind.Unspecified), maxNextReview.Value);

        var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
        Assert.NotNull(dateHistogramAgg);
        Assert.Equal(3, dateHistogramAgg.Buckets.Count);
        var oldestDate = DateTime.SpecifyKind(today.UtcDateTime.SubtractDays(2).AddHours(1).Date.SubtractHours(1), DateTimeKind.Unspecified);
        foreach (var bucket in dateHistogramAgg.Buckets)
        {
            AssertEqual(oldestDate, bucket.Date);
            Assert.Equal(1, bucket.Total);
            oldestDate = oldestDate.AddDays(1);
        }
    }

    private static void AssertEqual(DateTime expected, DateTime? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected, actual);
        Assert.Equal(expected.Kind, actual.Value.Kind);
    }

    [Fact]
    public async Task GetGeoGridAggregationsAsync()
    {
        await CreateDataAsync();

        const string aggregations = "geogrid:location";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(10, result.Total);
        Assert.Single(result.Aggregations);

        var geoHashAgg = result.Aggregations.GeoHash("geogrid_location");
        Assert.NotNull(geoHashAgg);
        var bucket = geoHashAgg.Buckets.Single();
        Assert.Equal("s", bucket.Key);
        Assert.Equal(10, bucket.Total);
        var avgLat = bucket.Aggregations.Average("avg_lat");
        Assert.NotNull(avgLat);
        Assert.Equal(Math.Round(14.9999999860302, 5), Math.Round(avgLat.Value.GetValueOrDefault(), 5));
        var avgLon = bucket.Aggregations.Average("avg_lon");
        Assert.NotNull(avgLon);
        Assert.Equal(Math.Round(14.9999999860302, 5), Math.Round(avgLon.Value.GetValueOrDefault(), 5));
    }

    [Fact]
    public async Task GetTermAggregationsAsync()
    {
        await CreateDataAsync();

        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression("terms:age"));
        Assert.Equal(10, result.Total);
        Assert.Single(result.Aggregations);
        var termsAge = result.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(termsAge);
        Assert.Equal(10, termsAge.Buckets.Count);
        Assert.Equal(1, termsAge.Buckets.First(f => f.Key == 19).Total);

        string json = JsonConvert.SerializeObject(result);
        var roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
        Assert.NotNull(roundTripped);
        Assert.Equal(10, roundTripped.Total);
        Assert.Single(roundTripped.Aggregations);
        var roundTrippedTermsAge = roundTripped.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(roundTrippedTermsAge);
        Assert.Equal(10, roundTrippedTermsAge.Buckets.Count);
        Assert.Equal(1, roundTrippedTermsAge.Buckets.First(f => f.Key == 19).Total);

        result = await _employeeRepository.CountAsync(q => q.AggregationsExpression("terms:(age~2 @missing:0 terms:(years~2 @missing:0))"));
        Assert.Equal(10, result.Total);
        Assert.Single(result.Aggregations);
        termsAge = result.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(termsAge);
        Assert.Equal(2, termsAge.Buckets.Count);
        var bucket = termsAge.Buckets.First(f => f.Key == 19);
        Assert.Equal(1, bucket.Total);
        Assert.Single(bucket.Aggregations);
        var termsYears = bucket.Aggregations.Terms<int>("terms_years");
        Assert.NotNull(termsYears);
        Assert.Single(termsYears.Buckets);

        json = JsonConvert.SerializeObject(result, Formatting.Indented);
        roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
        Assert.NotNull(roundTripped);
        string roundTrippedJson = JsonConvert.SerializeObject(roundTripped, Formatting.Indented);
        Assert.Equal(json, roundTrippedJson);
        Assert.Equal(10, roundTripped.Total);
        Assert.Single(roundTripped.Aggregations);
        var roundTrippedTermsAge2 = roundTripped.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(roundTrippedTermsAge2);
        Assert.Equal(2, roundTrippedTermsAge2.Buckets.Count);
        bucket = roundTrippedTermsAge2.Buckets.First(f => f.Key == 19);
        Assert.Equal(1, bucket.Total);
        Assert.Single(bucket.Aggregations);
        var roundTrippedTermsYears = bucket.Aggregations.Terms<int>("terms_years");
        Assert.NotNull(roundTrippedTermsYears);
        Assert.Single(roundTrippedTermsYears.Buckets);
    }

    [Fact]
    public async Task GetDateAggregationsAsync()
    {
        var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        await _employeeRepository.AddAsync(new List<Employee> {
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
            EmployeeGenerator.Generate(nextReview: utcToday)
        }, o => o.ImmediateConsistency());

        const string aggregations = "date:nextReview^1h";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(3, result.Total);
        Assert.Single(result.Aggregations);

        var dateHistogramAgg = result.Aggregations.DateHistogram("date_nextReview");
        Assert.NotNull(dateHistogramAgg);
        Assert.Equal(3, dateHistogramAgg.Buckets.Count);
        var oldestDate = DateTime.SpecifyKind(utcToday.UtcDateTime.Date.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified);
        foreach (var bucket in dateHistogramAgg.Buckets)
        {
            AssertEqual(oldestDate, bucket.Date);
            Assert.Equal(1, bucket.Total);
            oldestDate = oldestDate.AddDays(1);
        }

        string json = JsonConvert.SerializeObject(result);
        var roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
        Assert.NotNull(roundTripped);

        dateHistogramAgg = roundTripped.Aggregations.DateHistogram("date_nextReview");
        Assert.NotNull(dateHistogramAgg);
        Assert.Equal(3, dateHistogramAgg.Buckets.Count);
        oldestDate = DateTime.SpecifyKind(utcToday.UtcDateTime.Date.SubtractDays(2).SubtractHours(1), DateTimeKind.Unspecified);
        foreach (var bucket in dateHistogramAgg.Buckets)
        {
            AssertEqual(oldestDate, bucket.Date);
            Assert.Equal(1, bucket.Total);
            oldestDate = oldestDate.AddDays(1);
        }
    }

    [Fact]
    public async Task GetDateValueAggregatesAsync()
    {
        var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        await _employeeRepository.AddAsync(new List<Employee> {
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(2)),
            EmployeeGenerator.Generate(nextReview: utcToday.SubtractDays(1)),
            EmployeeGenerator.Generate(nextReview: utcToday)
        }, o => o.ImmediateConsistency());

        const string aggregations = "min:nextReview";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(3, result.Total);
        Assert.Single(result.Aggregations);

        var dateTermsAgg = result.Aggregations.Min<DateTime>("min_nextReview");
        Assert.NotNull(dateTermsAgg);
        Assert.Equal(utcToday.SubtractDays(2), dateTermsAgg.Value);

        string json = JsonConvert.SerializeObject(result);
        var roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
        Assert.NotNull(roundTripped);

        dateTermsAgg = roundTripped.Aggregations.Min<DateTime>("min_nextReview");
        Assert.NotNull(dateTermsAgg);
        Assert.Equal(utcToday.SubtractDays(2), dateTermsAgg.Value);
    }

    [Fact]
    public async Task GetTermAggregationsWithTopHitsAsync()
    {
        await CreateDataAsync();

        const string aggregations = "terms:(age tophits:_)";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(10, result.Total);
        Assert.Single(result.Aggregations);
        var termsAge = result.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(termsAge);
        Assert.Equal(10, termsAge.Buckets.Count);
        var bucket = termsAge.Buckets.First(f => f.Key == 19);
        Assert.Equal(1, bucket.Total);

        var tophits = bucket.Aggregations.TopHits();
        Assert.NotNull(tophits);
        var employees = tophits.Documents<Employee>();
        Assert.Single(employees);
        Assert.Equal(19, employees.First().Age);
        Assert.Equal(1, employees.First().YearsEmployed);

        string json = JsonConvert.SerializeObject(result);
        var roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
        Assert.NotNull(roundTripped);
        Assert.Equal(10, roundTripped.Total);
        Assert.Single(roundTripped.Aggregations);
        var roundTrippedTermsAge = roundTripped.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(roundTrippedTermsAge);
        Assert.Equal(10, roundTrippedTermsAge.Buckets.Count);
        bucket = roundTrippedTermsAge.Buckets.First(f => f.Key == 19);
        Assert.Equal(1, bucket.Total);

        string systemTextJson = System.Text.Json.JsonSerializer.Serialize(result);
        Assert.True(System.Text.Json.Nodes.JsonNode.DeepEquals(
            System.Text.Json.Nodes.JsonNode.Parse(json),
            System.Text.Json.Nodes.JsonNode.Parse(systemTextJson)),
            "Newtonsoft and System.Text.Json serialization should produce semantically equivalent JSON");
        roundTripped = System.Text.Json.JsonSerializer.Deserialize<CountResult>(systemTextJson);
        Assert.NotNull(roundTripped);
        Assert.Equal(10, roundTripped.Total);
        Assert.Single(roundTripped.Aggregations);
        var sysTermsAge = roundTripped.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(sysTermsAge);
        Assert.Equal(10, sysTermsAge.Buckets.Count);
        bucket = sysTermsAge.Buckets.First(f => f.Key == 19);
        Assert.Equal(1, bucket.Total);

        tophits = bucket.Aggregations.TopHits();
        Assert.NotNull(tophits);
        employees = tophits.Documents<Employee>(_serializer);
        Assert.Single(employees);
        Assert.Equal(19, employees.First().Age);
        Assert.Equal(1, employees.First().YearsEmployed);
    }

    [Fact]
    public void CanDeserializeHit()
    {
        string json = @"
            {
                ""_index"" : ""employees"",
                ""_type"" : ""_doc"",
                ""_id"" : ""53cc5800d3e0d1fed81452fd"",
                ""_score"" : 0.0,
                ""_source"" : {
                    ""id"" : ""53cc5800d3e0d1fed81452fd"",
                    ""companyId"" : ""62d982efd3e0d1fed81452f3"",
                    ""companyName"" : null,
                    ""unmappedCompanyName"" : null,
                    ""name"" : null,
                    ""emailAddress"" : null,
                    ""unmappedEmailAddress"" : null,
                    ""age"" : 45,
                    ""unmappedAge"" : 45,
                    ""location"" : ""20,20"",
                    ""yearsEmployed"" : 8,
                    ""lastReview"" : ""0001-01-01T00:00:00"",
                    ""nextReview"" : ""0001-01-01T00:00:00+00:00"",
                    ""createdUtc"" : ""2014-07-21T00:00:00Z"",
                    ""updatedUtc"" : ""2022-07-21T16:46:39.6914481Z"",
                    ""version"" : null,
                    ""isDeleted"" : false,
                    ""peerReviews"" : null,
                    ""phoneNumbers"" : [ ],
                    ""data"" : { }
                }
            }";

        using var hitStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        var employeeHit = _configuration.Client.ElasticsearchClientSettings.RequestResponseSerializer.Deserialize<Hit<Employee>>(hitStream);
        Assert.Equal("employees", employeeHit.Index);
        Assert.NotNull(employeeHit.Source);
        Assert.Equal("62d982efd3e0d1fed81452f3", employeeHit.Source.CompanyId);
    }

    [Fact]
    public async Task CanGetDupesAsync()
    {
        await CreateDataAsync();
        await _employeeRepository.AddAsync(EmployeeGenerator.Generate(age: 45, yearsEmployed: 12, location: "15,20", createdUtc: DateTime.UtcNow.Date.SubtractYears(12), updatedUtc: DateTime.UtcNow.Date.SubtractYears(12)), o => o.ImmediateConsistency());

        const string aggregations = "terms:(age^2 tophits:(_~100))";
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));
        Assert.Equal(11, result.Total);
        Assert.Single(result.Aggregations);
        var termsAge = result.Aggregations.Terms<int>("terms_age");
        Assert.NotNull(termsAge);
        Assert.Single(termsAge.Buckets);
        var bucket = termsAge.Buckets.First(f => f.Key == 45);
        Assert.Equal(2, bucket.Total);

        var tophits = bucket.Aggregations.TopHits();
        Assert.NotNull(tophits);
        var employees = tophits.Documents<Employee>();
        Assert.Equal(2, employees.Count);
        Assert.Equal(45, employees.First().Age);
        Assert.Equal(8, employees.First().YearsEmployed);
    }

    private Task CreateDataAsync()
    {
        var utcToday = DateTime.UtcNow.Date;
        return _employeeRepository.AddAsync(new List<Employee> {
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
