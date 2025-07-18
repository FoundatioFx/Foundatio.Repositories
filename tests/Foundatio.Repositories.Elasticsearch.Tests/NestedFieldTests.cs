using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class NestedFieldTests : ElasticRepositoryTestBase
{
    private readonly IEmployeeRepository _employeeRepository;

    public NestedFieldTests(ITestOutputHelper output) : base(output)
    {
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task FindAsync_WithNestedPeerReviewOrCondition_ReturnsMatchingEmployees()
    {
        // Arrange
        List<Employee> employees = [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 4 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 2 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var searchRepository = (ISearchableReadOnlyRepository<Employee>)_employeeRepository;

        // Act
        var results = await searchRepository.FindAsync(q => q.FilterExpression("peerReviews.rating:>=4 OR peerReviews.reviewerEmployeeId:bob_456"));

        // Assert
        Assert.Equal(2, results.Documents.Count);
        Assert.Contains(results.Documents, e => e.Name == "Alice");
        Assert.Contains(results.Documents, e => e.Name == "Bob");
    }

    [Fact]
    public async Task CountAsync_WithNestedPeerReviewAggregation_ReturnsAggregationData()
    {
        // Arrange
        List<Employee> employees = [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 },
                new PeerReview { ReviewerEmployeeId = "charlie_789", Rating = 4 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 3 },
                new PeerReview { ReviewerEmployeeId = "charlie_789", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 2 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression("terms:peerReviews.rating"));

        // Assert
        Assert.Equal(3, result.Total);
        Assert.Single(result.Aggregations);
        Assert.NotEmpty(result.Aggregations);

        var nestedPeerReviewsAgg = result.Aggregations["nested_peerReviews"] as SingleBucketAggregate;
        Assert.NotNull(nestedPeerReviewsAgg);
        Assert.NotEmpty(nestedPeerReviewsAgg.Aggregations);
    }

    [Fact]
    public async Task FindAsync_WithNestedFieldInDefaultSearch_ReturnsMatchingEmployee()
    {
        // Arrange
        const string specialReviewerId = "special_reviewer_123";
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = specialReviewerId, Rating = 5 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 4 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 3 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        var searchRepository = (ISearchableReadOnlyRepository<Employee>)_employeeRepository;

        // Act
        var results = await searchRepository.FindAsync(q => q.SearchExpression(specialReviewerId));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal("Alice", results.Documents.Single().Name);
    }

    [Fact]
    public async Task SearchAsync_WithNestedAggregations_ReturnsCorrectBuckets()
    {
        // Arrange
        var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        List<Employee> employees = [
            EmployeeGenerator.Generate("employee1", nextReview: utcToday.SubtractDays(2), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 4 }
            ]),
            EmployeeGenerator.Generate("employee2", nextReview: utcToday.SubtractDays(1), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 5 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var nestedAggQuery = _client.Search<Employee>(d => d.Index("employees").Aggregations(a => a
           .Nested("nested_reviewRating", h => h.Path("peerReviews")
               .Aggregations(a1 => a1.Terms("terms_rating", t => t.Field("peerReviews.rating").Meta(m => m.Add("@field_type", "integer")))))
            ));

        // Assert
        var result = nestedAggQuery.Aggregations.ToAggregations();
        Assert.Single(result);

        var nestedReviewRatingAgg = result["nested_reviewRating"] as SingleBucketAggregate;
        Assert.NotNull(nestedReviewRatingAgg);

        var termsRatingAgg = nestedReviewRatingAgg.Aggregations["terms_rating"] as BucketAggregate;
        Assert.NotNull(termsRatingAgg);
        Assert.Equal(2, termsRatingAgg.Items.Count);

        // Act - Test nested aggregation with filter
        var nestedAggQueryWithFilter = _client.Search<Employee>(d => d.Index("employees").Aggregations(a => a
           .Nested("nested_reviewRating", h => h.Path("peerReviews")
                .Aggregations(a1 => a1
                    .Filter("user_" + employees[0].Id, f => f.Filter(q => q.Term(t => t.Field("peerReviews.reviewerEmployeeId").Value(employees[0].Id)))
                        .Aggregations(a2 => a2.Terms("terms_rating", t => t.Field("peerReviews.rating").Meta(m => m.Add("@field_type", "integer")))))
            ))));

        // Assert - Verify filtered aggregation
        result = nestedAggQueryWithFilter.Aggregations.ToAggregations();
        Assert.Single(result);

        var nestedReviewRatingFilteredAgg = result["nested_reviewRating"] as SingleBucketAggregate;
        Assert.NotNull(nestedReviewRatingFilteredAgg);

        var userFilteredAgg = nestedReviewRatingFilteredAgg.Aggregations["user_" + employees[0].Id] as SingleBucketAggregate;
        Assert.NotNull(userFilteredAgg);
        Assert.Single(userFilteredAgg.Aggregations.Terms("terms_rating").Buckets);
        Assert.Equal("5", userFilteredAgg.Aggregations.Terms("terms_rating").Buckets.First().Key);
        Assert.Equal(1, userFilteredAgg.Aggregations.Terms("terms_rating").Buckets.First().Total);
    }

    [Fact]
    public async Task CountAsync_WithNestedLuceneBasedAggregations_ReturnsCorrectMetrics()
    {
        // Arrange
        var utcToday = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("employee1", nextReview: utcToday.SubtractDays(2), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("employee2", nextReview: utcToday.SubtractDays(1), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 5 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 3 }
            ]),
            EmployeeGenerator.Generate("employee3", nextReview: utcToday.SubtractDays(3), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 5 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        const string aggregations = "terms:(peerReviews.reviewerEmployeeId peerReviews.rating) max:peerReviews.rating min:peerReviews.rating cardinality:peerReviews.reviewerEmployeeId";

        // Act
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));

        // Assert
        Assert.Equal(3, result.Total);
        Assert.Equal(2, result.Aggregations.Count);

        var nestedPeerReviewsAgg = result.Aggregations["nested_peerReviews"] as SingleBucketAggregate;
        Assert.NotNull(nestedPeerReviewsAgg);

        var reviewerTermsAgg = nestedPeerReviewsAgg.Aggregations.Terms<string>("terms_peerReviews.reviewerEmployeeId");
        Assert.Equal(3, reviewerTermsAgg.Buckets.Count);

        var ratingTermsAgg = nestedPeerReviewsAgg.Aggregations.Terms<int>("terms_peerReviews.rating");
        Assert.Equal(3, ratingTermsAgg.Buckets.Count);

        Assert.Equal(3, nestedPeerReviewsAgg.Aggregations.Min("min_peerReviews.rating").Value);
        Assert.Equal(5, nestedPeerReviewsAgg.Aggregations.Max("max_peerReviews.rating").Value);
        Assert.Equal(3, nestedPeerReviewsAgg.Aggregations.Cardinality("cardinality_peerReviews.reviewerEmployeeId").Value);
    }

    [Fact]
    public async Task CountAsync_WithNestedAggregationsSerialization_CanRoundtripBothSerializers()
    {
        // Arrange
        List<Employee> employees = [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 },
                new PeerReview { ReviewerEmployeeId = "charlie_789", Rating = 4 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 3 },
                new PeerReview { ReviewerEmployeeId = "charlie_789", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 2 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression("terms:peerReviews.rating max:peerReviews.rating min:peerReviews.rating"));

        // Assert
        Assert.Equal(3, result.Total);
        Assert.Single(result.Aggregations);

        var nestedPeerReviewsAgg = result.Aggregations["nested_peerReviews"] as SingleBucketAggregate;
        Assert.NotNull(nestedPeerReviewsAgg);

        var ratingTermsAgg = nestedPeerReviewsAgg.Aggregations.Terms<int>("terms_peerReviews.rating");
        Assert.Equal(4, ratingTermsAgg.Buckets.Count);
        var bucket = ratingTermsAgg.Buckets.First(f => f.Key == 5);
        Assert.Equal(2, bucket.Total);

        // Test Newtonsoft.Json serialization
        string json = JsonConvert.SerializeObject(result);
        var roundTripped = JsonConvert.DeserializeObject<CountResult>(json);
        Assert.Equal(3, roundTripped.Total);
        Assert.Single(roundTripped.Aggregations);

        var roundTrippedNestedAgg = roundTripped.Aggregations["nested_peerReviews"] as SingleBucketAggregate;
        Assert.NotNull(roundTrippedNestedAgg);

        var roundTrippedRatingTermsAgg = roundTrippedNestedAgg.Aggregations.Terms<int>("terms_peerReviews.rating");
        Assert.Equal(4, roundTrippedRatingTermsAgg.Buckets.Count);
        bucket = roundTrippedRatingTermsAgg.Buckets.First(f => f.Key == 5);
        Assert.Equal(2, bucket.Total);

        // Test System.Text.Json serialization
        string systemTextJson = System.Text.Json.JsonSerializer.Serialize(result);
        Assert.Equal(json, systemTextJson);
        roundTripped = System.Text.Json.JsonSerializer.Deserialize<CountResult>(systemTextJson);
        Assert.Equal(3, roundTripped.Total);
        Assert.Single(roundTripped.Aggregations);

        roundTrippedNestedAgg = roundTripped.Aggregations["nested_peerReviews"] as SingleBucketAggregate;
        Assert.NotNull(roundTrippedNestedAgg);

        roundTrippedRatingTermsAgg = roundTrippedNestedAgg.Aggregations.Terms<int>("terms_peerReviews.rating");
        Assert.Equal(4, roundTrippedRatingTermsAgg.Buckets.Count);
        bucket = roundTrippedRatingTermsAgg.Buckets.First(f => f.Key == 5);
        Assert.Equal(2, bucket.Total);
    }
}
