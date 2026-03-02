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

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class NestedFieldTests : ElasticRepositoryTestBase
{
    private readonly IEmployeeRepository _employeeRepository;

    public NestedFieldTests(ITestOutputHelper output) : base(output)
    {
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task FindAsync_WithNestedPeerReviewOrCondition_ReturnsMatchingEmployees()
    {
        // Arrange
        List<Employee> employees =
        [
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

        // Act
        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("peerReviews.rating:>=4 OR peerReviews.reviewerEmployeeId:bob_456"));

        // Assert
        Assert.Equal(2, results.Documents.Count);
        Assert.Contains(results.Documents, e => String.Equals(e.Name, "Alice"));
        Assert.Contains(results.Documents, e => String.Equals(e.Name, "Bob"));
    }

    [Fact]
    public async Task CountAsync_WithNestedPeerReviewAggregation_ReturnsAggregationData()
    {
        // Arrange
        List<Employee> employees =
        [
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

        // Act
        var results = await _employeeRepository.FindAsync(q => q.SearchExpression(specialReviewerId));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal("Alice", results.Documents.Single().Name);
    }

    [Fact]
    public async Task SearchAsync_WithNestedAggregations_ReturnsCorrectBuckets()
    {
        // Arrange
        var baseDate = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("employee1", nextReview: baseDate.SubtractDays(2), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 4 }
            ]),
            EmployeeGenerator.Generate("employee2", nextReview: baseDate.SubtractDays(1), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 5 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var nestedAggQuery = _client.Search<Employee>(d => d.Indices("employees").Aggregations(a => a
           .Add("nested_reviewRating", agg => agg
               .Nested(h => h.Path("peerReviews"))
               .Aggregations(a1 => a1.Add("terms_rating", t => t.Terms(t1 => t1.Field("peerReviews.rating")).Meta(m => m.Add("@field_type", "integer")))))
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
        var nestedAggQueryWithFilter = _client.Search<Employee>(d => d.Indices("employees").Aggregations(a => a
           .Add("nested_reviewRating", agg => agg
               .Nested(h => h.Path("peerReviews"))
                .Aggregations(a1 => a1
                    .Add($"user_{employees[0].Id}", f => f
                        .Filter(q => q.Term(t => t.Field("peerReviews.reviewerEmployeeId").Value(employees[0].Id)))
                        .Aggregations(a2 => a2.Add("terms_rating", t => t.Terms(t1 => t1.Field("peerReviews.rating")).Meta(m => m.Add("@field_type", "integer")))))
            ))));

        // Assert - Verify filtered aggregation
        result = nestedAggQueryWithFilter.Aggregations.ToAggregations();
        Assert.Single(result);

        var nestedReviewRatingFilteredAgg = Assert.IsType<SingleBucketAggregate>(result["nested_reviewRating"]);

        var userFilteredAgg = Assert.IsType<SingleBucketAggregate>(nestedReviewRatingFilteredAgg.Aggregations[$"user_{employees[0].Id}"]);
        Assert.Single(userFilteredAgg.Aggregations.Terms("terms_rating").Buckets);
        Assert.Equal("5", userFilteredAgg.Aggregations.Terms("terms_rating").Buckets.First().Key);
        Assert.Equal(1, userFilteredAgg.Aggregations.Terms("terms_rating").Buckets.First().Total);
    }

    [Fact]
    public async Task CountAsync_WithNestedLuceneBasedAggregations_ReturnsCorrectMetrics()
    {
        // Arrange
        var baseDate = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("employee1", nextReview: baseDate.SubtractDays(2), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("employee2", nextReview: baseDate.SubtractDays(1), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 5 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 3 }
            ]),
            EmployeeGenerator.Generate("employee3", nextReview: baseDate.SubtractDays(3), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 5 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());
        const string aggregations = "terms:peerReviews.reviewerEmployeeId terms:peerReviews.rating max:peerReviews.rating min:peerReviews.rating cardinality:peerReviews.reviewerEmployeeId";

        // Act
        var result = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregations));

        // Assert
        Assert.Equal(3, result.Total);
        Assert.Single(result.Aggregations);

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
    public async Task CountAsync_WithNestedAggregationsIncludeFiltering_ReturnsFilteredResults()
    {
        // Arrange
        var baseDate = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("employee1", nextReview: baseDate.SubtractDays(2), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("employee2", nextReview: baseDate.SubtractDays(1), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 5 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 3 }
            ]),
            EmployeeGenerator.Generate("employee3", nextReview: baseDate.SubtractDays(3), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 5 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        const string aggregationsWithInclude = "terms:(peerReviews.reviewerEmployeeId @include:employee1 @include:employee2) terms:(peerReviews.rating @include:4 @include:5) max:peerReviews.rating min:peerReviews.rating";
        var resultWithInclude = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregationsWithInclude));

        // Assert
        Assert.Equal(3, resultWithInclude.Total);
        Assert.Single(resultWithInclude.Aggregations);

        var nestedPeerReviewsAggWithInclude = resultWithInclude.Aggregations["nested_peerReviews"] as SingleBucketAggregate;
        Assert.NotNull(nestedPeerReviewsAggWithInclude);

        var reviewerTermsAggWithInclude = nestedPeerReviewsAggWithInclude.Aggregations.Terms<string>("terms_peerReviews.reviewerEmployeeId");
        Assert.Equal(2, reviewerTermsAggWithInclude.Buckets.Count); // Only employee1 and employee2 should be included
        Assert.Contains(reviewerTermsAggWithInclude.Buckets, b => String.Equals(b.Key, "employee1"));
        Assert.Contains(reviewerTermsAggWithInclude.Buckets, b => String.Equals(b.Key, "employee2"));
        Assert.DoesNotContain(reviewerTermsAggWithInclude.Buckets, b => String.Equals(b.Key, "employee3"));

        var ratingTermsAggWithInclude = nestedPeerReviewsAggWithInclude.Aggregations.Terms<int>("terms_peerReviews.rating");
        Assert.Equal(2, ratingTermsAggWithInclude.Buckets.Count); // Only ratings 4 and 5 should be included
        Assert.Contains(ratingTermsAggWithInclude.Buckets, b => b.Key == 4);
        Assert.Contains(ratingTermsAggWithInclude.Buckets, b => b.Key == 5);
        Assert.DoesNotContain(ratingTermsAggWithInclude.Buckets, b => b.Key == 3);
    }

    [Fact]
    public async Task CountAsync_WithNestedAggregationsExcludeFiltering_ReturnsFilteredResults()
    {
        // Arrange
        var baseDate = new DateTimeOffset(DateTime.UtcNow.Year, 1, 1, 12, 0, 0, TimeSpan.FromHours(5));
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("employee1", nextReview: baseDate.SubtractDays(2), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("employee2", nextReview: baseDate.SubtractDays(1), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 5 },
                new PeerReview { ReviewerEmployeeId = "employee3", Rating = 3 }
            ]),
            EmployeeGenerator.Generate("employee3", nextReview: baseDate.SubtractDays(3), peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "employee1", Rating = 4 },
                new PeerReview { ReviewerEmployeeId = "employee2", Rating = 5 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        const string aggregationsWithExclude = "terms:(peerReviews.reviewerEmployeeId @exclude:employee3) terms:(peerReviews.rating @exclude:3) max:peerReviews.rating min:peerReviews.rating";
        var resultWithExclude = await _employeeRepository.CountAsync(q => q.AggregationsExpression(aggregationsWithExclude));

        // Assert
        Assert.Equal(3, resultWithExclude.Total);
        Assert.Single(resultWithExclude.Aggregations);

        var nestedPeerReviewsAggWithExclude = resultWithExclude.Aggregations["nested_peerReviews"] as SingleBucketAggregate;
        Assert.NotNull(nestedPeerReviewsAggWithExclude);

        var reviewerTermsAggWithExclude = nestedPeerReviewsAggWithExclude.Aggregations.Terms<string>("terms_peerReviews.reviewerEmployeeId");
        Assert.Equal(2, reviewerTermsAggWithExclude.Buckets.Count); // employee3 should be excluded
        Assert.Contains(reviewerTermsAggWithExclude.Buckets, b => String.Equals(b.Key, "employee1"));
        Assert.Contains(reviewerTermsAggWithExclude.Buckets, b => String.Equals(b.Key, "employee2"));
        Assert.DoesNotContain(reviewerTermsAggWithExclude.Buckets, b => String.Equals(b.Key, "employee3"));

        var ratingTermsAggWithExclude = nestedPeerReviewsAggWithExclude.Aggregations.Terms<int>("terms_peerReviews.rating");
        Assert.Equal(2, ratingTermsAggWithExclude.Buckets.Count); // rating 3 should be excluded
        Assert.Contains(ratingTermsAggWithExclude.Buckets, b => b.Key == 4);
        Assert.Contains(ratingTermsAggWithExclude.Buckets, b => b.Key == 5);
        Assert.DoesNotContain(ratingTermsAggWithExclude.Buckets, b => b.Key == 3);
    }

    [Fact]
    public async Task CountAsync_WithNestedAggregationsSerialization_CanRoundtripBothSerializers()
    {
        // Arrange
        List<Employee> employees =
        [
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
        Assert.NotNull(roundTrippedRatingTermsAgg);
        Assert.Equal(4, roundTrippedRatingTermsAgg.Buckets.Count);
        bucket = roundTrippedRatingTermsAgg.Buckets.First(f => f.Key == 5);
        Assert.Equal(2, bucket.Total);

        // Test System.Text.Json serialization
        string systemTextJson = System.Text.Json.JsonSerializer.Serialize(result);
        Assert.True(System.Text.Json.Nodes.JsonNode.DeepEquals(
            System.Text.Json.Nodes.JsonNode.Parse(json),
            System.Text.Json.Nodes.JsonNode.Parse(systemTextJson)),
            "Newtonsoft and System.Text.Json serialization should produce semantically equivalent JSON");
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

    [Fact]
    public async Task FindAsync_WithIndividualNestedFieldWithoutGroupSyntax_ReturnsMatchingEmployee()
    {
        // Arrange
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 3 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("peerReviews.rating:5"));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal("Alice", results.Documents.Single().Name);
    }

    [Fact]
    public async Task FindAsync_WithNestedRangeQuery_ReturnsMatchingEmployees()
    {
        // Arrange
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 2 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 4 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("peerReviews.rating:[4 TO 5]"));

        // Assert
        Assert.Equal(2, results.Documents.Count);
        Assert.Contains(results.Documents, e => String.Equals(e.Name, "Alice"));
        Assert.Contains(results.Documents, e => String.Equals(e.Name, "Charlie"));
    }

    [Fact]
    public async Task FindAsync_WithNestedAndConditionWithoutGroupSyntax_ReturnsMatchingEmployee()
    {
        // Arrange
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 3 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("peerReviews.rating:5 AND peerReviews.reviewerEmployeeId:bob_456"));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal("Alice", results.Documents.Single().Name);
    }

    [Fact]
    public async Task FindAsync_WithMixedNestedAndNonNestedFields_ReturnsMatchingEmployee()
    {
        // Arrange
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 3 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("name:Alice peerReviews.rating:5"));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal("Alice", results.Documents.Single().Name);
    }

    [Fact]
    public async Task FindAsync_WithNegatedNestedGroup_ExcludesMatchingEmployees()
    {
        // Arrange
        List<Employee> employees =
        [
            EmployeeGenerator.Generate("alice_123", "Alice", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "bob_456", Rating = 5 }
            ]),
            EmployeeGenerator.Generate("bob_456", "Bob", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 3 }
            ]),
            EmployeeGenerator.Generate("charlie_789", "Charlie", peerReviews:
            [
                new PeerReview { ReviewerEmployeeId = "alice_123", Rating = 4 }
            ])
        ];

        await _employeeRepository.AddAsync(employees, o => o.ImmediateConsistency());

        // Act
        var results = await _employeeRepository.FindAsync(q => q.FilterExpression("NOT peerReviews.rating:5"));

        // Assert
        Assert.Equal(2, results.Documents.Count);
        Assert.Contains(results.Documents, e => String.Equals(e.Name, "Bob"));
        Assert.Contains(results.Documents, e => String.Equals(e.Name, "Charlie"));
    }
}
