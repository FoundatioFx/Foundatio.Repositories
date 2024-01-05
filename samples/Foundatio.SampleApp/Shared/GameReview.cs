using Foundatio.Repositories.Models;

namespace Foundatio.SampleApp.Shared;

public class GameReview : IIdentity, IHaveDates
{
    public string Id { get; set; } = String.Empty;
    public string Name { get; set; } = String.Empty;
    public string? Description { get; set; }
    public string Category { get; set; } = "General";
    public ICollection<string> Tags { get; set; } = new List<string>();
    public DateTime UpdatedUtc { get; set; }
    public DateTime CreatedUtc { get; set; }
}

public class GameReviewSearchResult
{
    public IReadOnlyCollection<GameReview> Reviews { get; set; } = new List<GameReview>();
    public long Total { get; set; }
    public ICollection<AggCount> CategoryCounts { get; set; } = new List<AggCount>();
    public ICollection<AggCount> TagCounts { get; set; } = new List<AggCount>();

    public static GameReviewSearchResult From(FindResults<GameReview> results)
    {
        var categoryCounts = results.Aggregations.Terms("terms_category")?.Buckets.Select(t => new AggCount { Name = t.Key, Total = t.Total }).ToList() ?? new List<AggCount>();
        var tagCounts = results.Aggregations.Terms("terms_tags")?.Buckets.Select(t => new AggCount { Name = t.Key, Total = t.Total }).ToList() ?? new List<AggCount>();

        return new GameReviewSearchResult
        {
            Reviews = results.Documents,
            Total = results.Total,
            CategoryCounts = categoryCounts,
            TagCounts = tagCounts
        };
    }
}

public class AggCount
{
    public string Name { get; set; } = String.Empty;
    public long? Total { get; set; }
}
