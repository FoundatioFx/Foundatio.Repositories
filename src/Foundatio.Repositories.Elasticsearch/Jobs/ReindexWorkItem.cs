using System;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

public record ReindexWorkItem
{
    public required string OldIndex { get; set; }
    public required string NewIndex { get; set; }
    public string? Alias { get; set; }
    public string? Script { get; set; }
    public bool DeleteOld { get; set; }
    public string? TimestampField { get; set; }
    public DateTime? StartUtc { get; set; }
}
