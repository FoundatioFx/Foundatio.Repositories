using System;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

public class ReindexWorkItem
{
    public string OldIndex { get; set; } = null!;
    public string NewIndex { get; set; } = null!;
    public string? Alias { get; set; }
    public string? Script { get; set; }
    public bool DeleteOld { get; set; }
    public string? TimestampField { get; set; }
    public DateTime? StartUtc { get; set; }
}
