namespace Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;

/// <summary>Provisioning marker identifying a cluster as disposable for this test suite.</summary>
public sealed record DisposableClusterMarker
{
    /// <summary>Owner of the cluster. Must match <see cref="DisposableClusterGuard.ExpectedPurpose"/>.</summary>
    public string Purpose { get; init; } = null!;

    /// <summary>When the marker was written, for operator diagnostics only.</summary>
    public string? CreatedUtc { get; init; }
}
