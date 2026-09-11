using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Thrown when a migration's destination has already been promoted but there is no trustworthy evidence that
/// the migration ever completed, so the outcome is unknown and needs a decision rather than a retry.
/// </summary>
/// <remarks>
/// <para>
/// This is deliberately neither a success nor permission to copy again. The alias already points at the
/// destination, so reporting success would record a possibly short index as a finished migration, and copying
/// again would write into an index that is already serving live traffic. Both indexes are left exactly as they
/// were found: nothing is replayed, rolled back, or deleted.
/// </para>
/// <para>
/// The usual cause is a first attempt that failed after the alias cutover - the alias switch happens before
/// the catch-up pass, so a post-cutover failure advances the version without finishing the work. It also
/// occurs for migrations completed before this library recorded completion evidence, because completion is
/// never inferred from alias state or document counts.
/// </para>
/// <para>
/// Recovery is a human decision. Compare <see cref="SourceIndex"/> against <see cref="DestinationIndex"/> to
/// establish whether the destination is actually short, then either accept it, or migrate to a fresh index
/// version. When the source has already been deleted, the destination is all that remains.
/// </para>
/// </remarks>
public class ReindexCompletionUnknownException : RepositoryException
{
    public ReindexCompletionUnknownException(string alias, string sourceIndex, string destinationIndex, string reason)
        : base($"Reindex of {sourceIndex} -> {destinationIndex} for alias {alias} cannot be confirmed complete: {reason}. The destination is already promoted, so this is not being retried or replayed; both indexes were left in place for inspection.")
    {
        Alias = alias;
        SourceIndex = sourceIndex;
        DestinationIndex = destinationIndex;
        Reason = reason;
    }

    /// <summary>Alias whose migration could not be confirmed.</summary>
    public string Alias { get; }

    /// <summary>Index documents were being copied from. May already have been deleted.</summary>
    public string SourceIndex { get; }

    /// <summary>Index documents were being copied to, and which the alias already points at.</summary>
    public string DestinationIndex { get; }

    /// <summary>Why completion could not be confirmed.</summary>
    public string Reason { get; }
}
