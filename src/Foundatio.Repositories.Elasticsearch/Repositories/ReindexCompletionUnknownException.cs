using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Completion, task termination, or write-block ownership cannot be established safely.</summary>
/// <remarks>
/// This is neither success nor permission to replay. Inspect the named physical indexes, aliases, and durable
/// safety records. A current QuiesceSource flag cannot establish the earlier attempt's provenance. Unknown
/// dispatch or block ownership can require inspection even before alias promotion. No rollback is implied.
/// </remarks>
public class ReindexCompletionUnknownException : RepositoryException
{
    public ReindexCompletionUnknownException(string alias, string sourceIndex, string destinationIndex, string reason)
        : base($"Reindex of {sourceIndex} -> {destinationIndex} for alias {alias} cannot be confirmed complete: {reason}. Automatic replay is unsafe; inspect the physical indexes, aliases, and durable safety records before recovery.")
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

    /// <summary>Intended physical destination; inspect aliases to determine whether it was promoted.</summary>
    public string DestinationIndex { get; }

    /// <summary>Why completion could not be confirmed.</summary>
    public string Reason { get; }
}
