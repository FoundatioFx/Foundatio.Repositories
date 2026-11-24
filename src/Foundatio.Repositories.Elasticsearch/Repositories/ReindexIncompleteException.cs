using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>A copy, verification, or completion-record step did not satisfy the migration contract.</summary>
/// <remarks>
/// Source cleanup is not performed for an incomplete migration. This does not prove the alias stayed on the
/// source: a response can be lost after cutover, or recording completion can fail after promotion in either
/// ordering. Inspect aliases and durable safety state before retrying; never recopy blindly into live traffic.
/// </remarks>
public class ReindexIncompleteException : RepositoryException
{
    public ReindexIncompleteException(string oldIndex, string newIndex, string reason)
        : base($"Reindex of {oldIndex} -> {newIndex} did not complete: {reason}")
    {
        OldIndex = oldIndex;
        NewIndex = newIndex;
        Reason = reason;
    }

    /// <summary>Index documents were being copied from.</summary>
    public string OldIndex { get; }

    /// <summary>Index documents were being copied to. May be missing documents.</summary>
    public string NewIndex { get; }

    /// <summary>Why the copy did not complete.</summary>
    public string Reason { get; }
}
