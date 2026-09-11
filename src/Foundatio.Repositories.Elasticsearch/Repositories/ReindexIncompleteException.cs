using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Thrown when a reindex did not copy every document, so the destination index cannot be trusted to be a
/// complete replica of the source.
/// </summary>
/// <remarks>
/// This exists so an incomplete reindex is impossible to mistake for a successful one. A caller that
/// swallows it - a startup migration, a queued handler, an operator script - is choosing to run against a
/// destination that may be missing documents.
/// <para>
/// The old index is always left in place when this is thrown, so nothing is lost and the operation can be
/// retried. A retry recopies the source from the beginning; because reindex writes by document id, that
/// converges on a complete replica rather than duplicating. A retry cannot help when the cause is
/// deterministic, such as documents the destination's mapping rejects.
/// </para>
/// <para>
/// When this is thrown after the alias cutover - from the catch-up pass, rather than the first copy - the
/// destination is already serving reads, and it may be short. Check <see cref="NewIndex"/> before assuming
/// the old index is still the one being queried.
/// </para>
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
