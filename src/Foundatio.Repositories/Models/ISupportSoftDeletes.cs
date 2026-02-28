namespace Foundatio.Repositories.Models;

/// <summary>
/// Indicates that a document supports soft delete functionality.
/// </summary>
/// <remarks>
/// <para>
/// When a document type implements this interface, repository read operations automatically filter out
/// documents where <see cref="IsDeleted"/> is <c>true</c>. Use <c>options.IncludeSoftDeletes()</c> to
/// include soft-deleted documents in query results.
/// </para>
/// <para>
/// To soft delete a document, set <see cref="IsDeleted"/> to <c>true</c> and save it. The repository
/// will publish a <see cref="ChangeType.Removed"/> notification when <c>IsDeleted</c> changes from
/// <c>false</c> to <c>true</c>.
/// </para>
/// </remarks>
public interface ISupportSoftDeletes
{
    /// <summary>
    /// Gets or sets whether this document has been soft deleted.
    /// </summary>
    bool IsDeleted { get; set; }
}
