using System;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Thrown when the outcome of a compatibility reindex task submission or cancellation could not be confirmed
/// (the response was lost or the task ID could not be verified). Callers must retain the source write block and
/// destination index and inspect matching Elasticsearch tasks before retrying.
/// </summary>
internal sealed class ElasticReindexTaskUncertainException : RepositoryException
{
    public ElasticReindexTaskUncertainException(string message, Exception innerException) : base(message, innerException) { }
}
