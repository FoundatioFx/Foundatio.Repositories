using System;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Thrown when an Elasticsearch response for a compatibility-upgrade write operation (write block, alias marker,
/// or index creation) is lost or invalid, so whether the operation applied on the server cannot be determined.
/// Callers must retain both physical indexes and inspect recovery evidence before retrying.
/// </summary>
internal sealed class ElasticCompatibilityOperationUncertainException : RepositoryException
{
    public ElasticCompatibilityOperationUncertainException(string message, Exception innerException) : base(message, innerException) { }
}
