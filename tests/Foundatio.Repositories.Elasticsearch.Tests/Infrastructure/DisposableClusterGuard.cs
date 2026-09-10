using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;

namespace Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;

/// <summary>
/// Refuses to run the integration suite unless it is pointed at a cluster that has been explicitly
/// provisioned as disposable for these tests.
/// </summary>
/// <remarks>
/// <para>
/// This exists because ordinary test setup is destructive before any test body runs:
/// <c>ElasticRepositoryTestBase.RemoveDataAsync</c> calls <c>DeleteIndexesAsync()</c> and
/// <c>DeleteWildcardIndicesAsync("employee*")</c>, and the bundled compose file sets
/// <c>action.destructive_requires_name: false</c>. The default connection is
/// <c>elastic.localtest.me:9200</c>, which resolves to <c>127.0.0.1</c> - so a developer running an
/// unrelated cluster on port 9200 would have its indexes deleted by merely running the tests.
/// </para>
/// <para>
/// Deliberately insufficient as evidence, because none of these distinguish a disposable cluster from
/// an unrelated one on the same host: a recognizable hostname, a loopback address, a distinctive port,
/// or <c>ELASTICSEARCH_URL</c> simply being set. Validation instead relies on a provisioning marker
/// stored in the cluster itself, and <b>fails closed</b> when that marker is unreadable or belongs to
/// someone else.
/// </para>
/// <para>
/// An unmarked but <em>empty</em> cluster is adopted automatically, because an empty cluster cannot be
/// someone's populated environment. That is what lets a fresh CI container work with no configuration.
/// An unmarked cluster that already holds indexes is refused unless the operator explicitly overrides
/// via <see cref="OptInVariable"/>, since that is the case that actually destroys data.
/// </para>
/// </remarks>
public static class DisposableClusterGuard
{
    /// <summary>Index holding the provisioning marker. Never touched by repository tests.</summary>
    public const string MarkerIndex = "foundatio-disposable-test-cluster";

    /// <summary>Document id of the provisioning marker.</summary>
    public const string MarkerId = "marker";

    /// <summary>The marker value this suite requires. A different value means a different owner.</summary>
    public const string ExpectedPurpose = "foundatio-repositories-integration-tests";

    /// <summary>
    /// Environment variable that overrides the refusal to adopt a cluster which already holds indexes.
    /// Not required for an empty cluster.
    /// </summary>
    public const string OptInVariable = "FOUNDATIO_TEST_CLUSTER";

    /// <summary>Required value of <see cref="OptInVariable"/>.</summary>
    public const string OptInValue = "disposable";

    private static readonly SemaphoreSlim _validationLock = new(1, 1);
    private static Task? _validation;

    /// <summary>
    /// Validates once per process, before anything destructive runs. Subsequent calls reuse the first
    /// result, including a failure - a suite that was refused once stays refused.
    /// </summary>
    public static Task EnsureValidatedAsync(ElasticsearchClient client, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        if (_validation is not null)
            return _validation;

        return EnsureValidatedSlowAsync(client, Environment.GetEnvironmentVariable(OptInVariable), cancellationToken);
    }

    private static async Task EnsureValidatedSlowAsync(ElasticsearchClient client, string? optIn, CancellationToken cancellationToken)
    {
        await _validationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _validation ??= ValidateAsync(client, optIn, cancellationToken);
        }
        finally
        {
            _validationLock.Release();
        }

        await _validation.ConfigureAwait(false);
    }

    /// <summary>
    /// Validates the target cluster, throwing <see cref="InvalidOperationException"/> when it must not be
    /// used. Issues only read requests, plus a single marker write on a cluster it is allowed to provision.
    /// Never issues a destructive request.
    /// </summary>
    /// <param name="client">Client pointed at the cluster to validate.</param>
    /// <param name="optIn">
    /// Value of <see cref="OptInVariable"/>. Passed in rather than read from the environment so this is a
    /// pure function of its arguments and tests need not mutate process-global state.
    /// </param>
    /// <param name="cancellationToken">Cancels the validation reads.</param>
    public static async Task ValidateAsync(ElasticsearchClient client, string? optIn, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        var marker = await client.GetAsync<DisposableClusterMarker>(new GetRequest(MarkerIndex, MarkerId), cancellationToken).ConfigureAwait(false);

        if (marker.IsValidResponse && marker.Found)
        {
            string? purpose = marker.Source?.Purpose;
            if (String.Equals(purpose, ExpectedPurpose, StringComparison.Ordinal))
                return;

            throw new InvalidOperationException(
                $"""
                Refusing to run destructive integration tests: the target cluster's disposable marker belongs to something else.

                Expected purpose : {ExpectedPurpose}
                Actual purpose   : {purpose ?? "(missing)"}
                Marker           : {MarkerIndex}/{MarkerId}

                This cluster is marked as owned by a different suite or environment, so it is not safe to delete indexes in it.
                Point the tests at a disposable cluster instead.
                """);
        }

        bool markerMissing = marker.ApiCallDetails?.HttpStatusCode is 404;
        if (!markerMissing)
        {
            throw new InvalidOperationException(
                $"""
                Refusing to run destructive integration tests: could not read the disposable-cluster marker, so the target cannot be verified.

                Marker : {MarkerIndex}/{MarkerId}
                Status : {marker.ApiCallDetails?.HttpStatusCode.ToString() ?? "(no response)"}
                Error  : {marker.ElasticsearchServerError?.Error?.Reason ?? marker.ApiCallDetails?.OriginalException?.Message ?? "unknown"}

                This check fails closed: an unreadable marker is treated as "not disposable", never as "probably fine".
                """);
        }

        await ProvisionAsync(client, optIn, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes the marker onto a cluster holding no user indexes. An empty cluster cannot be someone's
    /// populated environment, which is what makes self-provisioning safe and lets a fresh CI container
    /// work without configuration. Adopting a <em>populated</em> unmarked cluster is the mistake this
    /// guard exists to prevent, so it requires an explicit <see cref="OptInVariable"/> override.
    /// </summary>
    private static async Task ProvisionAsync(ElasticsearchClient client, string? optIn, CancellationToken cancellationToken)
    {
        var existing = await client.Indices.GetAsync(Indices.Parse("*"), d => d.IgnoreUnavailable(), cancellationToken).ConfigureAwait(false);
        if (!existing.IsValidResponse)
        {
            throw new InvalidOperationException(
                $"""
                Refusing to run destructive integration tests: the disposable marker is absent and the cluster's index list could not be read, so it cannot be confirmed empty.

                Status : {existing.ApiCallDetails?.HttpStatusCode.ToString() ?? "(no response)"}
                Error  : {existing.ElasticsearchServerError?.Error?.Reason ?? "unknown"}

                {BuildManualProvisioningHelp()}
                """);
        }

        var userIndexes = existing.Indices is null
            ? []
            : existing.Indices.Keys.Select(i => i.ToString()).Where(n => !String.IsNullOrEmpty(n) && !n.StartsWith('.')).ToList();

        bool operatorOverrode = String.Equals(optIn, OptInValue, StringComparison.OrdinalIgnoreCase);
        if (userIndexes.Count > 0 && !operatorOverrode)
        {
            throw new InvalidOperationException(
                $"""
                Refusing to run destructive integration tests: the target cluster has no disposable marker and already holds {userIndexes.Count} user index(es), so it may be a real environment.

                First few: {String.Join(", ", userIndexes.Take(10))}

                Unmarked clusters are only adopted automatically when empty. If this cluster really is disposable, either
                set {OptInVariable}={OptInValue} to acknowledge that every index in it may be deleted, or add the marker explicitly:

                {BuildManualProvisioningHelp()}
                """);
        }

        var marker = new DisposableClusterMarker
        {
            Purpose = ExpectedPurpose,
            CreatedUtc = DateTime.UtcNow.ToString("O")
        };

        var response = await client.IndexAsync(marker, i => i.Index(MarkerIndex).Id(MarkerId).Refresh(Refresh.True), cancellationToken).ConfigureAwait(false);
        if (!response.IsValidResponse)
        {
            throw new InvalidOperationException(
                $"""
                Refusing to run destructive integration tests: failed to write the disposable-cluster marker.

                Status : {response.ApiCallDetails?.HttpStatusCode.ToString() ?? "(no response)"}
                Error  : {response.ElasticsearchServerError?.Error?.Reason ?? "unknown"}

                {BuildManualProvisioningHelp()}
                """);
        }
    }

    private static string BuildManualProvisioningHelp() =>
        $$"""
        curl -XPUT "$ELASTICSEARCH_URL/{{MarkerIndex}}/_doc/{{MarkerId}}?refresh=true" \
             -H 'Content-Type: application/json' \
             -d '{"purpose":"{{ExpectedPurpose}}"}'
        """;
}
