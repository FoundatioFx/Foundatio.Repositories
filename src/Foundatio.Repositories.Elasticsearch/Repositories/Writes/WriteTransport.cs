using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Elastic.Transport.Diagnostics.Auditing;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Uses the configured client serializers, mappings and connection resources while leaving every write retry to
/// <see cref="WriteRoundRunner"/>. The source configuration owns the shared node pool and request invoker.
/// </summary>
internal sealed class WriteTransport : ITransport<IElasticsearchClientSettings>
{
    private readonly ITransport<IElasticsearchClientSettings> _readTransport;
    private readonly DistributedTransport<ITransportConfiguration> _transport;
    private readonly DistributedTransport<ITransportConfiguration> _transportWithoutPings;
    private readonly Func<Node, bool> _isEligible;
    private readonly WriteNodePool _nodePool;

    public WriteTransport(ITransport<IElasticsearchClientSettings> readTransport)
    {
        _readTransport = readTransport;
        Configuration = readTransport.Configuration;
        _isEligible = Configuration.NodePredicate ?? Configuration.ProductRegistration.NodePredicate;
        _nodePool = new WriteNodePool(Configuration.NodePool);
        var configuration = new WriteConfiguration(Configuration, _nodePool);
        _transport = new DistributedTransport<ITransportConfiguration>(configuration with { DisablePings = false });
        _transportWithoutPings = new DistributedTransport<ITransportConfiguration>(configuration with { DisablePings = true });
    }

    public IElasticsearchClientSettings Configuration { get; }
    ITransportConfiguration ITransport.Configuration => _transport.Configuration;

    public TResponse Request<TResponse>(in EndpointPath path, PostData? postData, Action<Activity>? configureActivity, IRequestConfiguration? localConfiguration)
        where TResponse : TransportResponse, new()
    {
        (DistributedTransport<ITransportConfiguration> Transport, RequestConfiguration Configuration, Node? Node, int ReseedVersion) request;
        try
        {
            if (NeedsDiscovery(localConfiguration))
                CompleteDiscovery(_readTransport.Request<StringResponse>(new EndpointPath(HttpMethod.HEAD, "/"), null, null, localConfiguration));

            request = PrepareRequest(localConfiguration);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return CreateUnsentResponse<TResponse>(path, postData, localConfiguration, ex);
        }

        var response = request.Transport.Request<TResponse>(path, postData, configureActivity, request.Configuration);
        UpdateNodeHealth(request.Node, request.ReseedVersion, response);
        return response;
    }

    public Task<TResponse> RequestAsync<TResponse>(in EndpointPath path, PostData? postData, Action<Activity>? configureActivity, IRequestConfiguration? localConfiguration, CancellationToken cancellationToken = default)
        where TResponse : TransportResponse, new()
    {
        return RequestAsyncCore<TResponse>(path, postData, configureActivity, localConfiguration, cancellationToken);
    }

    private async Task<TResponse> RequestAsyncCore<TResponse>(EndpointPath path, PostData? postData, Action<Activity>? configureActivity, IRequestConfiguration? localConfiguration, CancellationToken cancellationToken)
        where TResponse : TransportResponse, new()
    {
        (DistributedTransport<ITransportConfiguration> Transport, RequestConfiguration Configuration, Node? Node, int ReseedVersion) request;
        try
        {
            if (NeedsDiscovery(localConfiguration))
                CompleteDiscovery(await _readTransport.RequestAsync<StringResponse>(new EndpointPath(HttpMethod.HEAD, "/"), null, null, localConfiguration, cancellationToken).AnyContext());

            request = PrepareRequest(localConfiguration);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return CreateUnsentResponse<TResponse>(path, postData, localConfiguration, ex);
        }

        var response = await request.Transport.RequestAsync<TResponse>(path, postData, configureActivity, request.Configuration, cancellationToken).AnyContext();
        UpdateNodeHealth(request.Node, request.ReseedVersion, response);
        return response;
    }

    private bool NeedsDiscovery(IRequestConfiguration? localConfiguration)
    {
        if (!Configuration.NodePool.SupportsReseeding || !Configuration.ProductRegistration.SupportsSniff || (Configuration.DisableSniff ?? localConfiguration?.DisableSniff ?? false))
            return false;

        if (Configuration.SniffsOnStartup && !Configuration.NodePool.SniffedOnStartup)
            return true;

        var clock = Configuration.DateTimeProvider ?? DefaultDateTimeProvider.Default;
        return Configuration.SniffInformationLifeSpan is { } lifetime && Configuration.NodePool.LastUpdate is { } lastUpdate && clock.Now() - lastUpdate > lifetime;
    }

    private void CompleteDiscovery(TransportResponse response)
    {
        if (!response.ApiCallDetails.HasSuccessfulStatusCode)
            throw new TransportException(PipelineFailure.NoNodesAttempted, "Cluster discovery failed before the write was sent.", response.ApiCallDetails.OriginalException);

        _nodePool.Synchronize();
    }

    private TResponse CreateUnsentResponse<TResponse>(in EndpointPath path, PostData? postData, IRequestConfiguration? localConfiguration, Exception exception)
        where TResponse : TransportResponse, new()
    {
        var failure = new TransportException(PipelineFailure.NoNodesAttempted, "No write was sent because a target node could not be prepared.", exception);
        return Configuration.RequestInvoker.ResponseFactory.Create<TResponse>(Endpoint.Empty(path), new BoundConfiguration(_transport.Configuration, localConfiguration), postData, failure, null, null, Stream.Null, null, -1, null, null);
    }

    private (DistributedTransport<ITransportConfiguration> Transport, RequestConfiguration Configuration, Node? Node, int ReseedVersion) PrepareRequest(IRequestConfiguration? localConfiguration)
    {
        int reseedVersion = _nodePool.ReseedVersion;
        Uri? forceNode = Configuration.ForceNode ?? localConfiguration?.ForceNode;
        Node? node = null;
        if (forceNode is null)
        {
            node = _transport.Configuration.NodePool.CreateView().FirstOrDefault(_isEligible);
            if (node is null)
                throw new TransportException(PipelineFailure.NoNodesAttempted, "No configured node is eligible for this write.");

            forceNode = node.Uri;
        }

        // ForceNode is the transport's supported single-dispatch path, including after sniffing reseeds the pool.
        var configuration = localConfiguration is null
            ? new RequestConfiguration { ForceNode = forceNode }
            : new RequestConfiguration(localConfiguration) { ForceNode = forceNode };
        bool disablePings = (localConfiguration?.DisablePings ?? Configuration.DisablePings ?? !Configuration.NodePool.SupportsPinging) || node is { IsResurrected: false };
        return (disablePings ? _transportWithoutPings : _transport, configuration, node, reseedVersion);
    }

    private void UpdateNodeHealth(Node? node, int reseedVersion, TransportResponse response)
    {
        if (node is null)
            return;

        // ForceNode uses a temporary node, so preserve health on the shared node used by subsequent reads and writes.
        if (_nodePool.ReseedVersion != reseedVersion)
            node = _nodePool.CreateView().FirstOrDefault(current => current.Uri == node.Uri) ?? node;
        if (response.ApiCallDetails.HasExpectedContentType && (response.ApiCallDetails.HasSuccessfulStatusCode || response.ApiCallDetails.HttpStatusCode is >= 400 and < 599 and not (502 or 503 or 504)))
            node.MarkAlive();
        else
        {
            var clock = Configuration.DateTimeProvider ?? DefaultDateTimeProvider.Default;
            node.MarkDead(clock.DeadTime(node.FailedAttempts, Configuration.DeadTimeout, Configuration.MaxDeadTimeout));
        }
    }

    private sealed record WriteConfiguration : TransportConfiguration, ITransportConfiguration
    {
        private readonly NodePool _nodePool;

        public WriteConfiguration(IElasticsearchClientSettings settings, NodePool nodePool) : base(settings)
        {
            _nodePool = nodePool;
            MaxRetries = 0;
            DisablePings = settings.DisablePings ?? !settings.NodePool.SupportsPinging;
            ThrowExceptions = false;
            ResponseBuilders = settings.ResponseBuilders;
        }

        NodePool ITransportConfiguration.NodePool => _nodePool;
    }

    /// <summary>
    /// Keeps the standard pipeline enabled because the single-node fast path can resend ambiguous failures independently
    /// of retry limits. Request-scoped ForceNode bounds dispatch while this pool preserves discovery and shared state.
    /// </summary>
    private sealed class WriteNodePool : NodePool
    {
        private readonly NodePool _source;
        private int _reseedVersion;

        public WriteNodePool(NodePool source)
        {
            _source = source;
            DateTimeProvider = source.DateTimeProvider;
            UsingSsl = source.UsingSsl;
            LastUpdate = source.LastUpdate;
            if (source.SniffedOnStartup)
                MarkAsSniffed();
        }

        public override IReadOnlyCollection<Node> Nodes => _source.Nodes;
        public override int MaxRetries => 0;
        public override DateTimeOffset? LastUpdate { get; protected set; }
        public override bool SupportsReseeding => _source.SupportsReseeding;
        public int ReseedVersion => Volatile.Read(ref _reseedVersion);

        // Select the standard pipeline for all pool sizes. The chosen transport preserves whether pings are needed.
        public override bool SupportsPinging => true;

        public override IEnumerable<Node> CreateView(Auditor? auditor = null)
        {
            Synchronize();
            return _source.CreateView(auditor);
        }

        public void Synchronize()
        {
            LastUpdate = _source.LastUpdate;
            if (_source.SniffedOnStartup)
                MarkAsSniffed();
        }

        public override void Reseed(IEnumerable<Node> nodes)
        {
            _source.Reseed(nodes);
            _source.MarkAsSniffed();
            LastUpdate = _source.LastUpdate;
            Interlocked.Increment(ref _reseedVersion);
        }
    }
}
