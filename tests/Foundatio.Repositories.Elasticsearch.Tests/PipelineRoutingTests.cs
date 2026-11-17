using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class PipelineRoutingTests(ITestOutputHelper output) : ElasticRepositoryTestBase(output)
{
    private const string PipelineId = "pipeline-routing-lowercase-name";

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task PatchAsync_WithUnroutedAndRoutedCopies_UpdatesEachRequestedLocationOnce(bool pipeline, bool multipleIds)
    {
        var invoker = new CapturingInvoker(new HttpRequestInvoker());
        using var configuration = new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log) { RequestInvoker = invoker };
        var index = new RoutedEmployeeIndex(configuration);
        using var repository = new RoutedEmployeeRepository(index, pipeline ? PipelineId : null);
        await index.DeleteAsync();
        try
        {
            await index.ConfigureAsync();
            var pipelineResponse = await _client.Ingest.PutPipelineAsync(PipelineId, descriptor => descriptor
                .Processors(processor => processor.Lowercase(lowercase => lowercase.Field("name"))), TestCancellationToken);
            Assert.True(pipelineResponse.IsValidResponse, pipelineResponse.DebugInformation);
            var (unrouted, routed) = await CreateDocumentsOnSeparateShardsAsync(index);
            var patch = new ScriptPatch("ctx._source.yearsEmployed += 1;");

            if (multipleIds)
            {
                var ids = new Ids(new Id(unrouted.Id), new Id(routed.Id, routed.CompanyId));
                Assert.Equal(2, await repository.PatchAsync(ids, patch, options => options.ImmediateConsistency().Retry(1)));
            }
            else
            {
                Assert.True(await repository.PatchAsync(new Id(unrouted.Id), patch, options => options.ImmediateConsistency().Retry(1)));
            }

            var first = await _client.GetAsync<Employee>(new GetRequest(index.Name, unrouted.Id), TestCancellationToken);
            var second = await _client.GetAsync<Employee>(new GetRequest(index.Name, routed.Id) { Routing = routed.CompanyId }, TestCancellationToken);
            Assert.True(first.Found);
            Assert.True(second.Found);
            Assert.Equal(1, first.Source?.YearsEmployed);
            int expectedRoutedUpdates = multipleIds ? 1 : 0;
            Assert.True(second.Source?.YearsEmployed == expectedRoutedUpdates,
                $"Expected routed document {routed.CompanyId}/{routed.Id} to be updated {expectedRoutedUpdates} times, but it was updated {second.Source?.YearsEmployed} times. Captured update-by-query requests: {String.Join(Environment.NewLine, invoker.UpdateByQueryRequests)}");
        }
        finally
        {
            await index.DeleteAsync();
            await _client.Ingest.DeletePipelineAsync(PipelineId, cancellationToken: TestCancellationToken);
        }
    }

    private async Task<(Employee Unrouted, Employee Routed)> CreateDocumentsOnSeparateShardsAsync(RoutedEmployeeIndex index)
    {
        var unrouted = EmployeeGenerator.Generate(id: "shared-id", companyId: "", name: "Unrouted", yearsEmployed: 0);
        var response = await _client.IndexAsync(unrouted, descriptor => descriptor.Index(index.Name).Id(unrouted.Id)
            .OpType(OpType.Create).Refresh(Refresh.True), TestCancellationToken);
        Assert.True(response.IsValidResponse, response.DebugInformation);

        for (int route = 0; route < 32; route++)
        {
            var routed = EmployeeGenerator.Generate(id: unrouted.Id, companyId: $"parent-{route}", name: "Routed", yearsEmployed: 0);
            response = await _client.IndexAsync(routed, descriptor => descriptor.Index(index.Name).Id(routed.Id).Routing(routed.CompanyId)
                .OpType(OpType.Create).Refresh(Refresh.True), TestCancellationToken);
            if (response.IsValidResponse)
                return (unrouted, routed);

            Assert.Equal(409, response.ApiCallDetails.HttpStatusCode);
        }

        throw new InvalidOperationException("Could not find a routing value on a different shard from the unrouted document.");
    }

    private sealed class RoutedEmployeeIndex(IElasticConfiguration configuration) : Index<Employee>(configuration, "pipeline-routing-employees")
    {
        public override void ConfigureIndex(CreateIndexRequestDescriptor descriptor)
        {
            base.ConfigureIndex(descriptor.Settings(settings => settings.NumberOfShards(2).NumberOfReplicas(0)));
        }
    }

    private sealed class RoutedEmployeeRepository : ElasticRepositoryBase<Employee>
    {
        public RoutedEmployeeRepository(IIndex index, string? pipeline) : base(index)
        {
            HasParent = true;
            GetParentIdFunc = employee => employee.CompanyId;
            DefaultPipeline = pipeline;
            DisableCache();
            NotificationsEnabled = false;
        }
    }

    private sealed class CapturingInvoker(IRequestInvoker inner) : IRequestInvoker
    {
        public List<string> UpdateByQueryRequests { get; } = [];
        public ResponseFactory ResponseFactory => inner.ResponseFactory;

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            var response = inner.Request<TResponse>(endpoint, boundConfiguration, postData);
            Capture(endpoint, response);
            return response;
        }

        public async Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            var response = await inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);
            Capture(endpoint, response);
            return response;
        }

        private void Capture(Endpoint endpoint, TransportResponse response)
        {
            if (endpoint.PathAndQuery.Contains("_update_by_query", StringComparison.Ordinal))
            {
                string body = response.ApiCallDetails.RequestBodyInBytes is { } bytes ? Encoding.UTF8.GetString(bytes) : "<body unavailable>";
                UpdateByQueryRequests.Add($"{endpoint.PathAndQuery}: {body}");
            }
        }

        public void Dispose() => inner.Dispose();
    }
}
