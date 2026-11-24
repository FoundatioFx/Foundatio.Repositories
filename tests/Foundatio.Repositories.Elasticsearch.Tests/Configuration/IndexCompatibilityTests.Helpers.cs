using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private sealed class RequestInvokerElasticConfiguration : ElasticConfiguration
    {
        private readonly IRequestInvoker _requestInvoker;

        public RequestInvokerElasticConfiguration(IRequestInvoker requestInvoker)
        {
            _requestInvoker = requestInvoker;
        }

        public int RequestCount { get; private set; }
        public Uri? LastRequestUri { get; private set; }

        protected override ElasticsearchClient CreateElasticClient()
        {
            var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), _requestInvoker)
                .OnRequestCompleted(call =>
                {
                    RequestCount++;
                    LastRequestUri = call.Uri;
                });
            return new ElasticsearchClient(settings);
        }
    }

    private sealed record StubResponse(int StatusCode, string Content, Exception? Exception = null, string? Request = null);

    private sealed class SequenceRequestInvoker : IRequestInvoker
    {
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new()
        {
            ["x-elastic-product"] = ["Elasticsearch"]
        };

        private readonly Queue<StubResponse> _responses;
        private readonly InMemoryRequestInvoker _responseFactory = new();

        public SequenceRequestInvoker(params StubResponse[] responses)
        {
            _responses = new Queue<StubResponse>(responses);
        }

        public ResponseFactory ResponseFactory => _responseFactory.ResponseFactory;
        public List<string> Requests { get; } = [];
        public int RemainingResponses => _responses.Count;

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            return GetResponse(endpoint).Request<TResponse>(endpoint, boundConfiguration, postData);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            return GetResponse(endpoint).RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);
        }

        private InMemoryRequestInvoker GetResponse(Endpoint endpoint)
        {
            string request = $"{endpoint.Method} {Uri.UnescapeDataString(endpoint.Uri.AbsolutePath)}";
            Requests.Add(request);
            if (_responses.Count is 0)
                throw new InvalidOperationException("No response configured for request.");

            var response = _responses.Dequeue();
            if (response.Request is not null)
                global::Xunit.Assert.Equal(response.Request, request);
            return new InMemoryRequestInvoker(
                response.Exception is null ? Encoding.UTF8.GetBytes(response.Content) : [],
                response.StatusCode,
                response.Exception,
                "application/json",
                _headers);
        }

        public void Dispose()
        {
            ((IDisposable)_responseFactory).Dispose();
        }
    }
}
