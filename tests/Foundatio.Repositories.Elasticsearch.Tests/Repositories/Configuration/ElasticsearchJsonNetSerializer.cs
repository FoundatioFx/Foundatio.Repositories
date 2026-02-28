// Note: The NEST-style custom serializer (ConnectionSettingsAwareSerializerBase) is no longer available
// in the new Elastic.Clients.Elasticsearch client. Custom serialization should be handled differently.
// This file is kept for reference but the class is disabled.

// using Newtonsoft.Json;
// using Newtonsoft.Json.Serialization;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

// The new Elastic client uses System.Text.Json by default and has different extension points for serialization.
// If custom serialization is needed, see: https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/serialization.html

/*
public class ElasticsearchJsonNetSerializer
{
    // Custom serialization in the new client is handled via SourceSerializerFactory
    // Example:
    // var settings = new ElasticsearchClientSettings(pool,
    //     sourceSerializer: (defaultSerializer, settings) => new CustomSerializer());
}
*/
