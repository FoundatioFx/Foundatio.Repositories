using Elasticsearch.Net;
using Nest;
using Nest.JsonNetSerializer;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

public class ElasticsearchJsonNetSerializer : ConnectionSettingsAwareSerializerBase
{
    public ElasticsearchJsonNetSerializer(IElasticsearchSerializer builtinSerializer, IConnectionSettingsValues connectionSettings)
        : base(builtinSerializer, connectionSettings) { }

    protected override JsonSerializerSettings CreateJsonSerializerSettings() =>
        new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Include
        };

    protected override void ModifyContractResolver(ConnectionSettingsAwareContractResolver resolver)
    {
        resolver.NamingStrategy = new CamelCaseNamingStrategy();
    }
}
