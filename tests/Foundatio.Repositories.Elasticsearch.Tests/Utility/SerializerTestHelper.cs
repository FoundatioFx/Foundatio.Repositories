using Foundatio.Serializer;

namespace Foundatio.Repositories.Elasticsearch.Tests.Utility;

public static class SerializerTestHelper
{
    public static ITextSerializer[] GetTextSerializers() =>
    [
        new SystemTextJsonSerializer(),
        new JsonNetSerializer()
    ];
}
