using System.Text.Json;
using Foundatio.Repositories.Serialization;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Tests.Serialization;

public static class SerializerTestHelper
{
    public static ITextSerializer[] GetTextSerializers() =>
    [
        new SystemTextJsonSerializer(new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults()),
        new JsonNetSerializer()
    ];
}
