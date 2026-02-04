using Foundatio.Serializer;

namespace Foundatio.Repositories.Tests.Utility;

public static class SerializerTestHelper
{
    public static ITextSerializer[] GetTextSerializers() =>
    [
        new SystemTextJsonSerializer(),
        new JsonNetSerializer()
    ];
}
