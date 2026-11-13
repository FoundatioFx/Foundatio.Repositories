using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Serialization;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DocumentVersionSourceExclusionTests
{
    private static JsonSerializerOptions CreateOptions(params System.Type[] documentTypes)
    {
        return new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }
            .ConfigureFoundatioRepositoryDefaults()
            .ExcludeDocumentVersionFromSource(documentTypes);
    }

    [Fact]
    public void Serialize_WithRegisteredRootDocument_ExcludesRootVersionAndKeepsNestedVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));
        var document = new VersionedRootDocument
        {
            Id = "1",
            Version = "1:5",
            MetaData = new VersionedMetaData { Version = "business-7", Name = "nested" }
        };

        // Act
        string json = JsonSerializer.Serialize(document, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-7", parsed.RootElement.GetProperty("metaData").GetProperty("version").GetString());
    }

    [Fact]
    public void Deserialize_WithStaleRootVersionInSource_IgnoresRootVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));
        const string json = """{"id":"1","version":"112:2","metaData":{"version":"business-7","name":"nested"}}""";

        // Act
        var document = JsonSerializer.Deserialize<VersionedRootDocument>(json, options);

        // Assert
        Assert.NotNull(document);
        Assert.Null(document.Version);
        Assert.Equal("business-7", document.MetaData?.Version);
    }

    [Fact]
    public void Serialize_WithUnregisteredVersionedType_KeepsVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));

        // Act
        string json = JsonSerializer.Serialize(new VersionedMetaData { Version = "3", Name = "standalone" }, options);

        // Assert
        Assert.Contains("\"version\":\"3\"", json);
    }

    [Fact]
    public void Serialize_WithDerivedDocumentType_ExcludesVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));

        // Act
        string derivedJson = JsonSerializer.Serialize(new DerivedRootDocument { Id = "1", Version = "1:5", Extra = "x" }, options);
        string polymorphicJson = JsonSerializer.Serialize<VersionedRootDocument>(new DerivedRootDocument { Id = "1", Version = "1:5", Extra = "x" }, options);
        var deserialized = JsonSerializer.Deserialize<DerivedRootDocument>("""{"id":"1","version":"9:9","extra":"x"}""", options);

        // Assert
        Assert.DoesNotContain("version", derivedJson);
        Assert.Contains("\"extra\":\"x\"", derivedJson);
        Assert.DoesNotContain("version", polymorphicJson);
        Assert.NotNull(deserialized);
        Assert.Null(deserialized.Version);
    }

    [Fact]
    public void Serialize_WithRootTypeNestedInAnotherDocument_KeepsNestedVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(ContainerDocument));
        var container = new ContainerDocument
        {
            Id = "c",
            Version = "1:1",
            Child = new VersionedRootDocument { Id = "child", Version = "2:7" },
            Children = [new VersionedRootDocument { Id = "child2", Version = "3:8" }]
        };

        // Act
        string json = JsonSerializer.Serialize(container, options);
        var roundTripped = JsonSerializer.Deserialize<ContainerDocument>(json, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("2:7", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
        Assert.Equal("3:8", parsed.RootElement.GetProperty("children")[0].GetProperty("version").GetString());
        Assert.Equal("2:7", roundTripped?.Child?.Version);
        Assert.Equal("3:8", roundTripped?.Children[0].Version);
    }

    [Fact]
    public void Serialize_WithRootTypeInObjectAndDictionaryProperties_KeepsNestedVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(DynamicContainerDocument));
        var container = new DynamicContainerDocument
        {
            Id = "c",
            Version = "1:1",
            Payload = new VersionedRootDocument { Id = "payload", Version = "2:7" },
            Items = new Dictionary<string, object> { ["entry"] = new VersionedRootDocument { Id = "entry", Version = "3:8" } },
            List = [new VersionedRootDocument { Id = "listed", Version = "4:9" }]
        };

        // Act
        string json = JsonSerializer.Serialize(container, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("2:7", parsed.RootElement.GetProperty("payload").GetProperty("version").GetString());
        Assert.Equal("3:8", parsed.RootElement.GetProperty("items").GetProperty("entry").GetProperty("version").GetString());
        Assert.Equal("4:9", parsed.RootElement.GetProperty("list")[0].GetProperty("version").GetString());
    }

    [Fact]
    public void Serialize_WithObjectPropertyHoldingNonDocumentValues_SerializesNormally()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(DynamicContainerDocument));
        var container = new DynamicContainerDocument
        {
            Id = "c",
            Payload = "text",
            Items = new Dictionary<string, object> { ["number"] = 5, ["nested"] = new VersionedMetaData { Version = "business-7", Name = "n" } }
        };

        // Act
        string json = JsonSerializer.Serialize(container, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.Equal("text", parsed.RootElement.GetProperty("payload").GetString());
        Assert.Equal(5, parsed.RootElement.GetProperty("items").GetProperty("number").GetInt32());
        Assert.Equal("business-7", parsed.RootElement.GetProperty("items").GetProperty("nested").GetProperty("version").GetString());
    }

    [Fact]
    public void Serialize_WithRegisteredDocumentBehindPolymorphicBase_KeepsNestedVersion()
    {
        var options = CreateOptions(typeof(PolymorphicContainerDocument), typeof(PolymorphicRootDocument));
        var document = new PolymorphicContainerDocument
        {
            Version = "1:5",
            Child = new PolymorphicRootDocument { Version = "business-2", Name = "nested" }
        };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<PolymorphicContainerDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
        Assert.Equal("business-2", Assert.IsType<PolymorphicRootDocument>(roundTripped?.Child).Version);
    }

    [Fact]
    public void Serialize_WithRegisteredDocumentInNonGenericCollection_KeepsNestedVersion()
    {
        var options = CreateOptions(typeof(CollectionContainerDocument), typeof(VersionedRootDocument));
        var document = new CollectionContainerDocument
        {
            Version = "1:5",
            Children = [new VersionedRootDocument { Id = "nested", Version = "business-2" }]
        };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<CollectionContainerDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("children")[0].GetProperty("version").GetString());
        Assert.Equal("business-2", roundTripped?.Children[0].Version);
    }

    [Fact]
    public void Serialize_WithExtensionData_RoundTripsValuesAndExcludesRootVersion()
    {
        var options = CreateOptions(typeof(ExtensionDataDocument));
        var document = new ExtensionDataDocument
        {
            Id = "extension",
            Version = "1:5",
            Extra = new Dictionary<string, object> { ["label"] = "preserved", ["number"] = 7 }
        };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<ExtensionDataDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("preserved", parsed.RootElement.GetProperty("label").GetString());
        Assert.Equal(7, parsed.RootElement.GetProperty("number").GetInt32());
        Assert.NotNull(roundTripped);
        Assert.Null(roundTripped.Version);
        Assert.Equal(2, roundTripped.Extra.Count);
    }

    [Fact]
    public void Serialize_WithRegisteredDocumentInExtensionData_KeepsNestedVersion()
    {
        var options = CreateOptions(typeof(ExtensionDataDocument), typeof(VersionedRootDocument));
        var document = new ExtensionDataDocument
        {
            Id = "extension",
            Version = "1:5",
            Extra = new Dictionary<string, object>
            {
                ["child"] = new VersionedRootDocument { Id = "nested", Version = "business-2" }
            }
        };

        string json = JsonSerializer.Serialize(document, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
    }

    [Theory]
    [InlineData("\"1:5\"")]
    [InlineData("23")]
    [InlineData("{\"legacy\":true}")]
    public void Deserialize_WithStaleRootVersionAndExtensionData_DoesNotCaptureVersion(string storedVersion)
    {
        var options = CreateOptions(typeof(ExtensionDataDocument));

        var document = JsonSerializer.Deserialize<ExtensionDataDocument>($$"""{"id":"extension","version":{{storedVersion}},"label":"preserved"}""", options);

        Assert.NotNull(document);
        Assert.Null(document.Version);
        Assert.False(document.Extra.ContainsKey("version"));
        Assert.Single(document.Extra);
        Assert.DoesNotContain("\"version\"", JsonSerializer.Serialize(document, options));
    }

    [Fact]
    public void Serialize_WithCustomExtensionDataDictionary_PreservesValuesWithoutMutation()
    {
        var options = CreateOptions(typeof(CustomExtensionDataDocument), typeof(VersionedRootDocument));
        var child = new VersionedRootDocument { Id = "nested", Version = "business-2" };
        var extra = new ExtensionDictionary { ["child"] = child, ["name"] = "preserved", ["empty"] = null! };
        var document = new CustomExtensionDataDocument { Version = "1:5", Extra = extra };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<CustomExtensionDataDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
        Assert.Equal("preserved", parsed.RootElement.GetProperty("name").GetString());
        Assert.Equal(JsonValueKind.Null, parsed.RootElement.GetProperty("empty").ValueKind);
        Assert.Same(extra, document.Extra);
        Assert.Same(child, extra["child"]);
        Assert.Equal("business-2", child.Version);
        Assert.NotNull(roundTripped);
        Assert.IsType<ExtensionDictionary>(roundTripped.Extra);
        Assert.Equal(3, roundTripped.Extra.Count);
    }

    [Fact]
    public void Serialize_WithNullExtensionDataDictionary_SerializesNormally()
    {
        var options = CreateOptions(typeof(ExtensionDataDocument));
        var document = new ExtensionDataDocument { Id = "extension", Version = "1:5", Extra = null! };

        string json = JsonSerializer.Serialize(document, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("extension", parsed.RootElement.GetProperty("id").GetString());
        Assert.Single(parsed.RootElement.EnumerateObject());
    }

    [Fact]
    public void Serialize_WithJsonElementExtensionData_KeepsValuesAndIgnoresLegacyVersion()
    {
        var options = CreateOptions(typeof(JsonElementExtensionDataDocument));
        var document = JsonSerializer.Deserialize<JsonElementExtensionDataDocument>("""{"version":"1:5","child":{"version":"business-2"}}""", options);

        Assert.NotNull(document);
        string json = JsonSerializer.Serialize(document, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.Null(document.Version);
        Assert.False(document.Extra.ContainsKey("version"));
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
    }

    [Fact]
    public void Serialize_WithExplicitConcurrencyVersion_KeepsPublicBusinessVersion()
    {
        var options = CreateOptions(typeof(ExplicitVersionDocument));
        var document = new ExplicitVersionDocument { Id = "explicit", Version = "business-7" };
        ((IVersioned)document).Version = "1:5";

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<ExplicitVersionDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.Equal("business-7", parsed.RootElement.GetProperty("version").GetString());
        Assert.NotNull(roundTripped);
        Assert.Equal("business-7", roundTripped.Version);
        Assert.Null(((IVersioned)roundTripped).Version);
    }

    [Fact]
    public void ExcludeDocumentVersionFromSource_WithConcurrentFirstUse_IsThreadSafe()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(ContainerDocument));
        var container = new ContainerDocument { Id = "c", Version = "1:1", Child = new VersionedRootDocument { Id = "child", Version = "2:7" } };

        // Act
        var results = new System.Collections.Concurrent.ConcurrentBag<string>();
        System.Threading.Tasks.Parallel.For(0, 32, _ => results.Add(JsonSerializer.Serialize(container, options)));

        // Assert
        Assert.All(results, json => Assert.Contains("\"version\":\"2:7\"", json));
    }

    [Fact]
    public void ExcludeDocumentVersionFromSource_WithSeparateOptions_DoesNotAffectOtherOptions()
    {
        // Arrange
        var registered = CreateOptions(typeof(VersionedRootDocument));
        var other = CreateOptions(typeof(ContainerDocument));
        var document = new VersionedRootDocument { Id = "1", Version = "1:5" };

        // Act & Assert
        Assert.DoesNotContain("version", JsonSerializer.Serialize(document, registered));
        Assert.Contains("\"version\":\"1:5\"", JsonSerializer.Serialize(document, other));
    }

    [Theory]
    [InlineData("list")]
    [InlineData("array")]
    [InlineData("dictionary")]
    [InlineData("inherited-list")]
    [InlineData("inherited-dictionary")]
    [InlineData("interface-list")]
    [InlineData("named-floating-point")]
    public void SerializeAndDeserialize_WithNumberHandlingOnNumericCollections_PreservesPropertyFlags(string shape)
    {
        object document = shape switch
        {
            "list" => new NumberHandlingDocument<List<int>> { Values = [1, 2] },
            "array" => new NumberHandlingDocument<int[]> { Values = [1, 2] },
            "dictionary" => new NumberHandlingDocument<Dictionary<string, int>> { Values = new() { ["one"] = 1 } },
            "inherited-list" => new NumberHandlingDocument<NumberList> { Values = [1, 2] },
            "inherited-dictionary" => new NumberHandlingDocument<NumberDictionary> { Values = new() { ["one"] = 1 } },
            "interface-list" => new NumberHandlingDocument<IReadOnlyList<int>> { Values = new[] { 1, 2 } },
            "named-floating-point" => new NumberHandlingDocument<double[]> { Values = [Double.NaN, Double.PositiveInfinity, 1.5] },
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        if (shape is "named-floating-point")
            AssertNumberHandlingMatchesUnmodifiedSerializer(document, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        else
            AssertNumberHandlingMatchesUnmodifiedSerializer(document);
    }

    [Theory]
    [InlineData("object-number")]
    [InlineData("object-document")]
    [InlineData("object-list")]
    [InlineData("object-dictionary")]
    [InlineData("array-list")]
    public void SerializeAndDeserialize_WithNumberHandlingOnDocumentBearingValues_PreservesPropertyScope(string shape)
    {
        var child = new NumericNestedDocument { Version = "business-2", Number = 7, Numbers = [8, 9] };
        object document = shape switch
        {
            "object-number" => new NumberHandlingDocument<object> { Values = 3 },
            "object-document" => new NumberHandlingDocument<object> { Values = child },
            "object-list" => new NumberHandlingDocument<List<object>> { Values = [3, child] },
            "object-dictionary" => new NumberHandlingDocument<Dictionary<string, object>> { Values = new() { ["number"] = 3, ["child"] = child } },
            "array-list" => new NumberHandlingDocument<ArrayList> { Values = [3, child] },
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        AssertNumberHandlingMatchesUnmodifiedSerializer(document, typeof(NumericNestedDocument));
    }

    [Fact]
    public void SerializeAndDeserialize_WithStrictCollectionProperty_DoesNotInheritGlobalNumberHandling()
    {
        var options = CreateOptions(typeof(StrictNumberHandlingDocument));
        options.NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString;

        string json = JsonSerializer.Serialize(new StrictNumberHandlingDocument { Version = "1:5", Values = [1, 2] }, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal(JsonValueKind.Number, parsed.RootElement.GetProperty("values")[0].ValueKind);
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<StrictNumberHandlingDocument>("""{"values":["1"]}""", options));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Serialize_WithNativeObjectNumberHandling_PreservesWriteAsString(bool runtimeCollection)
    {
        var document = new NumberHandlingDocument<object> { Values = runtimeCollection ? new List<int> { 3 } : 3 };

        AssertNumberHandlingMatchesUnmodifiedSerializer(document, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
    }

    [Fact]
    public void Serialize_WithStrictWrappedProperty_PreservesOverrideWithoutChangingDescendants()
    {
        var document = new StrictWrappedNumberHandlingDocument
        {
            Values = [3, new NumericNestedDocument { Version = "business-2", Number = 7, Numbers = [8] }]
        };
        var baselineOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString
        };
        using var baseline = JsonDocument.Parse(JsonSerializer.Serialize(document, baselineOptions));
        Assert.Equal(JsonValueKind.Number, baseline.RootElement.GetProperty("values")[0].ValueKind);
        Assert.Equal(JsonValueKind.String, baseline.RootElement.GetProperty("values")[1].GetProperty("number").ValueKind);

        AssertNumberHandlingMatchesUnmodifiedSerializer(document, baselineOptions, typeof(NumericNestedDocument));
    }

    [Fact]
    public void Serialize_WithDeclaringTypeNumberHandling_PreservesDirectElementsWithoutChangingDescendants()
    {
        var document = new DeclaringTypeNumberHandlingDocument
        {
            Values = [3, new NumericNestedDocument { Version = "business-2", Number = 7, Numbers = [8] }],
            TypedChild = new NumericNestedDocument { Version = "typed-3", Number = 4, Numbers = [5] }
        };

        AssertNumberHandlingMatchesUnmodifiedSerializer(document, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }, typeof(NumericNestedDocument));
    }

    [Fact]
    public void SerializeAndDeserialize_WithGeneratedNumberHandlingMetadata_PreservesNestedValues()
    {
        var document = new GeneratedNumberHandlingDocument
        {
            Values = [3, new GeneratedNumericNestedDocument { Version = "business-2", Number = 7 }]
        };
        var baselineOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            TypeInfoResolver = NumberHandlingGeneratedContext.Default
        };

        AssertNumberHandlingMatchesUnmodifiedSerializer(document, baselineOptions, typeof(GeneratedNumericNestedDocument));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SerializeAndDeserialize_WithNumberHandlingOnExtensionData_PreservesNativeValueScope(bool repositoryDefaults)
    {
        var document = new NumberHandlingExtensionDocument
        {
            Extra = new Dictionary<string, object>
            {
                ["number"] = 3,
                ["child"] = new NumericNestedDocument { Version = "business-2", Number = 7, Numbers = [8] }
            }
        };
        var baselineOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        if (repositoryDefaults)
            baselineOptions.ConfigureFoundatioRepositoryDefaults();

        AssertNumberHandlingMatchesUnmodifiedSerializer(document, baselineOptions, typeof(NumericNestedDocument));
    }

    [Fact]
    public void Deserialize_WithNumericCollectionPopulate_PreservesNativePopulation()
    {
        var options = CreateOptions(typeof(NumericPopulateDocument));

        var document = JsonSerializer.Deserialize<NumericPopulateDocument>("""{"values":[2]}""", options);

        Assert.NotNull(document);
        Assert.Equal(new[] { 1, 2 }, document.Values);
    }

    [Theory]
    [InlineData("property")]
    [InlineData("type")]
    [InlineData("options")]
    public void Deserialize_WithPopulateOnDocumentBearingProperty_ThrowsClearUnsupportedCombination(string scope)
    {
        Type documentType = scope switch
        {
            "property" => typeof(ExplicitPopulateDocument),
            "type" => typeof(TypePopulateDocument),
            _ => typeof(PopulateContainerDocument)
        };
        var options = CreateOptions(documentType, typeof(VersionedRootDocument));
        if (scope is "options")
            options.PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate;

        var exception = Assert.Throws<NotSupportedException>(() => JsonSerializer.Deserialize("""{"children":[{"id":"new","version":"business-2"}]}""", documentType, options));

        Assert.Contains("Populate", exception.Message);
        Assert.Contains("children", exception.Message);
    }

    [Fact]
    public void Deserialize_WithExplicitReplaceOnDocumentBearingProperty_OverridesPopulatePreference()
    {
        var options = CreateOptions(typeof(ReplacePopulateDocument), typeof(VersionedRootDocument));
        options.PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate;

        var document = JsonSerializer.Deserialize<ReplacePopulateDocument>("""{"children":[{"id":"new","version":"business-2"}]}""", options);

        Assert.NotNull(document);
        Assert.Equal("new", Assert.Single(document.Children).Id);
        Assert.Equal("business-2", document.Children[0].Version);
    }

    [Fact]
    public void Deserialize_WithIgnoredDocumentProperty_DoesNotRejectPopulatePreference()
    {
        var options = CreateOptions(typeof(IgnoredPopulateDocument), typeof(VersionedRootDocument));
        options.PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate;

        var document = JsonSerializer.Deserialize<IgnoredPopulateDocument>("""{"number":7}""", options);

        Assert.NotNull(document);
        Assert.Equal(7, document.Number);
    }

    [Fact]
    public void Serialize_WithInapplicableNumberHandlingOnTypedDocument_PreservesNativeRejection()
    {
        var document = new InvalidNumberHandlingDocument { Child = new NumericNestedDocument { Version = "business-2", Number = 7 } };
        var plain = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        var excluded = new JsonSerializerOptions(plain).ExcludeDocumentVersionFromSource([typeof(InvalidNumberHandlingDocument), typeof(NumericNestedDocument)]);

        Assert.Throws<InvalidOperationException>(() => JsonSerializer.Serialize(document, plain));
        Assert.Throws<InvalidOperationException>(() => JsonSerializer.Serialize(document, excluded));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Serialize_WithReferenceHandlingAndNestedDocuments_RejectsIndependentReferenceScopes(bool preserve, bool extensionData)
    {
        object document;
        var child = new ReferenceDocument { Version = "business-2" };
        if (extensionData)
        {
            document = new ExtensionDataDocument { Version = "1:5", Extra = new() { ["first"] = child, ["second"] = child } };
        }
        else
        {
            var parent = new ReferenceContainerDocument { Version = "1:5", First = child, Second = child };
            child.Parent = parent;
            document = parent;
        }
        var options = CreateOptions(document.GetType(), typeof(ReferenceDocument));
        options.ReferenceHandler = preserve ? ReferenceHandler.Preserve : ReferenceHandler.IgnoreCycles;

        var exception = Assert.Throws<NotSupportedException>(() => JsonSerializer.Serialize(document, document.GetType(), options));

        Assert.Contains("ReferenceHandler", exception.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SerializeAndDeserialize_WithReferenceHandlingOnFlatRoot_KeepsNativeSupport(bool preserve)
    {
        var options = CreateOptions(typeof(FlatReferenceDocument));
        options.ReferenceHandler = preserve ? ReferenceHandler.Preserve : ReferenceHandler.IgnoreCycles;

        string json = JsonSerializer.Serialize(new FlatReferenceDocument { Version = "1:5", Number = 7 }, options);
        var roundTripped = JsonSerializer.Deserialize<FlatReferenceDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.NotNull(roundTripped);
        Assert.Equal(7, roundTripped.Number);
    }

    private static void AssertNumberHandlingMatchesUnmodifiedSerializer(object document, params Type[] nestedDocumentTypes)
    {
        var baselineOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }.ConfigureFoundatioRepositoryDefaults();
        AssertNumberHandlingMatchesUnmodifiedSerializer(document, baselineOptions, nestedDocumentTypes);
    }

    private static void AssertNumberHandlingMatchesUnmodifiedSerializer(object document, JsonSerializerOptions baselineOptions, params Type[] nestedDocumentTypes)
    {
        var documentTypes = new List<Type>(nestedDocumentTypes) { document.GetType() };
        var options = new JsonSerializerOptions(baselineOptions).ExcludeDocumentVersionFromSource(documentTypes);
        string baselineJson = JsonSerializer.Serialize(document, document.GetType(), baselineOptions);
        var baselineRoundTripped = JsonSerializer.Deserialize(baselineJson, document.GetType(), baselineOptions);
        Assert.NotNull(baselineRoundTripped);
        Assert.Equal(baselineJson, JsonSerializer.Serialize(baselineRoundTripped, baselineRoundTripped.GetType(), baselineOptions));

        string json = JsonSerializer.Serialize(document, document.GetType(), options);
        var roundTripped = JsonSerializer.Deserialize(json, document.GetType(), options);

        using var baseline = JsonDocument.Parse(baselineJson);
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        foreach (var property in baseline.RootElement.EnumerateObject())
        {
            if (property.Name is not "version")
                Assert.Equal(property.Value.GetRawText(), parsed.RootElement.GetProperty(property.Name).GetRawText());
        }
        Assert.NotNull(roundTripped);
        Assert.Equal(json, JsonSerializer.Serialize(roundTripped, roundTripped.GetType(), options));
    }

    private class VersionedRootDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public VersionedMetaData? MetaData { get; set; }
    }

    private sealed class DerivedRootDocument : VersionedRootDocument
    {
        public string? Extra { get; set; }
    }

    private sealed class ContainerDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public VersionedRootDocument? Child { get; set; }
        public List<VersionedRootDocument> Children { get; set; } = [];
    }

    private sealed class DynamicContainerDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public object? Payload { get; set; }
        public Dictionary<string, object> Items { get; set; } = [];
        public List<object> List { get; set; } = [];
    }

    [JsonDerivedType(typeof(PolymorphicRootDocument), "root")]
    private abstract class PolymorphicBase
    {
        public string? Name { get; set; }
    }

    private sealed class PolymorphicRootDocument : PolymorphicBase, IVersioned
    {
        public string? Version { get; set; }
    }

    private sealed class PolymorphicContainerDocument : IVersioned
    {
        public string? Version { get; set; }
        public PolymorphicBase? Child { get; set; }
    }

    private sealed class DocumentList : List<VersionedRootDocument>
    {
    }

    private sealed class CollectionContainerDocument : IVersioned
    {
        public string? Version { get; set; }
        public DocumentList Children { get; set; } = [];
    }

    private sealed class VersionedMetaData : IVersioned
    {
        public string? Version { get; set; }
        public string? Name { get; set; }
    }

    private sealed class ExtensionDataDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }

        [JsonExtensionData]
        public Dictionary<string, object> Extra { get; set; } = [];
    }

    private sealed class ExplicitVersionDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        string? IVersioned.Version { get; set; }
    }

    private sealed class CustomExtensionDataDocument : IVersioned
    {
        public string? Version { get; set; }

        [JsonExtensionData]
        public ExtensionDictionary Extra { get; set; } = [];
    }

    private sealed class ExtensionDictionary : Dictionary<string, object>
    {
    }

    private sealed class JsonElementExtensionDataDocument : IVersioned
    {
        public string? Version { get; set; }

        [JsonExtensionData]
        public Dictionary<string, JsonElement> Extra { get; set; } = [];
    }

    private sealed class NumberHandlingDocument<TValues> : IVersioned
    {
        public string? Version { get; set; } = "1:5";

        [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString | JsonNumberHandling.AllowNamedFloatingPointLiterals)]
        public TValues Values { get; set; } = default!;

        public int Unannotated { get; set; } = 11;
    }

    private sealed class StrictNumberHandlingDocument : IVersioned
    {
        public string? Version { get; set; }

        [JsonNumberHandling(JsonNumberHandling.Strict)]
        public List<int> Values { get; set; } = [];
    }

    private sealed class NumberList : List<int>
    {
    }

    private sealed class NumberDictionary : Dictionary<string, int>
    {
    }

    private sealed class NumericNestedDocument : IVersioned
    {
        public string? Version { get; set; }
        public int Number { get; set; }
        public List<int> Numbers { get; set; } = [];
    }

    private sealed class StrictWrappedNumberHandlingDocument : IVersioned
    {
        public string? Version { get; set; } = "1:5";

        [JsonNumberHandling(JsonNumberHandling.Strict)]
        public List<object> Values { get; set; } = [];

        public int Unannotated { get; set; } = 11;
    }

    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString)]
    private sealed class DeclaringTypeNumberHandlingDocument : IVersioned
    {
        public string? Version { get; set; } = "1:5";
        public List<object> Values { get; set; } = [];
        public NumericNestedDocument? TypedChild { get; set; }
        public int Unannotated { get; set; } = 11;
    }

    private sealed class NumericPopulateDocument : IVersioned
    {
        public string? Version { get; set; }

        [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
        public List<int> Values { get; set; } = [1];
    }

    private class PopulateContainerDocument : IVersioned
    {
        public string? Version { get; set; }
        public virtual List<VersionedRootDocument> Children { get; set; } = [new() { Id = "old", Version = "business-1" }];
    }

    private sealed class ExplicitPopulateDocument : PopulateContainerDocument
    {
        [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
        public override List<VersionedRootDocument> Children { get; set; } = [new() { Id = "old", Version = "business-1" }];
    }

    [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
    private sealed class TypePopulateDocument : PopulateContainerDocument
    {
    }

    [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
    private sealed class ReplacePopulateDocument : PopulateContainerDocument
    {
        [JsonObjectCreationHandling(JsonObjectCreationHandling.Replace)]
        public override List<VersionedRootDocument> Children { get; set; } = [new() { Id = "old", Version = "business-1" }];
    }

    private sealed class InvalidNumberHandlingDocument : IVersioned
    {
        public string? Version { get; set; }

        [JsonNumberHandling(JsonNumberHandling.WriteAsString)]
        public NumericNestedDocument? Child { get; set; }
    }

    private sealed class NumberHandlingExtensionDocument : IVersioned
    {
        public string? Version { get; set; } = "1:5";

        [JsonExtensionData]
        [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString)]
        public Dictionary<string, object> Extra { get; set; } = [];
    }

    private sealed class IgnoredPopulateDocument : IVersioned
    {
        public string? Version { get; set; }
        public int Number { get; set; }

        [JsonIgnore]
        public List<VersionedRootDocument> Children { get; set; } = [];
    }

    private sealed class ReferenceContainerDocument : IVersioned
    {
        public string? Version { get; set; }
        public ReferenceDocument? First { get; set; }
        public ReferenceDocument? Second { get; set; }
    }

    private sealed class ReferenceDocument : IVersioned
    {
        public string? Version { get; set; }
        public ReferenceContainerDocument? Parent { get; set; }
    }

    private sealed class FlatReferenceDocument : IVersioned
    {
        public string? Version { get; set; }
        public int Number { get; set; }
    }
}
