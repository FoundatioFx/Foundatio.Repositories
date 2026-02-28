using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using Foundatio.Repositories.Utility;
using Xunit;

namespace Foundatio.Repositories.Tests.JsonPatch;

// TODO: is there a public nuget package we can use for this?
/// <summary>
/// Tests for JSON Patch (RFC 6902) operations.
/// Converted from Newtonsoft.Json (JToken) to System.Text.Json (JsonNode) to align with
/// Elastic.Clients.Elasticsearch which exclusively uses System.Text.Json for serialization.
/// </summary>
public class JsonPatchTests
{
    [Fact]
    public void Add_an_array_element()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/-";

        patchDocument.AddOperation(new AddOperation { Path = pointer, Value = JsonNode.Parse(@"{ ""author"": ""James Brown"" }") });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        var list = sample["books"] as JsonArray;

        Assert.Equal(3, list.Count);
    }

    [Fact]
    public void Add_an_array_element_to_non_existent_property()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/someobject/somearray/-";

        patchDocument.AddOperation(new AddOperation { Path = pointer, Value = JsonNode.Parse(@"{ ""author"": ""James Brown"" }") });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        var list = sample["someobject"]["somearray"] as JsonArray;

        Assert.Single(list);
    }

    [Fact]
    public void Add_an_existing_member_property()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/0/title";

        patchDocument.AddOperation(new AddOperation { Path = pointer, Value = JsonValue.Create("Little Red Riding Hood") });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        string result = sample.SelectPatchToken(pointer)?.GetValue<string>();
        Assert.Equal("Little Red Riding Hood", result);
    }

    [Fact]
    public void Add_an_non_existing_member_property()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/0/SBN";

        patchDocument.AddOperation(new AddOperation { Path = pointer, Value = JsonValue.Create("213324234343") });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        string result = sample.SelectPatchToken(pointer)?.GetValue<string>();
        Assert.Equal("213324234343", result);
    }

    [Fact]
    public void Copy_array_element()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string frompointer = "/books/0";
        string topointer = "/books/-";

        patchDocument.AddOperation(new CopyOperation { FromPath = frompointer, Path = topointer });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        var result = sample.SelectPatchToken("/books/2");
        Assert.IsType<JsonObject>(result);
    }

    [Fact]
    public void Copy_property()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string frompointer = "/books/0/ISBN";
        string topointer = "/books/1/ISBN";

        patchDocument.AddOperation(new AddOperation { Path = frompointer, Value = JsonValue.Create("21123123") });
        patchDocument.AddOperation(new CopyOperation { FromPath = frompointer, Path = topointer });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        var result = sample.SelectPatchToken("/books/1/ISBN");
        Assert.Equal("21123123", result?.GetValue<string>());
    }

    [Fact]
    public void Move_property()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string frompointer = "/books/0/author";
        string topointer = "/books/1/author";

        patchDocument.AddOperation(new MoveOperation { FromPath = frompointer, Path = topointer });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        string result = sample.SelectPatchToken(topointer)?.GetValue<string>();
        Assert.Equal("F. Scott Fitzgerald", result);
    }

    [Fact]
    public void Move_array_element()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string frompointer = "/books/1";
        string topointer = "/books/0/child";

        patchDocument.AddOperation(new MoveOperation { FromPath = frompointer, Path = topointer });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        var result = sample.SelectPatchToken(topointer);
        Assert.IsType<JsonObject>(result);
    }

    [Fact]
    public void CreateEmptyPatch()
    {
        var sample = GetSample2();
        string sampletext = sample.ToJsonString();

        var patchDocument = new PatchDocument();
        new JsonPatcher().Patch(ref sample, patchDocument);

        Assert.Equal(sampletext, sample.ToJsonString());
    }

    [Fact]
    public void TestExample1()
    {
        var targetDoc = JsonNode.Parse(@"{ ""foo"": ""bar""}");
        var patchDoc = PatchDocument.Parse(@"[
                                                    { ""op"": ""add"", ""path"": ""/baz"", ""value"": ""qux"" }
                                                ]");
        new JsonPatcher().Patch(ref targetDoc, patchDoc);

        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(@"{
                                                             ""foo"": ""bar"",
                                                             ""baz"": ""qux""
                                                           }"), targetDoc));
    }

    [Fact]
    public void SerializePatchDocument()
    {
        var patchDoc = new PatchDocument(
            new TestOperation { Path = "/a/b/c", Value = JsonValue.Create("foo") },
            new RemoveOperation { Path = "/a/b/c" },
            new AddOperation { Path = "/a/b/c", Value = new JsonArray(JsonValue.Create("foo"), JsonValue.Create("bar")) },
            new ReplaceOperation { Path = "/a/b/c", Value = JsonValue.Create(42) },
            new MoveOperation { FromPath = "/a/b/c", Path = "/a/b/d" },
            new CopyOperation { FromPath = "/a/b/d", Path = "/a/b/e" });

        string json = JsonSerializer.Serialize(patchDoc);
        var roundTripped = JsonSerializer.Deserialize<PatchDocument>(json);
        string roundTrippedJson = JsonSerializer.Serialize(roundTripped);
        Assert.Equal(json, roundTrippedJson);

        var outputstream = patchDoc.ToStream();
        string output = new StreamReader(outputstream).ReadToEnd();

        var jOutput = JsonNode.Parse(output);

        Assert.Equal(@"[{""op"":""test"",""path"":""/a/b/c"",""value"":""foo""},{""op"":""remove"",""path"":""/a/b/c""},{""op"":""add"",""path"":""/a/b/c"",""value"":[""foo"",""bar""]},{""op"":""replace"",""path"":""/a/b/c"",""value"":42},{""op"":""move"",""path"":""/a/b/d"",""from"":""/a/b/c""},{""op"":""copy"",""path"":""/a/b/e"",""from"":""/a/b/d""}]",
            jOutput.ToJsonString());
    }

    [Fact]
    public void Remove_a_property()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/0/author";

        patchDocument.AddOperation(new RemoveOperation { Path = pointer });

        new JsonPatcher().Patch(ref sample, patchDocument);

        Assert.Null(sample.SelectPatchToken(pointer));
    }

    [Fact]
    public void Remove_an_array_element()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/0";

        patchDocument.AddOperation(new RemoveOperation { Path = pointer });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        Assert.Null(sample.SelectPatchToken("/books/1"));
    }

    [Fact]
    public void Remove_an_array_element_with_numbered_custom_fields()
    {
        var sample = JsonNode.Parse(@"{
    ""data"": {
        ""2017PropertyOne"" : ""2017 property one value"",
        ""2017PropertyTwo"" : ""2017 property two value"",
        ""2017Properties"" : [""First value from 2017"",""Second value from 2017""],
        ""2018PropertyOne"" : ""2018 property value"",
        ""2018PropertyTwo"" : ""2018 property two value"",
        ""2018Properties"" : [""First value from 2018"",""Second value from 2018""]
    }
}");

        Assert.NotNull(sample.SelectPatchToken("/data/2017Properties/1"));

        var patchDocument = new PatchDocument();
        string pointer = "/data/2017Properties/0";

        patchDocument.AddOperation(new RemoveOperation { Path = pointer });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        Assert.Null(sample.SelectPatchToken("/data/2017Properties/1"));
    }

    [Fact]
    public void Replace_a_property_value_with_a_new_value()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/0/author";

        patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = JsonValue.Create("Bob Brown") });

        new JsonPatcher().Patch(ref sample, patchDocument);

        Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer)?.GetValue<string>());
    }

    [Fact]
    public void Replace_non_existant_property()
    {
        var sample = JsonNode.Parse(@"{ ""data"": {} }");

        var patchDocument = new PatchDocument();
        string pointer = "/data/author";

        patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = JsonValue.Create("Bob Brown") });

        new JsonPatcher().Patch(ref sample, patchDocument);

        Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer)?.GetValue<string>());

        sample = JsonNode.Parse("{}");

        patchDocument = new PatchDocument();
        pointer = "/data/author";

        patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = JsonValue.Create("Bob Brown") });

        new JsonPatcher().Patch(ref sample, patchDocument);

        Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer)?.GetValue<string>());

        sample = JsonNode.Parse("{}");

        patchDocument = new PatchDocument();
        pointer = "/";

        patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = JsonValue.Create("Bob Brown") });

        new JsonPatcher().Patch(ref sample, patchDocument);

        Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer)?.GetValue<string>());

        sample = JsonNode.Parse("{}");

        patchDocument = new PatchDocument();
        pointer = "/hey/now/0/you";

        patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = JsonValue.Create("Bob Brown") });

        new JsonPatcher().Patch(ref sample, patchDocument);

        Assert.Equal("{}", sample.ToJsonString());
    }

    [Fact]
    public void Replace_a_property_value_with_an_object()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/0/author";

        patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = JsonNode.Parse(@"{ ""hello"": ""world"" }") });

        new JsonPatcher().Patch(ref sample, patchDocument);

        string newPointer = "/books/0/author/hello";
        Assert.Equal("world", sample.SelectPatchToken(newPointer)?.GetValue<string>());
    }

    [Fact]
    public void SyncValuesWithRemovesAndReplaces()
    {
        const string operations = "[{\"op\":\"remove\",\"path\":\"/data/Address/full_address\"},{\"op\":\"remove\",\"path\":\"/data/Address/longitude\"},{\"op\":\"remove\",\"path\":\"/data/Address/latitude\"},{\"op\":\"remove\",\"path\":\"/data/Address/geo_locality\"},{\"op\":\"remove\",\"path\":\"/data/Address/geo_level2\"},{\"op\":\"remove\",\"path\":\"/data/Address/geo_level1\"},{\"op\":\"remove\",\"path\":\"/data/Address/geo_country\"},{\"op\":\"remove\",\"path\":\"/data/Address/normalized_geo_hash\"},{\"op\":\"remove\",\"path\":\"/data/Address/geo_hash\"},{\"op\":\"remove\",\"path\":\"/data/Address/geo\"},{\"op\":\"replace\",\"path\":\"/data/Address/country\",\"value\":\"US\"},{\"op\":\"replace\",\"path\":\"/data/Address/postal_code\",\"value\":\"54173\"},{\"op\":\"replace\",\"path\":\"/data/Address/state\",\"value\":\"Wi\"},{\"op\":\"replace\",\"path\":\"/data/Address/city\",\"value\":\"Suamico\"},{\"op\":\"remove\",\"path\":\"/data/Address/address2\"},{\"op\":\"replace\",\"path\":\"/data/Address/address1\",\"value\":\"100 Main Street\"}]";

        var patchDocument = JsonSerializer.Deserialize<PatchDocument>(operations);
        var token = JsonNode.Parse("{ \"data\": { \"Address\": { \"address1\": null, \"address2\": null, \"city\": \"e\", \"state\": null, \"postal_code\": null, \"country\": null, \"geo\": null, \"geo_hash\": null, \"normalized_geo_hash\": null, \"geo_country\": null, \"geo_level1\": null, \"geo_level2\": null, \"geo_locality\": null, \"latitude\": null, \"longitude\": null, \"full_address\": null } } }");

        new JsonPatcher().Patch(ref token, patchDocument);

        Assert.Equal("{\"data\":{\"Address\":{\"address1\":\"100 Main Street\",\"city\":\"Suamico\",\"state\":\"Wi\",\"postal_code\":\"54173\",\"country\":\"US\"}}}", token.ToJsonString());
    }

    [Fact]
    public void Test_a_value()
    {
        var sample = GetSample2();

        var patchDocument = new PatchDocument();
        string pointer = "/books/0/author";

        patchDocument.AddOperation(new TestOperation { Path = pointer, Value = JsonValue.Create("Billy Burton") });

        Assert.Throws<InvalidOperationException>(() =>
        {
            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);
        });
    }

    [Fact]
    public void Can_replace_existing_boolean()
    {
        var sample = JsonSerializer.SerializeToNode(new MyConfigClass { RequiresConfiguration = true });

        var patchDocument = new PatchDocument();
        patchDocument.AddOperation(new ReplaceOperation { Path = "/RequiresConfiguration", Value = JsonValue.Create(false) });

        var patcher = new JsonPatcher();
        patcher.Patch(ref sample, patchDocument);

        Assert.False(sample.Deserialize<MyConfigClass>().RequiresConfiguration);
    }

    public static JsonNode GetSample2()
    {
        return JsonNode.Parse(@"{
    ""books"": [
        {
          ""title"" : ""The Great Gatsby"",
          ""author"" : ""F. Scott Fitzgerald""
        },
        {
          ""title"" : ""The Grapes of Wrath"",
          ""author"" : ""John Steinbeck""
        }
    ]
}");
    }
}

public class MyConfigClass
{
    public bool RequiresConfiguration { get; set; }
}
