using System;
using System.IO;
using Foundatio.Repositories.JsonPatch;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Foundatio.Repositories.Tests.JsonPatch {
    public class JsonPatchTests {
        [Fact]
        public void Add_an_array_element() {

            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/-";

            patchDocument.AddOperation(new AddOperation { Path = pointer, Value = new JObject(new[] { new JProperty("author", "James Brown") }) });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            var list = sample["books"] as JArray;

            Assert.Equal(3, list.Count);
        }

        [Fact]
        public void Add_an_existing_member_property()  // Why isn't this replace?
        {

            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/0/title";

            patchDocument.AddOperation(new AddOperation { Path = pointer, Value = "Little Red Riding Hood" });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            string result = sample.SelectPatchToken(pointer).Value<string>();
            Assert.Equal("Little Red Riding Hood", result);
        }

        [Fact]
        public void Add_an_non_existing_member_property() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/0/SBN";

            patchDocument.AddOperation(new AddOperation { Path = pointer, Value = "213324234343" });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            string result = sample.SelectPatchToken(pointer).Value<string>();
            Assert.Equal("213324234343", result);
        }

        [Fact]
        public void Copy_array_element() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string frompointer = "/books/0";
            string topointer = "/books/-";

            patchDocument.AddOperation(new CopyOperation { FromPath = frompointer, Path = topointer });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            var result = sample.SelectPatchToken("/books/2");
            Assert.IsType<JObject>(result);
        }

        [Fact]
        public void Copy_property() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string frompointer = "/books/0/ISBN";
            string topointer = "/books/1/ISBN";

            patchDocument.AddOperation(new AddOperation { Path = frompointer, Value = new JValue("21123123") });
            patchDocument.AddOperation(new CopyOperation { FromPath = frompointer, Path = topointer });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            var result = sample.SelectPatchToken("/books/1/ISBN");
            Assert.Equal("21123123", result);
        }

        [Fact]
        public void Move_property() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string frompointer = "/books/0/author";
            string topointer = "/books/1/author";

            patchDocument.AddOperation(new MoveOperation { FromPath = frompointer, Path = topointer });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            string result = sample.SelectPatchToken(topointer).Value<string>();
            Assert.Equal("F. Scott Fitzgerald", result);
        }

        [Fact]
        public void Move_array_element() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string frompointer = "/books/1";
            string topointer = "/books/0/child";

            patchDocument.AddOperation(new MoveOperation { FromPath = frompointer, Path = topointer });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            var result = sample.SelectPatchToken(topointer);
            Assert.IsType<JObject>(result);
        }

        [Fact]
        public void CreateEmptyPatch() {
            var sample = GetSample2();
            string sampletext = sample.ToString();

            var patchDocument = new PatchDocument();
            new JsonPatcher().Patch(ref sample, patchDocument);

            Assert.Equal(sampletext, sample.ToString());
        }

        [Fact]
        public void TestExample1() {
            var targetDoc = JToken.Parse("{ 'foo': 'bar'}");
            var patchDoc = PatchDocument.Parse(@"[
                                                    { 'op': 'add', 'path': '/baz', 'value': 'qux' }
                                                ]");
            new JsonPatcher().Patch(ref targetDoc, patchDoc);

            Assert.True(JToken.DeepEquals(JToken.Parse(@"{
                                                             'foo': 'bar',
                                                             'baz': 'qux'
                                                           }"), targetDoc));
        }

        [Fact]
        public void SerializePatchDocument() {
            var patchDoc = new PatchDocument(
                new TestOperation { Path = "/a/b/c", Value = new JValue("foo") },
                new RemoveOperation { Path = "/a/b/c" },
                new AddOperation { Path = "/a/b/c", Value = new JArray(new JValue("foo"), new JValue("bar")) },
                new ReplaceOperation { Path = "/a/b/c", Value = new JValue(42) },
                new MoveOperation { FromPath = "/a/b/c", Path = "/a/b/d" },
                new CopyOperation { FromPath = "/a/b/d", Path = "/a/b/e" });

            var json = JsonConvert.SerializeObject(patchDoc);
            var roundTripped = JsonConvert.DeserializeObject<PatchDocument>(json);
            var roundTrippedJson = JsonConvert.SerializeObject(roundTripped);
            Assert.Equal(json, roundTrippedJson);

            var outputstream = patchDoc.ToStream();
            string output = new StreamReader(outputstream).ReadToEnd();

            var jOutput = JToken.Parse(output);

            Assert.Equal(@"[{""op"":""test"",""path"":""/a/b/c"",""value"":""foo""},{""op"":""remove"",""path"":""/a/b/c""},{""op"":""add"",""path"":""/a/b/c"",""value"":[""foo"",""bar""]},{""op"":""replace"",""path"":""/a/b/c"",""value"":42},{""op"":""move"",""path"":""/a/b/d"",""from"":""/a/b/c""},{""op"":""copy"",""path"":""/a/b/e"",""from"":""/a/b/d""}]",
                jOutput.ToString(Formatting.None));
        }

        [Fact]
        public void Remove_a_property() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/0/author";

            patchDocument.AddOperation(new RemoveOperation { Path = pointer });

            new JsonPatcher().Patch(ref sample, patchDocument);

            Assert.Null(sample.SelectPatchToken(pointer));
        }

        [Fact]
        public void Remove_an_array_element() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/0";

            patchDocument.AddOperation(new RemoveOperation { Path = pointer });

            var patcher = new JsonPatcher();
            patcher.Patch(ref sample, patchDocument);

            Assert.Null(sample.SelectPatchToken("/books/1"));
        }

        [Fact]
        public void Remove_an_array_element_with_numbered_custom_fields() {
            var sample = JToken.Parse(@"{
    'data': {
        '2017PropertyOne' : '2017 property one value',
        '2017PropertyTwo' : '2017 property two value',
        '2017Properties' : ['First value from 2017','Second value from 2017'],
        '2018PropertyOne' : '2018 property value',
        '2018PropertyTwo' : '2018 property two value',
        '2018Properties' : ['First value from 2018','Second value from 2018']
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
        public void Replace_a_property_value_with_a_new_value() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/0/author";

            patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = "Bob Brown" });

            new JsonPatcher().Patch(ref sample, patchDocument);

            Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer).Value<string>());
        }

        [Fact]
        public void Replace_non_existant_property() {
            var sample = JToken.Parse(@"{ ""data"": {} }");

            var patchDocument = new PatchDocument();
            string pointer = "/data/author";

            patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = "Bob Brown" });

            new JsonPatcher().Patch(ref sample, patchDocument);

            Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer).Value<string>());

            sample = JToken.Parse(@"{}");

            patchDocument = new PatchDocument();
            pointer = "/data/author";

            patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = "Bob Brown" });

            new JsonPatcher().Patch(ref sample, patchDocument);

            Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer).Value<string>());

            sample = JToken.Parse(@"{}");

            patchDocument = new PatchDocument();
            pointer = "/";

            patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = "Bob Brown" });

            new JsonPatcher().Patch(ref sample, patchDocument);

            Assert.Equal("Bob Brown", sample.SelectPatchToken(pointer).Value<string>());

            sample = JToken.Parse(@"{}");

            patchDocument = new PatchDocument();
            pointer = "/hey/now/0/you";

            patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = "Bob Brown" });

            new JsonPatcher().Patch(ref sample, patchDocument);

            Assert.Equal("{}", sample.ToString(Formatting.None));
        }

        [Fact]
        public void Replace_a_property_value_with_an_object() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/0/author";

            patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = new JObject(new[] { new JProperty("hello", "world") }) });

            new JsonPatcher().Patch(ref sample, patchDocument);

            string newPointer = "/books/0/author/hello";
            Assert.Equal("world", sample.SelectPatchToken(newPointer).Value<string>());
        }

        [Fact]
        public void Replace_multiple_property_values_with_jsonpath() {

            var sample = JToken.Parse(@"{
    'books': [
        {
          'title' : 'The Great Gatsby',
          'author' : 'F. Scott Fitzgerald'
        },
        {
          'title' : 'The Grapes of Wrath',
          'author' : 'John Steinbeck'
        },
        {
          'title' : 'Some Other Title',
          'author' : 'John Steinbeck'
        }
    ]
}");

            var patchDocument = new PatchDocument();
            string pointer = "$.books[?(@.author == 'John Steinbeck')].author";

            patchDocument.AddOperation(new ReplaceOperation { Path = pointer, Value = "Eric" });

            new JsonPatcher().Patch(ref sample, patchDocument);

            string newPointer = "/books/1/author";
            Assert.Equal("Eric", sample.SelectPatchToken(newPointer).Value<string>());

            newPointer = "/books/2/author";
            Assert.Equal("Eric", sample.SelectPatchToken(newPointer).Value<string>());
        }

        [Fact]
        public void Test_a_value() {
            var sample = GetSample2();

            var patchDocument = new PatchDocument();
            string pointer = "/books/0/author";

            patchDocument.AddOperation(new TestOperation { Path = pointer, Value = new JValue("Billy Burton") });

            Assert.Throws<InvalidOperationException>(() => {
                var patcher = new JsonPatcher();
                patcher.Patch(ref sample, patchDocument);
            });
        }

        public static JToken GetSample2() {
            return JToken.Parse(@"{
    'books': [
        {
          'title' : 'The Great Gatsby',
          'author' : 'F. Scott Fitzgerald'
        },
        {
          'title' : 'The Grapes of Wrath',
          'author' : 'John Steinbeck'
        }
    ]
}");
        }
    }
}
