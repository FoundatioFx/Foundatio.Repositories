using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexScriptTests
{
    [Fact]
    public void RenameFieldScript_WithTopLevelField_ProducesCorrectCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "companyName", "companyNameRenamed");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- produces 2 scripts (copy + remove), wrapped in functions
        Assert.Contains("void f000(def ctx) { if (ctx._source.containsKey('companyName')) { ctx._source.companyNameRenamed = ctx._source.companyName; } }", result);
        Assert.Contains("void f001(def ctx) { if (ctx._source.containsKey('companyName')) { ctx._source.remove('companyName'); } }", result);
        Assert.Contains("f000(ctx); f001(ctx);", result);
    }

    [Fact]
    public void RenameFieldScript_WithTopLevelFieldNoRemove_ProducesOnlyCopyScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "oldName", "newName", remove: false);

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- single script returned directly (no function wrapping)
        Assert.Equal(
            "if (ctx._source.containsKey('oldName')) { ctx._source.newName = ctx._source.oldName; }",
            result);
    }

    [Fact]
    public void RenameFieldScript_WithNestedField_ProducesNullSafeCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "data.oldField", "data.newField");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- copy script with null-safety + remove script
        Assert.Contains(
            "void f000(def ctx) { if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { if (ctx._source.data == null) { ctx._source.data = [:]; } ctx._source.data.newField = ctx._source.data.oldField; } }",
            result);
        Assert.Contains(
            "void f001(def ctx) { if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { ctx._source.data.remove('oldField'); } }",
            result);
        Assert.Contains("f000(ctx); f001(ctx);", result);
    }

    [Fact]
    public void RenameFieldScript_WithNestedFieldNoRemove_ProducesOnlyCopyScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "data.oldField", "data.newField", remove: false);

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- single copy script with null-safety guards
        Assert.Equal(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { if (ctx._source.data == null) { ctx._source.data = [:]; } ctx._source.data.newField = ctx._source.data.oldField; }",
            result);
    }

    [Fact]
    public void RenameFieldScript_WithDeeplyNestedField_ProducesChainedNullSafeScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "metadata.author.name", "metadata.author.displayName");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- copy script with chained null-safety for each parent level
        Assert.Contains(
            "if (ctx._source.metadata != null && ctx._source.metadata.author != null && ctx._source.metadata.author.containsKey('name')) { "
            + "if (ctx._source.metadata == null) { ctx._source.metadata = [:]; } "
            + "if (ctx._source.metadata.author == null) { ctx._source.metadata.author = [:]; } "
            + "ctx._source.metadata.author.displayName = ctx._source.metadata.author.name; }",
            result);

        // Assert -- remove script with chained null checks
        Assert.Contains(
            "if (ctx._source.metadata != null && ctx._source.metadata.author != null && ctx._source.metadata.author.containsKey('name')) { "
            + "ctx._source.metadata.author.remove('name'); }",
            result);
    }

    [Fact]
    public void RenameFieldScript_WithDifferentParents_ProducesCorrectCompiledScript()
    {
        // Arrange -- move field from one parent to a different parent
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "data.oldField", "meta.newField");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- copy guards data.oldField, assigns to meta.newField
        Assert.Contains(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { "
            + "if (ctx._source.meta == null) { ctx._source.meta = [:]; } "
            + "ctx._source.meta.newField = ctx._source.data.oldField; }",
            result);

        // Assert -- remove targets data.oldField
        Assert.Contains(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { ctx._source.data.remove('oldField'); }",
            result);
    }

    [Fact]
    public void RenameFieldScript_FromNestedToTopLevel_ProducesCorrectCompiledScript()
    {
        // Arrange -- promote a nested field to top-level
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "data.oldField", "promoted");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- guard checks nested, assignment is top-level (no parent init needed)
        Assert.Contains(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { ctx._source.promoted = ctx._source.data.oldField; }",
            result);

        // Assert -- remove targets nested
        Assert.Contains(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { ctx._source.data.remove('oldField'); }",
            result);
    }

    [Fact]
    public void RenameFieldScript_FromTopLevelToNested_ProducesCorrectCompiledScript()
    {
        // Arrange -- demote a top-level field into a nested object
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "companyName", "data.company");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- guard checks top-level, assignment creates nested parent
        Assert.Contains(
            "if (ctx._source.containsKey('companyName')) { if (ctx._source.data == null) { ctx._source.data = [:]; } ctx._source.data.company = ctx._source.companyName; }",
            result);

        // Assert -- remove targets top-level
        Assert.Contains(
            "if (ctx._source.containsKey('companyName')) { ctx._source.remove('companyName'); }",
            result);
    }

    [Fact]
    public void RemoveFieldScript_WithTopLevelField_ProducesCorrectCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRemoveFieldScript(2, "companyName");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- single script, returned directly
        Assert.Equal(
            "if (ctx._source.containsKey('companyName')) { ctx._source.remove('companyName'); }",
            result);
    }

    [Fact]
    public void RemoveFieldScript_WithNestedField_ProducesNullSafeCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRemoveFieldScript(2, "data.oldField");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- null guard on parent, parent-scoped remove
        Assert.Equal(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { ctx._source.data.remove('oldField'); }",
            result);
    }

    [Fact]
    public void RemoveFieldScript_WithDeeplyNestedField_ProducesChainedNullSafeScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRemoveFieldScript(2, "metadata.author.name");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- chained null checks, parent-scoped remove
        Assert.Equal(
            "if (ctx._source.metadata != null && ctx._source.metadata.author != null && ctx._source.metadata.author.containsKey('name')) { "
            + "ctx._source.metadata.author.remove('name'); }",
            result);
    }

    [Fact]
    public void GetReindexScripts_WithNoScripts_ReturnsNull()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);

        // Act
        string result = index.TestGetReindexScripts(0);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetReindexScripts_WithSingleScript_ReturnsScriptDirectly()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestAddReindexScript(2, "ctx._source.name = 'test';");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert
        Assert.Equal("ctx._source.name = 'test';", result);
    }

    [Fact]
    public void GetReindexScripts_WithMultipleScripts_WrapsInFunctions()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestAddReindexScript(2, "ctx._source.a = 1;");
        index.TestAddReindexScript(3, "ctx._source.b = 2;");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert
        Assert.Contains("void f000(def ctx) { ctx._source.a = 1; }", result);
        Assert.Contains("void f001(def ctx) { ctx._source.b = 2; }", result);
        Assert.Contains("f000(ctx); f001(ctx);", result);
    }

    [Fact]
    public void GetReindexScripts_FiltersScriptsByVersion_IncludesOnlyRelevantVersions()
    {
        // Arrange
        var index = new TestableVersionedIndex(3);
        index.TestAddReindexScript(1, "ctx._source.v1 = true;");
        index.TestAddReindexScript(2, "ctx._source.v2 = true;");
        index.TestAddReindexScript(3, "ctx._source.v3 = true;");
        index.TestAddReindexScript(4, "ctx._source.v4 = true;");

        // Act -- upgrading from v1, index version is 3, so only v2 and v3 scripts apply
        string result = index.TestGetReindexScripts(1);

        // Assert
        Assert.DoesNotContain("v1", result);
        Assert.Contains("v2", result);
        Assert.Contains("v3", result);
        Assert.DoesNotContain("v4", result);
    }

    [Fact]
    public void GetReindexScripts_WithRenameAndRemoveAtSameVersion_IncludesBothScripts()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "oldName", "newName");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- contains both the copy and the remove
        Assert.Contains("newName", result);
        Assert.Contains("remove('oldName')", result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void RenameFieldScript_WithNullOrEmptyOriginalName_ThrowsArgumentException(string originalName)
    {
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.ThrowsAny<ArgumentException>(() => index.TestRenameFieldScript(2, originalName, "newName"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void RenameFieldScript_WithNullOrEmptyCurrentName_ThrowsArgumentException(string currentName)
    {
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.ThrowsAny<ArgumentException>(() => index.TestRenameFieldScript(2, "oldName", currentName));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void RemoveFieldScript_WithNullOrEmptyFieldName_ThrowsArgumentException(string fieldName)
    {
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.ThrowsAny<ArgumentException>(() => index.TestRemoveFieldScript(2, fieldName));
    }

    [Theory]
    [InlineData(".field")]
    [InlineData("field.")]
    [InlineData("a..b")]
    [InlineData("a.b..c")]
    public void RenameFieldScript_WithInvalidDotNotation_ThrowsArgumentException(string fieldPath)
    {
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.Throws<ArgumentException>(() => index.TestRenameFieldScript(2, fieldPath, "valid"));
    }

    [Theory]
    [InlineData("field'name")]
    [InlineData("field\\name")]
    public void RenameFieldScript_WithScriptInjectionCharacters_ThrowsArgumentException(string fieldPath)
    {
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.Throws<ArgumentException>(() => index.TestRenameFieldScript(2, fieldPath, "valid"));
    }

    [Theory]
    [InlineData(".field")]
    [InlineData("field.")]
    [InlineData("a..b")]
    public void RemoveFieldScript_WithInvalidDotNotation_ThrowsArgumentException(string fieldPath)
    {
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.Throws<ArgumentException>(() => index.TestRemoveFieldScript(2, fieldPath));
    }

    [Theory]
    [InlineData("field'name")]
    [InlineData("field\\name")]
    public void RemoveFieldScript_WithScriptInjectionCharacters_ThrowsArgumentException(string fieldPath)
    {
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.Throws<ArgumentException>(() => index.TestRemoveFieldScript(2, fieldPath));
    }

    [Theory]
    [InlineData("field'name")]
    [InlineData("field\\name")]
    public void RenameFieldScript_WithScriptInjectionInCurrentName_ThrowsArgumentException(string currentName)
    {
        var index = new TestableVersionedIndex(2);

        Assert.Throws<ArgumentException>(() => index.TestRenameFieldScript(2, "validField", currentName));
    }

    [Fact]
    public void RenameFieldScript_WithSameSourceAndTarget_ThrowsArgumentException()
    {
        var index = new TestableVersionedIndex(2);

        Assert.Throws<ArgumentException>(() => index.TestRenameFieldScript(2, "fieldName", "fieldName"));
    }

    [Fact]
    public void RenameFieldScript_WithSameNestedSourceAndTarget_ThrowsArgumentException()
    {
        var index = new TestableVersionedIndex(2);

        Assert.Throws<ArgumentException>(() => index.TestRenameFieldScript(2, "data.field", "data.field"));
    }

    [Fact]
    public void RenameFieldScript_WithSingleCharacterFields_ProducesCorrectScript()
    {
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "a", "b");

        string result = index.TestGetReindexScripts(1);

        Assert.Contains("containsKey('a')", result);
        Assert.Contains("ctx._source.b = ctx._source.a;", result);
        Assert.Contains("remove('a')", result);
    }

    [Fact]
    public void RenameFieldScript_WithDeeplyNestedFiveLevels_ProducesCorrectScript()
    {
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "a.b.c.d.e", "a.b.c.d.f", remove: false);

        string result = index.TestGetReindexScripts(1);

        Assert.Contains("ctx._source.a != null", result);
        Assert.Contains("ctx._source.a.b != null", result);
        Assert.Contains("ctx._source.a.b.c != null", result);
        Assert.Contains("ctx._source.a.b.c.d != null", result);
        Assert.Contains("ctx._source.a.b.c.d.containsKey('e')", result);
        Assert.Contains("ctx._source.a.b.c.d.f = ctx._source.a.b.c.d.e;", result);
    }

    [Fact]
    public void GetReindexScripts_WithMultiVersionRenameAndRemove_CombinesAllScripts()
    {
        var index = new TestableVersionedIndex(4);
        index.TestRenameFieldScript(2, "oldName", "newName");
        index.TestRemoveFieldScript(3, "data.legacy");
        index.TestAddReindexScript(4, "ctx._source.migrated = true;");

        string result = index.TestGetReindexScripts(1);

        Assert.Contains("newName", result);
        Assert.Contains("remove('oldName')", result);
        Assert.Contains("data.remove('legacy')", result);
        Assert.Contains("ctx._source.migrated = true;", result);
        Assert.Contains("f000(ctx);", result);
        Assert.Contains("f001(ctx);", result);
        Assert.Contains("f002(ctx);", result);
        Assert.Contains("f003(ctx);", result);
    }

    [Theory]
    [InlineData("field with space")]
    [InlineData("field;name")]
    [InlineData("field(name)")]
    [InlineData("@timestamp")]
    [InlineData("field-name")]
    [InlineData("123field")]
    public void RenameFieldScript_WithNonIdentifierCharacters_ThrowsArgumentException(string fieldPath)
    {
        var index = new TestableVersionedIndex(2);

        Assert.Throws<ArgumentException>(() => index.TestRenameFieldScript(2, fieldPath, "valid"));
    }

    private sealed class TestableVersionedIndex : VersionedIndex
    {
        public TestableVersionedIndex(int version)
            : base(new ElasticConfiguration(), "test", version) { }

        public void TestRenameFieldScript(int v, string from, string to, bool remove = true)
            => RenameFieldScript(v, from, to, remove);

        public void TestRemoveFieldScript(int v, string field)
            => RemoveFieldScript(v, field);

        public string TestGetReindexScripts(int currentVersion)
            => GetReindexScripts(currentVersion);

        public void TestAddReindexScript(int v, string script)
            => AddReindexScript(v, script);
    }
}
