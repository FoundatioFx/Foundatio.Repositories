using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexScriptTests
{
    [Fact]
    public void BuildContainsKeyGuard_WithTopLevelField_ReturnsSimpleContainsKey()
    {
        // Arrange
        string fieldPath = "companyName";

        // Act
        string result = VersionedIndex.BuildContainsKeyGuard(fieldPath);

        // Assert
        Assert.Equal("ctx._source.containsKey('companyName')", result);
    }

    [Fact]
    public void BuildContainsKeyGuard_WithNestedField_ReturnsNullCheckAndContainsKey()
    {
        // Arrange
        string fieldPath = "data.oldField";

        // Act
        string result = VersionedIndex.BuildContainsKeyGuard(fieldPath);

        // Assert
        Assert.Equal("ctx._source.data != null && ctx._source.data.containsKey('oldField')", result);
    }

    [Fact]
    public void BuildContainsKeyGuard_WithDeeplyNestedField_ReturnsChainedNullChecks()
    {
        // Arrange
        string fieldPath = "a.b.c";

        // Act
        string result = VersionedIndex.BuildContainsKeyGuard(fieldPath);

        // Assert
        Assert.Equal("ctx._source.a != null && ctx._source.a.b != null && ctx._source.a.b.containsKey('c')", result);
    }

    [Fact]
    public void BuildContainsKeyGuard_WithThreeLevelNesting_ReturnsAllNullChecks()
    {
        // Arrange
        string fieldPath = "metadata.author.name";

        // Act
        string result = VersionedIndex.BuildContainsKeyGuard(fieldPath);

        // Assert
        Assert.Equal(
            "ctx._source.metadata != null && ctx._source.metadata.author != null && ctx._source.metadata.author.containsKey('name')",
            result);
    }

    [Fact]
    public void BuildFieldAccessor_WithTopLevelField_ReturnsDotPath()
    {
        // Arrange / Act
        string result = VersionedIndex.BuildFieldAccessor("companyName");

        // Assert
        Assert.Equal("ctx._source.companyName", result);
    }

    [Fact]
    public void BuildFieldAccessor_WithNestedField_ReturnsDotPath()
    {
        // Arrange / Act
        string result = VersionedIndex.BuildFieldAccessor("data.oldField");

        // Assert
        Assert.Equal("ctx._source.data.oldField", result);
    }

    [Fact]
    public void BuildFieldAssignment_WithTopLevelField_ReturnsDirectAssignment()
    {
        // Arrange
        string targetPath = "newField";
        string value = "ctx._source.oldField";

        // Act
        string result = VersionedIndex.BuildFieldAssignment(targetPath, value);

        // Assert
        Assert.Equal("ctx._source.newField = ctx._source.oldField;", result);
    }

    [Fact]
    public void BuildFieldAssignment_WithNestedField_ReturnsNullSafeAssignment()
    {
        // Arrange
        string targetPath = "data.newField";
        string value = "ctx._source.data.oldField";

        // Act
        string result = VersionedIndex.BuildFieldAssignment(targetPath, value);

        // Assert
        Assert.Equal(
            "if (ctx._source.data == null) { ctx._source.data = [:]; } ctx._source.data.newField = ctx._source.data.oldField;",
            result);
    }

    [Fact]
    public void BuildFieldAssignment_WithDeeplyNestedField_ReturnsChainedNullSafeAssignment()
    {
        // Arrange
        string targetPath = "a.b.d";
        string value = "ctx._source.a.b.c";

        // Act
        string result = VersionedIndex.BuildFieldAssignment(targetPath, value);

        // Assert
        Assert.Equal(
            "if (ctx._source.a == null) { ctx._source.a = [:]; } if (ctx._source.a.b == null) { ctx._source.a.b = [:]; } ctx._source.a.b.d = ctx._source.a.b.c;",
            result);
    }

    [Fact]
    public void BuildFieldRemoval_WithTopLevelField_ReturnsSourceRemove()
    {
        // Arrange / Act
        string result = VersionedIndex.BuildFieldRemoval("companyName");

        // Assert
        Assert.Equal("ctx._source.remove('companyName');", result);
    }

    [Fact]
    public void BuildFieldRemoval_WithNestedField_ReturnsParentRemove()
    {
        // Arrange / Act
        string result = VersionedIndex.BuildFieldRemoval("data.oldField");

        // Assert
        Assert.Equal("ctx._source.data.remove('oldField');", result);
    }

    [Fact]
    public void BuildFieldRemoval_WithDeeplyNestedField_ReturnsParentRemove()
    {
        // Arrange / Act
        string result = VersionedIndex.BuildFieldRemoval("a.b.c");

        // Assert
        Assert.Equal("ctx._source.a.b.remove('c');", result);
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

        // Act -- upgrading from v1, index version is 3, so scripts for v2 and v3 should be included
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

        // Assert -- should contain both the copy and the remove script
        Assert.Contains("newName", result);
        Assert.Contains("remove('oldName')", result);
    }

    #region Compiled Script Verification -- Top-Level Fields

    [Fact]
    public void RenameFieldScript_WithTopLevelField_ProducesCorrectCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "companyName", "companyNameRenamed");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- RenameFieldScript with removeOriginal:true produces 2 scripts (copy + remove)
        Assert.Contains("void f000(def ctx) { if (ctx._source.containsKey('companyName')) { ctx._source.companyNameRenamed = ctx._source.companyName; } }", result);
        Assert.Contains("void f001(def ctx) { if (ctx._source.containsKey('companyName')) { ctx._source.remove('companyName'); } }", result);
        Assert.Contains("f000(ctx); f001(ctx);", result);
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
    public void RenameFieldScript_WithTopLevelFieldNoRemove_ProducesOnlyCopyScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "oldName", "newName", remove: false);

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- single script (no remove), returned directly
        Assert.Equal(
            "if (ctx._source.containsKey('oldName')) { ctx._source.newName = ctx._source.oldName; }",
            result);
    }

    #endregion

    #region Compiled Script Verification -- Nested Fields

    [Fact]
    public void RenameFieldScript_WithNestedField_ProducesCorrectCompiledScript()
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
    public void RemoveFieldScript_WithNestedField_ProducesCorrectCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRemoveFieldScript(2, "data.oldField");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- single script, returned directly
        Assert.Equal(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { ctx._source.data.remove('oldField'); }",
            result);
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

    #endregion

    #region Compiled Script Verification -- Deeply Nested Fields

    [Fact]
    public void RenameFieldScript_WithDeeplyNestedField_ProducesCorrectCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "metadata.author.name", "metadata.author.displayName");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- copy script with chained null-safety
        Assert.Contains(
            "if (ctx._source.metadata != null && ctx._source.metadata.author != null && ctx._source.metadata.author.containsKey('name')) { "
            + "if (ctx._source.metadata == null) { ctx._source.metadata = [:]; } "
            + "if (ctx._source.metadata.author == null) { ctx._source.metadata.author = [:]; } "
            + "ctx._source.metadata.author.displayName = ctx._source.metadata.author.name; }",
            result);

        // Assert -- remove script
        Assert.Contains(
            "if (ctx._source.metadata != null && ctx._source.metadata.author != null && ctx._source.metadata.author.containsKey('name')) { "
            + "ctx._source.metadata.author.remove('name'); }",
            result);
    }

    [Fact]
    public void RemoveFieldScript_WithDeeplyNestedField_ProducesCorrectCompiledScript()
    {
        // Arrange
        var index = new TestableVersionedIndex(5);
        index.TestRemoveFieldScript(2, "metadata.author.name");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert
        Assert.Equal(
            "if (ctx._source.metadata != null && ctx._source.metadata.author != null && ctx._source.metadata.author.containsKey('name')) { "
            + "ctx._source.metadata.author.remove('name'); }",
            result);
    }

    #endregion

    #region Compiled Script Verification -- Cross-Parent Rename

    [Fact]
    public void RenameFieldScript_WithDifferentParents_ProducesCorrectCompiledScript()
    {
        // Arrange -- move field from one parent to a different parent
        var index = new TestableVersionedIndex(5);
        index.TestRenameFieldScript(2, "data.oldField", "meta.newField");

        // Act
        string result = index.TestGetReindexScripts(1);

        // Assert -- copy script guards data.oldField but assigns to meta.newField
        Assert.Contains(
            "if (ctx._source.data != null && ctx._source.data.containsKey('oldField')) { "
            + "if (ctx._source.meta == null) { ctx._source.meta = [:]; } "
            + "ctx._source.meta.newField = ctx._source.data.oldField; }",
            result);

        // Assert -- remove script targets data.oldField
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

        // Assert -- guard checks nested, assignment is top-level
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

    #endregion

    #region Validation -- RemoveFieldScript Edge Cases

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
        // Arrange
        var index = new TestableVersionedIndex(2);

        // Act / Assert
        Assert.Throws<ArgumentException>(() => index.TestRenameFieldScript(2, "validField", currentName));
    }

    #endregion

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
