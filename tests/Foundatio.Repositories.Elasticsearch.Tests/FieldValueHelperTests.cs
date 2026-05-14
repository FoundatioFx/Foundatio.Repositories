using System;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public class FieldValueHelperTests
{
    [Fact]
    public void ToFieldValue_WithJsonStringEnumMemberName_UsesAttributeValue()
    {
        var result = FieldValueHelper.ToFieldValue(TestEnumWithJsonName.Active);
        Assert.Equal(FieldValue.String("active"), result);
    }

    [Fact]
    public void ToFieldValue_WithEnumMember_UsesAttributeValue()
    {
        var result = FieldValueHelper.ToFieldValue(TestEnumWithEnumMember.InProgress);
        Assert.Equal(FieldValue.String("in-progress"), result);
    }

    [Fact]
    public void ToFieldValue_WithBothAttributes_PrefersJsonStringEnumMemberName()
    {
        var result = FieldValueHelper.ToFieldValue(TestEnumWithBothAttributes.Special);
        Assert.Equal(FieldValue.String("json-special"), result);
    }

    [Fact]
    public void ToFieldValue_WithNoAttribute_UsesToString()
    {
        var result = FieldValueHelper.ToFieldValue(TestEnumPlain.Pending);
        Assert.Equal(FieldValue.String("Pending"), result);
    }

    [Fact]
    public void ToFieldValue_WithNull_ReturnsNull()
    {
        var result = FieldValueHelper.ToFieldValue(null);
        Assert.Equal(FieldValue.Null, result);
    }

    [Fact]
    public void ToFieldValue_WithString_ReturnsString()
    {
        var result = FieldValueHelper.ToFieldValue("hello");
        Assert.Equal(FieldValue.String("hello"), result);
    }

    [Fact]
    public void ToFieldValue_WithBoolTrue_ReturnsTrue()
    {
        var result = FieldValueHelper.ToFieldValue(true);
        Assert.Equal(FieldValue.Boolean(true), result);
    }

    [Fact]
    public void ToFieldValue_WithBoolFalse_ReturnsFalse()
    {
        var result = FieldValueHelper.ToFieldValue(false);
        Assert.Equal(FieldValue.Boolean(false), result);
    }

    [Fact]
    public void ToFieldValue_WithInt_ReturnsLong()
    {
        var result = FieldValueHelper.ToFieldValue(42);
        Assert.Equal(FieldValue.Long(42), result);
    }

    [Fact]
    public void ToFieldValue_WithLong_ReturnsLong()
    {
        var result = FieldValueHelper.ToFieldValue(123456789L);
        Assert.Equal(FieldValue.Long(123456789L), result);
    }

    [Fact]
    public void ToFieldValue_WithDouble_ReturnsDouble()
    {
        var result = FieldValueHelper.ToFieldValue(3.14);
        Assert.Equal(FieldValue.Double(3.14), result);
    }

    [Fact]
    public void ToFieldValue_WithFloat_ReturnsDouble()
    {
        var result = FieldValueHelper.ToFieldValue(2.5f);
        Assert.Equal(FieldValue.Double(2.5f), result);
    }

    [Fact]
    public void ToFieldValue_WithDecimal_ReturnsDouble()
    {
        var result = FieldValueHelper.ToFieldValue(99.99m);
        Assert.Equal(FieldValue.Double(99.99), result);
    }

    [Fact]
    public void ToFieldValue_WithDateTime_ReturnsRoundTripString()
    {
        var dt = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var result = FieldValueHelper.ToFieldValue(dt);
        Assert.Equal(FieldValue.String(dt.ToString("o")), result);
    }

    [Fact]
    public void ToFieldValue_WithDateTimeOffset_ReturnsRoundTripString()
    {
        var dto = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.FromHours(-5));
        var result = FieldValueHelper.ToFieldValue(dto);
        Assert.Equal(FieldValue.String(dto.ToString("o")), result);
    }

    private enum TestEnumWithJsonName
    {
        [JsonStringEnumMemberName("active")]
        Active,
        [JsonStringEnumMemberName("inactive")]
        Inactive
    }

    private enum TestEnumWithEnumMember
    {
        [EnumMember(Value = "in-progress")]
        InProgress,
        [EnumMember(Value = "completed")]
        Completed
    }

    private enum TestEnumWithBothAttributes
    {
        [JsonStringEnumMemberName("json-special")]
        [EnumMember(Value = "enum-special")]
        Special
    }

    private enum TestEnumPlain
    {
        Pending,
        Done
    }
}
