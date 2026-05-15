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
    public void ToFieldValue_WithEnumMember_MultipleMembers_AllResolveCorrectly()
    {
        Assert.Equal(FieldValue.String("in-progress"), FieldValueHelper.ToFieldValue(TestEnumWithEnumMember.InProgress));
        Assert.Equal(FieldValue.String("completed"), FieldValueHelper.ToFieldValue(TestEnumWithEnumMember.Completed));
    }

    [Fact]
    public void ToFieldValue_WithFlagsEnum_SingleValue_UsesAttribute()
    {
        var result = FieldValueHelper.ToFieldValue(TestFlagsEnum.Read);
        Assert.Equal(FieldValue.String("read"), result);
    }

    [Fact]
    public void ToFieldValue_WithFlagsEnum_CombinedValue_FallsBackToString()
    {
        var combined = TestFlagsEnum.Read | TestFlagsEnum.Write;
        var result = FieldValueHelper.ToFieldValue(combined);
        Assert.Equal(FieldValue.String("Read, Write"), result);
    }

    [Fact]
    public void ToFieldValue_WithUndefinedEnumValue_FallsBackToString()
    {
        var undefined = (TestEnumPlain)999;
        var result = FieldValueHelper.ToFieldValue(undefined);
        Assert.Equal(FieldValue.String("999"), result);
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
    public void ToFieldValue_WithEmptyString_ReturnsEmptyString()
    {
        var result = FieldValueHelper.ToFieldValue("");
        Assert.Equal(FieldValue.String(""), result);
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
    public void ToFieldValue_WithShort_ReturnsLong()
    {
        var result = FieldValueHelper.ToFieldValue((short)7);
        Assert.Equal(FieldValue.Long(7), result);
    }

    [Fact]
    public void ToFieldValue_WithByte_ReturnsLong()
    {
        var result = FieldValueHelper.ToFieldValue((byte)255);
        Assert.Equal(FieldValue.Long(255), result);
    }

    [Fact]
    public void ToFieldValue_WithULongWithinLongRange_ReturnsLong()
    {
        var result = FieldValueHelper.ToFieldValue((ulong)100);
        Assert.Equal(FieldValue.Long(100), result);
    }

    [Fact]
    public void ToFieldValue_WithULongExceedingLongMax_ReturnsDouble()
    {
        var result = FieldValueHelper.ToFieldValue(ulong.MaxValue);
        Assert.Equal(FieldValue.Double((double)ulong.MaxValue), result);
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

    [Fact]
    public void ToFieldValue_WithGuid_ReturnsString()
    {
        var guid = Guid.Parse("12345678-1234-1234-1234-123456789012");
        var result = FieldValueHelper.ToFieldValue(guid);
        Assert.Equal(FieldValue.String("12345678-1234-1234-1234-123456789012"), result);
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

    [Flags]
    private enum TestFlagsEnum
    {
        None = 0,
        [JsonStringEnumMemberName("read")]
        Read = 1,
        [JsonStringEnumMemberName("write")]
        Write = 2,
        [JsonStringEnumMemberName("execute")]
        Execute = 4
    }
}
