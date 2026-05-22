using System;
using System.Buffers;
using System.Text.Json;

namespace Foundatio.Repositories.Serialization;

/// <summary>
/// Provides consistent number type inference from JSON tokens, shared between
/// <see cref="ObjectToInferredTypesConverter"/> and any code that needs to convert
/// a <see cref="JsonElement"/> number to an appropriate CLR type.
/// </summary>
/// <remarks>
/// <para>Rules:</para>
/// <list type="bullet">
///   <item>If the raw bytes contain '.', 'e', or 'E' → <see cref="double"/></item>
///   <item>If the value fits in <see cref="long"/> → <see cref="long"/></item>
///   <item>Otherwise → <see cref="double"/></item>
/// </list>
/// <para>
/// This avoids the <see cref="OverflowException"/> that occurs when calling
/// <see cref="JsonElement.GetDouble"/> on values like <c>1e999</c> by catching
/// and returning <see cref="double.PositiveInfinity"/> / <see cref="double.NegativeInfinity"/>.
/// </para>
/// </remarks>
public static class JsonNumberInference
{
    /// <summary>
    /// Reads a number from a <see cref="Utf8JsonReader"/> positioned on a <see cref="JsonTokenType.Number"/> token.
    /// </summary>
    public static object ReadNumber(ref Utf8JsonReader reader)
    {
        ReadOnlySpan<byte> rawValue = reader.HasValueSequence
            ? reader.ValueSequence.ToArray()
            : reader.ValueSpan;

        if (rawValue.Contains((byte)'.') || rawValue.Contains((byte)'e') || rawValue.Contains((byte)'E'))
        {
            try
            {
                return reader.GetDouble();
            }
            catch (FormatException)
            {
                return double.PositiveInfinity;
            }
        }

        if (reader.TryGetInt64(out long l))
            return l;

        return reader.GetDouble();
    }

    /// <summary>
    /// Infers the CLR numeric type from a <see cref="JsonElement"/> with <see cref="JsonValueKind.Number"/>.
    /// </summary>
    public static object ReadNumber(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Number)
            throw new ArgumentException("JsonElement must be of kind Number.", nameof(element));

        string raw = element.GetRawText();

        if (raw.Contains('.') || raw.Contains('e') || raw.Contains('E'))
        {
            if (element.TryGetDouble(out double d))
                return d;

            return double.PositiveInfinity;
        }

        if (element.TryGetInt64(out long l))
            return l;

        if (element.TryGetDouble(out double fallback))
            return fallback;

        return double.PositiveInfinity;
    }
}
