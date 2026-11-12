using System;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// An Elasticsearch optimistic concurrency token made of the shard primary term and sequence number
/// (<c>_primary_term</c> and <c>_seq_no</c>). Its string form is <c>"{primaryTerm}:{sequenceNumber}"</c>.
/// </summary>
/// <remarks>
/// A version is empty when the primary term is not positive or the sequence number is negative. That covers
/// a missing version, the <c>0:0</c> default and the <c>_seq_no: -2, _primary_term: 0</c> sentinel that
/// Elasticsearch returns for indexes with sequence numbers disabled. All empty versions are equal to each other,
/// sort before every non-empty version, and are never sent as <c>if_seq_no</c>/<c>if_primary_term</c>.
/// </remarks>
public readonly struct ElasticDocumentVersion : IEquatable<ElasticDocumentVersion>, IComparable<ElasticDocumentVersion>, IComparable
{
    /// <summary>The empty version (<c>0:0</c>).</summary>
    public static readonly ElasticDocumentVersion Empty = new();

    public ElasticDocumentVersion(long primaryTerm, long sequenceNumber)
    {
        PrimaryTerm = primaryTerm;
        SequenceNumber = sequenceNumber;
    }

    public long PrimaryTerm { get; }
    public long SequenceNumber { get; }

    /// <summary>
    /// <c>true</c> when the primary term is not positive or the sequence number is negative (unassigned or disabled).
    /// </summary>
    public bool IsEmpty => PrimaryTerm <= 0 || SequenceNumber < 0;

    public override string ToString() => $"{PrimaryTerm}:{SequenceNumber}";

    /// <summary>
    /// Returns the <c>"{primaryTerm}:{sequenceNumber}"</c> string, or <c>null</c> when the version is empty.
    /// This is the value assigned to <see cref="IVersioned.Version"/>.
    /// </summary>
    public string? ToVersionString() => IsEmpty ? null : ToString();

    public override bool Equals(object? obj)
    {
        return obj switch
        {
            ElasticDocumentVersion version => Equals(version),
            string stringVersion => Equals(Parse(stringVersion)),
            IVersioned versioned => Equals(versioned.GetElasticVersion()),
            _ => false
        };
    }

    public override int GetHashCode()
    {
        return IsEmpty ? 0 : HashCode.Combine(PrimaryTerm, SequenceNumber);
    }

    public bool Equals(ElasticDocumentVersion v)
    {
        if (IsEmpty || v.IsEmpty)
            return IsEmpty && v.IsEmpty;

        return PrimaryTerm == v.PrimaryTerm && SequenceNumber == v.SequenceNumber;
    }

    public static bool operator ==(ElasticDocumentVersion lhs, ElasticDocumentVersion rhs)
    {
        return lhs.Equals(rhs);
    }

    public static bool operator !=(ElasticDocumentVersion lhs, ElasticDocumentVersion rhs)
    {
        return !lhs.Equals(rhs);
    }

    /// <summary>
    /// Parses a <c>"{primaryTerm}:{sequenceNumber}"</c> string. Returns <see cref="Empty"/> for <c>null</c>,
    /// malformed values and empty or sentinel values such as <c>0:0</c> or <c>0:-2</c>.
    /// </summary>
    public static ElasticDocumentVersion Parse(string? version)
    {
        if (String.IsNullOrEmpty(version))
            return Empty;

        string[] parts = version.Split(':');
        if (parts.Length is not 2)
            return Empty;

        if (!Int64.TryParse(parts[0], out long primaryTerm) || !Int64.TryParse(parts[1], out long sequenceNumber))
            return Empty;

        var parsed = new ElasticDocumentVersion(primaryTerm, sequenceNumber);
        return parsed.IsEmpty ? Empty : parsed;
    }

    public static implicit operator ElasticDocumentVersion(string version) => Parse(version);
    public static implicit operator string(ElasticDocumentVersion version) => version.ToString();

    public int CompareTo(ElasticDocumentVersion other)
    {
        if (IsEmpty || other.IsEmpty)
            return IsEmpty.CompareTo(other.IsEmpty) * -1;

        int result = PrimaryTerm.CompareTo(other.PrimaryTerm);
        if (result is not 0)
            return result;

        return SequenceNumber.CompareTo(other.SequenceNumber);
    }

    public int CompareTo(object? obj)
    {
        return obj switch
        {
            null => 1,
            ElasticDocumentVersion version => CompareTo(version),
            string stringVersion => CompareTo(Parse(stringVersion)),
            IVersioned versioned => CompareTo(versioned.GetElasticVersion()),
            _ => throw new ArgumentException($"Object must be of type {nameof(ElasticDocumentVersion)}")
        };
    }

    public static bool operator <(ElasticDocumentVersion left, ElasticDocumentVersion right)
    {
        return left.CompareTo(right) < 0;
    }

    public static bool operator >(ElasticDocumentVersion left, ElasticDocumentVersion right)
    {
        return left.CompareTo(right) > 0;
    }

    public static bool operator <=(ElasticDocumentVersion left, ElasticDocumentVersion right)
    {
        return left.CompareTo(right) <= 0;
    }

    public static bool operator >=(ElasticDocumentVersion left, ElasticDocumentVersion right)
    {
        return left.CompareTo(right) >= 0;
    }
}
