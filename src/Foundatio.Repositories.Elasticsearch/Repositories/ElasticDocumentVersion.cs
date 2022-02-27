using System;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch;

public struct ElasticDocumentVersion : IEquatable<ElasticDocumentVersion>, IComparable<ElasticDocumentVersion>, IComparable {
    public static ElasticDocumentVersion Empty = new();
    
    public ElasticDocumentVersion(long primaryTerm, long sequenceNumber) {
        PrimaryTerm = primaryTerm;
        SequenceNumber = sequenceNumber;
    }

    public long PrimaryTerm { get; }
    public long SequenceNumber { get; }
    public bool IsEmpty => PrimaryTerm <= 0 && SequenceNumber <= 0;
    
    public override string ToString() => String.Concat(PrimaryTerm.ToString(), ":", SequenceNumber.ToString());

    public override bool Equals(object obj) {
        if (obj is ElasticDocumentVersion version)
            return Equals(version);
        
        if (obj is string stringVersion)
            return Equals(Parse(stringVersion));
        
        if (obj is IVersioned versioned)
            return Equals(versioned.GetElasticVersion());

        return false;
    }

    public override int GetHashCode() {
        unchecked {
            return (PrimaryTerm.GetHashCode() * 397) ^ SequenceNumber.GetHashCode();
        }
    }
    
    public bool Equals(ElasticDocumentVersion v) {
        if (IsEmpty || v.IsEmpty)
            return false;
        
        return PrimaryTerm == v.PrimaryTerm && SequenceNumber == v.SequenceNumber;
    }

    public static bool operator ==(ElasticDocumentVersion lhs, ElasticDocumentVersion rhs) {
        return lhs.Equals(rhs);
    }

    public static bool operator !=(ElasticDocumentVersion lhs, ElasticDocumentVersion rhs) {
        return !lhs.Equals(rhs);
    }

    public static ElasticDocumentVersion Parse(string version) {
        if (String.IsNullOrEmpty(version))
            return Empty;

        string[] parts = version.Split(':');
        if (parts.Length != 2)
            return Empty;
        
        if (Int64.TryParse(parts[0], out long primaryTerm) && Int64.TryParse(parts[1], out long sequenceNumber))
            return new ElasticDocumentVersion(primaryTerm, sequenceNumber);
        
        return Empty;
    }
    
    public static implicit operator ElasticDocumentVersion(string version) => Parse(version);
    public static implicit operator string(ElasticDocumentVersion version) => version.ToString();

    public int CompareTo(ElasticDocumentVersion other) {
        int result = PrimaryTerm.CompareTo(other.PrimaryTerm);
        if (result != 0)
            return result;

        return SequenceNumber.CompareTo(other.SequenceNumber);
    }

    public int CompareTo(object obj) {
        if (ReferenceEquals(null, obj))
            return 1;
        
        if (obj is ElasticDocumentVersion version)
            return CompareTo(version);
        
        if (obj is string stringVersion)
            return CompareTo(Parse(stringVersion));
        
        if (obj is IVersioned versioned)
            return CompareTo(versioned.GetElasticVersion());

        throw new ArgumentException($"Object must be of type {nameof(ElasticDocumentVersion)}");;
    }

    public static bool operator <(ElasticDocumentVersion left, ElasticDocumentVersion right) {
        return left.CompareTo(right) < 0;
    }

    public static bool operator >(ElasticDocumentVersion left, ElasticDocumentVersion right) {
        return left.CompareTo(right) > 0;
    }

    public static bool operator <=(ElasticDocumentVersion left, ElasticDocumentVersion right) {
        return left.CompareTo(right) <= 0;
    }

    public static bool operator >=(ElasticDocumentVersion left, ElasticDocumentVersion right) {
        return left.CompareTo(right) >= 0;
    }
}
