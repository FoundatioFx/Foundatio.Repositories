using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch {
    public struct ElasticDocumentVersion : IEquatable<ElasticDocumentVersion>, IComparable<ElasticDocumentVersion>, IComparable {
        public static ElasticDocumentVersion Empty = new ElasticDocumentVersion();
        
        public ElasticDocumentVersion(long sequenceNumber, long primaryTerm) {
            SequenceNumber = sequenceNumber;
            PrimaryTerm = primaryTerm;
        }

        public long SequenceNumber { get; }
        public long PrimaryTerm { get; }
        
        public override string ToString() => String.Concat(SequenceNumber.ToString(), ":", PrimaryTerm.ToString());

        public override bool Equals(object obj) {
            if (obj is ElasticDocumentVersion version)
                return Equals(version);
            
            if (obj is string stringVersion)
                return Equals(Parse(stringVersion));
            
            if (obj is IVersioned versioned)
                return Equals(versioned.GetVersion());

            return false;
        }

        public override int GetHashCode() {
            unchecked {
                return (SequenceNumber.GetHashCode() * 397) ^ PrimaryTerm.GetHashCode();
            }
        }
        
        public bool Equals(ElasticDocumentVersion v) {
            if (v == Empty || this == Empty)
                return false;
            
            return SequenceNumber == v.SequenceNumber && PrimaryTerm == v.PrimaryTerm;
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
            
            var parts = version.Split(':');
            if (Int64.TryParse(parts[0], out long sequenceNumber) && Int64.TryParse(parts[1], out long primaryTerm))
                return new ElasticDocumentVersion(sequenceNumber, primaryTerm);
            
            return Empty;
        }
        
        public static implicit operator ElasticDocumentVersion(string version) => Parse(version);
        public static implicit operator string(ElasticDocumentVersion version) => version.ToString();

        public int CompareTo(ElasticDocumentVersion other) {
            int sequenceNumberComparison = SequenceNumber.CompareTo(other.SequenceNumber);
            return sequenceNumberComparison != 0 ? sequenceNumberComparison : PrimaryTerm.CompareTo(other.PrimaryTerm);
        }

        public int CompareTo(object obj) {
            if (ReferenceEquals(null, obj))
                return 1;
            
            if (obj is ElasticDocumentVersion version)
                return CompareTo(version);
            
            if (obj is string stringVersion)
                return CompareTo(Parse(stringVersion));
            
            if (obj is IVersioned versioned)
                return CompareTo(versioned.GetVersion());

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
}