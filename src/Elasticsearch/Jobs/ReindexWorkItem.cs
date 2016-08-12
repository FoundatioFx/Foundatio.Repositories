using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class ReindexWorkItem {
        public ReindexWorkItem() {
            ParentMaps = new List<ParentMap>();
        }

        protected bool Equals(ReindexWorkItem other) {
            return string.Equals(OldIndex, other.OldIndex) && string.Equals(NewIndex, other.NewIndex) && string.Equals(Alias, other.Alias) && DeleteOld == other.DeleteOld && string.Equals(TimestampField, other.TimestampField) && StartUtc.Equals(other.StartUtc) && Equals(ParentMaps, other.ParentMaps);
        }

        public override bool Equals(object obj) {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((ReindexWorkItem)obj);
        }

        public override int GetHashCode() {
            unchecked {
                var hashCode = (OldIndex != null ? OldIndex.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (NewIndex != null ? NewIndex.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Alias != null ? Alias.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ DeleteOld.GetHashCode();
                hashCode = (hashCode * 397) ^ (TimestampField != null ? TimestampField.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ StartUtc.GetHashCode();
                hashCode = (hashCode * 397) ^ (ParentMaps != null ? ParentMaps.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(ReindexWorkItem left, ReindexWorkItem right) {
            return Equals(left, right);
        }

        public static bool operator !=(ReindexWorkItem left, ReindexWorkItem right) {
            return !Equals(left, right);
        }

        public string OldIndex { get; set; }
        public string NewIndex { get; set; }
        public string Alias { get; set; }
        public bool DeleteOld { get; set; }
        public string TimestampField { get; set; }
        public DateTime? StartUtc { get; set; }
        public List<ParentMap> ParentMaps { get; set; }
    }

    public class ParentMap {
        public string Type { get; set; }
        public string ParentPath { get; set; }

        protected bool Equals(ParentMap other) {
            return string.Equals(Type, other.Type) && string.Equals(ParentPath, other.ParentPath);
        }

        public override bool Equals(object obj) {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((ParentMap)obj);
        }

        public override int GetHashCode() {
            unchecked {
                return ((Type != null ? Type.GetHashCode() : 0) * 397) ^ (ParentPath != null ? ParentPath.GetHashCode() : 0);
            }
        }

        public static bool operator ==(ParentMap left, ParentMap right) {
            return Equals(left, right);
        }

        public static bool operator !=(ParentMap left, ParentMap right) {
            return !Equals(left, right);
        }
    }
}