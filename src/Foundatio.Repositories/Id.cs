using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Foundatio.Repositories {
    public struct Id : IEquatable<Id> {
        public Id(string id, string routing = null) {
            Value = id;
            Routing = routing;
        }

        public string Value { get; }
        public string Routing { get; }

        public static implicit operator Id(string id) => new Id(id);
        public static implicit operator string(Id id) => id.ToString();

        public override string ToString() {
            if (Routing == null)
                return Value;

            return String.Concat(Routing, "-", Value);
        }

        public bool Equals(Id other) {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Value, other.Value) && Equals(Routing, other.Routing);
        }

        public override bool Equals(object obj) {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == this.GetType() && Equals((Id)obj);
        }

        public override int GetHashCode() {
            unchecked {
                return ((Value?.GetHashCode() ?? 0) * 397) ^ (Routing?.GetHashCode() ?? 0);
            }
        }

        public static bool operator ==(Id left, Id right) {
            return Equals(left, right);
        }

        public static bool operator !=(Id left, Id right) {
            return !Equals(left, right);
        }
    }

    public class Ids : List<Id> {
        public Ids() { }

        public Ids(IEnumerable<string> ids) : base(ids.Select(i => (Id)i)) {}

        public static implicit operator Ids(List<string> ids) {
            var result = new Ids();

            foreach (string id in ids)
                result.Add(id);

            return result;
        }

        public static implicit operator Ids(Collection<string> ids) {
            var result = new Ids();

            foreach (string id in ids)
                result.Add(id);

            return result;
        }

        public static implicit operator Ids(string[] ids) {
            var result = new Ids();

            foreach (string id in ids)
                result.Add(id);

            return result;
        }

        public static implicit operator List<string>(Ids ids) {
            var result = new List<string>();
            for (int i = 0; i < ids.Count; i++)
                result[i] = ids[i].ToString();

            return result;
        }

        public static implicit operator Collection<string>(Ids ids) {
            var result = new Collection<string>();
            for (int i = 0; i < ids.Count; i++)
                result[i] = ids[i].ToString();

            return result;
        }

        public static implicit operator string[] (Ids ids) {
            string[] result = new string[ids.Count];
            for (int i = 0; i < ids.Count; i++)
                result[i] = ids[i].ToString();

            return result;
        }
    }
}