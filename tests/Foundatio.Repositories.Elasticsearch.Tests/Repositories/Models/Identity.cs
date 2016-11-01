using System;
using System.Collections.Generic;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models {
    public class Identity : IIdentity {
        public string Id { get; set; }

        protected bool Equals(Identity other) {
            return String.Equals(Id, other.Id, StringComparison.InvariantCultureIgnoreCase);
        }

        public override bool Equals(object obj) {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((Identity)obj);
        }

        public override int GetHashCode() {
            return (Id != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(Id) : 0);
        }

        public static bool operator ==(Identity left, Identity right) {
            return Equals(left, right);
        }

        public static bool operator !=(Identity left, Identity right) {
            return !Equals(left, right);
        }
    }

    public static class IdentityGenerator {
        public static readonly string DefaultId = ObjectId.GenerateNewId().ToString();

        public static Identity Default => new Identity {
            Id = DefaultId
        };

        public static Identity Generate(string id = null) {
            return new Identity { Id = id };
        }

        public static List<Identity> GenerateIdentities(int count = 10) {
            var results = new List<Identity>(count);
            for (int index = 0; index < count; index++)
                results.Add(Generate());

            return results;
        }
    }
}