using System;
using Exceptionless;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Models {
    public class Employee : IIdentity, IHaveDates, IVersioned {
        public string Id { get; set; }
        public string CompanyId { get; set; }
        public string CompanyName { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime UpdatedUtc { get; set; }
        public long Version { get; set; }

        protected bool Equals(Employee other) {
            return String.Equals(Id, other.Id, StringComparison.InvariantCultureIgnoreCase) && String.Equals(CompanyId, other.CompanyId, StringComparison.InvariantCultureIgnoreCase) && String.Equals(CompanyName, other.CompanyName, StringComparison.InvariantCultureIgnoreCase) && String.Equals(Name, other.Name, StringComparison.InvariantCultureIgnoreCase) && Age == other.Age && CreatedUtc.Equals(other.CreatedUtc) && UpdatedUtc.Equals(other.UpdatedUtc) && Version == other.Version;
        }

        public override bool Equals(object obj) {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((Employee)obj);
        }
        
        public override int GetHashCode() {
            unchecked {
                var hashCode = (Id != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(Id) : 0);
                hashCode = (hashCode * 397) ^ (CompanyId != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(CompanyId) : 0);
                hashCode = (hashCode * 397) ^ (CompanyName != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(CompanyName) : 0);
                hashCode = (hashCode * 397) ^ (Name != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(Name) : 0);
                hashCode = (hashCode * 397) ^ Age;
                hashCode = (hashCode * 397) ^ CreatedUtc.GetHashCode();
                hashCode = (hashCode * 397) ^ UpdatedUtc.GetHashCode();
                hashCode = (hashCode * 397) ^ Version.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(Employee left, Employee right) {
            return Equals(left, right);
        }

        public static bool operator !=(Employee left, Employee right) {
            return !Equals(left, right);
        }
    }

    public static class EmployeeGenerator {
        public static readonly string DefaultCompanyId = ObjectId.GenerateNewId().ToString();

        public static Employee Default => new Employee {
            Name = "Blake",
            Age = 29,
            CompanyName = "Exceptionless",
            CompanyId = DefaultCompanyId
        };

        public static Employee Generate(string id = null, string name = null, int? age = null, string companyName = null, string companyId = null, DateTime? createdUtc = null, DateTime? updatedUtc = null) {
            return new Employee {
                Id = id,
                Name = name ?? RandomData.GetAlphaString(),
                Age = age ?? RandomData.GetInt(18, 100),
                CompanyName = companyName ?? RandomData.GetAlphaString(),
                CompanyId = companyId ?? ObjectId.GenerateNewId().ToString(),
                CreatedUtc = createdUtc.GetValueOrDefault(),
                UpdatedUtc = updatedUtc.GetValueOrDefault()
            };
        }
    }
}

