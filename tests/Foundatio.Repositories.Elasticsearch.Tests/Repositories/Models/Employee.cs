using System;
using System.Collections.Generic;
using Exceptionless;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

public class PeerReview {
    public string ReviewerEmployeeId { get; set; }
    public int Rating { get; set; }
}

public class PhoneInfo {
    public string Number { get; set; }
    public string Extension { get; set; }
}

public class Employee : IIdentity, IHaveDates, IVersioned, ISupportSoftDeletes, IHaveCustomFields {
    public string Id { get; set; }
    public string CompanyId { get; set; }
    public string CompanyName { get; set; }
    public string UnmappedCompanyName => CompanyName;
    public string Name { get; set; }
    public string EmailAddress { get; set; }
    public string UnmappedEmailAddress => EmailAddress;
    public int Age { get; set; }
    public int UnmappedAge => Age;
    public string Location { get; set; }
    public int YearsEmployed { get; set; }
    public DateTime LastReview { get; set; }
    public DateTimeOffset NextReview { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public string Version { get; set; }
    public bool IsDeleted { get; set; }
    public PeerReview[] PeerReviews { get; set; }
    public IList<PhoneInfo> PhoneNumbers { get; set; } = new List<PhoneInfo>();

    public IDictionary<string, object> Idx { get; set; } = new Dictionary<string, object>();
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();

    //IDictionary<string, object> IHaveVirtualCustomFields.GetCustomFields() {
    //    return Data;
    //}

    //object IHaveVirtualCustomFields.GetCustomField(string name) {
    //    return Data[name];
    //}

    //void IHaveVirtualCustomFields.SetCustomField(string name, object value) {
    //    Data[name] = value;
    //}

    //void IHaveVirtualCustomFields.RemoveCustomField(string name) {
    //    Data.Remove(name);
    //}

    string IHaveCustomFields.GetTenantKey() {
        return CompanyId;
    }

    protected bool Equals(Employee other) {
        return String.Equals(Id, other.Id, StringComparison.InvariantCultureIgnoreCase) &&
            String.Equals(EmailAddress, other.EmailAddress, StringComparison.InvariantCultureIgnoreCase) &&
            String.Equals(CompanyId, other.CompanyId, StringComparison.InvariantCultureIgnoreCase) &&
            String.Equals(CompanyName, other.CompanyName, StringComparison.InvariantCultureIgnoreCase) &&
            String.Equals(Name, other.Name, StringComparison.InvariantCultureIgnoreCase) &&
            Age == other.Age &&
            YearsEmployed == other.YearsEmployed &&
            LastReview.Equals(other.LastReview) &&
            CreatedUtc.Equals(other.CreatedUtc) &&
            UpdatedUtc.Equals(other.UpdatedUtc) &&
            Version == other.Version;
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
            int hashCode = (Id != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(Id) : 0);
            hashCode = (hashCode * 397) ^ (EmailAddress != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(EmailAddress) : 0);
            hashCode = (hashCode * 397) ^ (CompanyId != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(CompanyId) : 0);
            hashCode = (hashCode * 397) ^ (CompanyName != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(CompanyName) : 0);
            hashCode = (hashCode * 397) ^ (Name != null ? StringComparer.InvariantCultureIgnoreCase.GetHashCode(Name) : 0);
            hashCode = (hashCode * 397) ^ Age;
            hashCode = (hashCode * 397) ^ YearsEmployed;
            hashCode = (hashCode * 397) ^ LastReview.GetHashCode();
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

    public static Employee Default => new() {
        Name = "Blake",
        EmailAddress = "blake@exceptionless.com",
        Age = 29,
        YearsEmployed = 9,
        CompanyName = "Exceptionless",
        CompanyId = DefaultCompanyId
    };

    public static Employee Generate(string id = null, string name = null, int? age = null, int? yearsEmployed = null, string companyName = null, string companyId = null, string location = null, DateTime? lastReview = null, DateTimeOffset? nextReview = null, DateTime? createdUtc = null, DateTime? updatedUtc = null) {
        return new Employee {
            Id = id,
            Name = name,
            Age = age ?? RandomData.GetInt(18, 100),
            YearsEmployed = yearsEmployed ?? RandomData.GetInt(0, 1),
            CompanyName = companyName,
            CompanyId = companyId ?? ObjectId.GenerateNewId().ToString(),
            LastReview = lastReview.GetValueOrDefault(),
            NextReview = nextReview.GetValueOrDefault(),
            CreatedUtc = createdUtc.GetValueOrDefault(),
            UpdatedUtc = updatedUtc.GetValueOrDefault(),
            Location = location ?? RandomData.GetCoordinate()
        };
    }

    public static Employee GenerateRandom(string id = null, string name = null, int? age = null, int? yearsEmployed = null, string companyName = null, string companyId = null, string location = null, DateTime? lastReview = null, DateTimeOffset? nextReview = null, DateTime? createdUtc = null, DateTime? updatedUtc = null) {
        var employee = new Employee {
            Id = id,
            Name = name ?? RandomData.GetAlphaString(),
            Age = age ?? RandomData.GetInt(18, 100),
            YearsEmployed = yearsEmployed ?? RandomData.GetInt(0, 40),
            CompanyName = companyName ?? RandomData.GetAlphaString(),
            CompanyId = companyId ?? ObjectId.GenerateNewId().ToString(),
            LastReview = lastReview ?? RandomData.GetDateTime(DateTime.Now.SubtractDays(365), DateTime.Now),
            CreatedUtc = createdUtc ?? RandomData.GetDateTime(DateTime.Now.SubtractDays(365), DateTime.Now),
            Location = location ?? RandomData.GetCoordinate()
        };

        employee.NextReview = nextReview ?? RandomData.GetDateTimeOffset(employee.NextReview, DateTime.Now);
        employee.UpdatedUtc = updatedUtc ?? RandomData.GetDateTime(employee.CreatedUtc, DateTime.Now);

        return employee;
    }

    public static List<Employee> GenerateEmployees(int count = 10, string id = null, string name = null, int? age = null, int? yearsEmployed = null, string companyName = null, string companyId = null, string location = null, DateTime? lastReview = null, DateTimeOffset? nextReview = null, DateTime? createdUtc = null, DateTime? updatedUtc = null) {
        var results = new List<Employee>(count);
        for (int index = 0; index < count; index++)
            results.Add(GenerateRandom(id, name, age, yearsEmployed, companyName, companyId, location, lastReview, nextReview, createdUtc, updatedUtc));

        return results;
    }
}

