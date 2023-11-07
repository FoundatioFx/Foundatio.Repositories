using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

public record FileAccessHistory : IIdentity {
    public string Id { get; set; }
    public DateTime AccessedDateUtc { get; set; }
    public string Path { get; set; }
}