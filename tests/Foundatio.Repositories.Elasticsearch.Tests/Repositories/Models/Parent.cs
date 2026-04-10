using System;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

public class Parent : IParentChildDocument, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; } = null!;
    string IParentChildDocument.ParentId { get; set; } = null!;
    JoinField IParentChildDocument.Discriminator { get; set; } = null!;
    public string ParentProperty { get; set; } = null!;
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}

public static class ParentGenerator
{
    public static readonly string DefaultId = ObjectId.GenerateNewId().ToString();

    public static Parent Default => new() { Id = DefaultId };

    public static Parent Generate(string? id = null)
    {
        return new Parent { Id = id! };
    }
}
