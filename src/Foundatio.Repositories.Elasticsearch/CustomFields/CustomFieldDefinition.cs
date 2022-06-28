using System;
using System.Collections.Generic;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class CustomFieldDefinition : IIdentity, IHaveDates, ISupportSoftDeletes {
    public string Id { get; set; }

    /// <summary>
    /// The entity type that this custom field is for.
    /// </summary>
    public string EntityType { get; set; }

    /// <summary>
    /// The tenant key which could be a composite key that this custom field belongs to. Each tenant can have it's own
    /// set of custom fields for an entity.
    /// </summary>
    public string TenantKey { get; set; }

    /// <summary>
    /// The friendly custom field name.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// The custom field description.
    /// </summary>
    public string Description { get; set; }

    /// <summary>
    /// Gets or sets the display order.
    /// </summary>
    public int DisplayOrder { get; set; }

    /// <summary>
    /// The type of index this custom field value should be stored in. (ie. string, number, float, address)
    /// </summary>
    public string IndexType { get; set; }

    /// <summary>
    /// The reserved indexing slot for this custom field. This name will be pooled across tenants for the same index
    /// type in order to keep dynamic indexes in Elasticsearch from exploding.
    /// </summary>
    public int IndexSlot { get; set; }
    
    /// <summary>
    /// Any additional custom data that needs to be associated to this custom field.
    /// </summary>
    public DataDictionary Data { get; set; }

    /// <summary>
    /// Date the custom field was last updated.
    /// </summary>
    public DateTime UpdatedUtc { get; set; }

    /// <summary>
    /// Date the custom field was originally created.
    /// </summary>
    public DateTime CreatedUtc { get; set; }

    /// <summary>
    /// Whether this custom field has been soft deleted
    /// </summary>
    public bool IsDeleted { get; set; }
}

public interface IHaveCustomFields {
    IDictionary<string, object> Idx { get; set; }
    IDictionary<string, object> CustomFields { get; set; }
}