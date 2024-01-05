using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public interface ICustomFieldType
{
    string Type { get; }
    Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object value, CustomFieldDefinition fieldDefinition) where T : class;
    IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class;
}

public class ProcessFieldValueResult
{
    public object Value { get; set; }
    public object Idx { get; set; }
    public bool IsCustomFieldDefinitionModified { get; set; }
}
