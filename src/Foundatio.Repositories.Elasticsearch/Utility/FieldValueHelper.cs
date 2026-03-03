using System;
using Elastic.Clients.Elasticsearch;

namespace Foundatio.Repositories.Elasticsearch.Utility;

public static class FieldValueHelper
{
    public static FieldValue ToFieldValue(object value)
    {
        return value switch
        {
            null => FieldValue.Null,
            string s => FieldValue.String(s),
            bool b => FieldValue.Boolean(b),
            long l => FieldValue.Long(l),
            int i => FieldValue.Long(i),
            double d => FieldValue.Double(d),
            float f => FieldValue.Double(f),
            decimal m => FieldValue.Double((double)m),
            DateTime dt => FieldValue.String(dt.ToString("o")),
            DateTimeOffset dto => FieldValue.String(dto.ToString("o")),
            _ => FieldValue.String(value.ToString())
        };
    }
}
