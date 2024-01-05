using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Utility;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("{Type} {ChangeType}: Id={Id}")]
public class EntityChanged : IHaveData
{
    public EntityChanged()
    {
        Data = new DataDictionary();
    }

    public string Type { get; set; }
    public string Id { get; set; }
    public ChangeType ChangeType { get; set; }
    public IDictionary<string, object> Data { get; set; }
}
