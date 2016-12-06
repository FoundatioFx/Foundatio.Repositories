using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public interface IAggregate {
        IReadOnlyDictionary<string, object> Data { get; set; }
    }
}
