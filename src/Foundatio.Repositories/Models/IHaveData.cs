using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public interface IHaveData {
        IReadOnlyDictionary<string, object> Data { get; }
    }
}
