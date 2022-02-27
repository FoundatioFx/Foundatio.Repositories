using System.Collections.Generic;

namespace Foundatio.Repositories.Models;

public interface IHaveData {
    IDictionary<string, object> Data { get; }
}
