using System;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Models {
    public interface IVersioned {
        /// <summary>
        /// Current modification version for the document.
        /// </summary>
        [JsonIgnore]
        long Version { get; set; }
    }
}
