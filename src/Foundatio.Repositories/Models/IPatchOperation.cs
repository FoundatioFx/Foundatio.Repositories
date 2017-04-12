using System;
using System.Collections.Generic;
using Foundatio.Repositories.JsonPatch;

namespace Foundatio.Repositories.Models {
    public interface IPatchOperation {}

    public class PartialPatch : IPatchOperation {
        public PartialPatch(object document) {
            Document = document ?? throw new ArgumentNullException(nameof(document));
        }

        public object Document { get; }
    }

    public class JsonPatch : IPatchOperation {
        public JsonPatch(PatchDocument patch) {
            Patch = patch ?? throw new ArgumentNullException(nameof(patch));
        }

        public PatchDocument Patch { get; }
    }

    public class ScriptPatch : IPatchOperation {
        public ScriptPatch(string script) {
            if (String.IsNullOrEmpty(script))
                throw new ArgumentNullException(nameof(script));

            Script = script;
        }

        public string Script { get; }
        public Dictionary<string, object> Params { get; set; }
        
    }
}