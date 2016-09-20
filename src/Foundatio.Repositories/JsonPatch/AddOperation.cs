using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.JsonPatch {
    public class AddOperation : Operation {
        public JToken Value { get; set; }

        public override void Write(JsonWriter writer) {
            writer.WriteStartObject();

            WriteOp(writer, "add");
            WritePath(writer, Path);
            WriteValue(writer, Value);

            writer.WriteEndObject();
        }

        public override void Read(JObject jOperation) {
            Path = jOperation.Value<string>("path");
            Value = jOperation.GetValue("value");
        }
    }
}