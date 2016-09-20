using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.JsonPatch {
    public class MoveOperation : Operation {
        public string FromPath { get; set; }

        public override void Write(JsonWriter writer) {
            writer.WriteStartObject();

            WriteOp(writer, "move");
            WritePath(writer, Path);
            WriteFromPath(writer, FromPath);

            writer.WriteEndObject();
        }

        public override void Read(JObject jOperation) {
            Path = jOperation.Value<string>("path");
            FromPath = jOperation.Value<string>("from");
        }
    }
}