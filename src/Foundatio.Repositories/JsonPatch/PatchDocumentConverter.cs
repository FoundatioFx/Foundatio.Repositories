using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.JsonPatch {
    public class PatchDocumentConverter : JsonConverter {
        public override bool CanConvert(Type objectType) {
            return true;
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) {
            if (objectType != typeof(PatchDocument))
                throw new ArgumentException("Object must be of type PatchDocument", nameof(objectType));

            try {
                if (reader.TokenType == JsonToken.Null)
                    return null;

                var patch = JArray.Load(reader);
                return PatchDocument.Parse(patch.ToString());
            } catch (Exception ex) {
                throw new ArgumentException("Invalid patch document: " + ex.Message);
            }
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) {
            if (!(value is PatchDocument))
                return;

            var jsonPatchDoc = (PatchDocument)value;
            writer.WriteStartArray();
            foreach (var op in jsonPatchDoc.Operations)
                op.Write(writer);
            writer.WriteEndArray();
        }
    }
}
