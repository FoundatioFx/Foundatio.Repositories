using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Serializer;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Models {
    public interface ILazyDocument {
        T As<T>(ITextSerializer serializer = null);
        object As(Type objectType, ITextSerializer serializer = null);
    }

    public class LazyDocument : ILazyDocument {
        internal byte[] Data { get; }

        public LazyDocument(byte[] data) {
            Data = data;
        }

        public T As<T>(ITextSerializer serializer = null) {
            if (Data == null || Data.Length == 0)
                return default;
            
            if (serializer == null)
                serializer = new JsonNetSerializer();
            
            return serializer.Deserialize<T>(Data);
        }

        public object As(Type objectType, ITextSerializer serializer = null) {
            if (Data == null || Data.Length == 0)
                return null;

            if (serializer == null)
                serializer = new JsonNetSerializer();
            
            return serializer.Deserialize(Data, objectType);
        }
    }
}