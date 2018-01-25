using System;
using System.IO;
using System.Text;
using Foundatio.Serializer;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Models {
	public interface ILazyDocument {
		T As<T>(ITextSerializer serializer = null);
		object As(Type objectType, ITextSerializer serializer = null);
	}

	public class LazyDocument : ILazyDocument {
		internal JToken Token { get; }

		public LazyDocument(JToken token) {
			Token = token;
		}

		public T As<T>(ITextSerializer serializer = null) {
			if (Token == null)
                return default(T);
            
            if (serializer != null)
                return serializer.Deserialize<T>(Token.ToString());
            
            return Token.ToObject<T>();
		}

		public object As(Type objectType, ITextSerializer serializer = null) {
			if (Token == null)
                return null;
            
            if (serializer != null)
                return serializer.Deserialize(Token.ToString(), objectType);
            
            return Token.ToObject(objectType);
		}
	}
}