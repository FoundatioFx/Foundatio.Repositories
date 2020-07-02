using System;
using Nest;
using ILazyDocument = Foundatio.Repositories.Models.ILazyDocument;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public class ElasticLazyDocument : ILazyDocument {
        private readonly Nest.ILazyDocument _inner;
        
        public ElasticLazyDocument(Nest.ILazyDocument inner) {
            _inner = inner;
        }
        
        public T As<T>() where T : class {
            var hit = _inner.As<IHit<T>>();
            return hit?.Source;
        }

        public object As(Type objectType) {
            var hitType = typeof(IHit<>).MakeGenericType(objectType);
            return _inner.As(hitType);
        }
    }
}