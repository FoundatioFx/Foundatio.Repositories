using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IElasticIndex {
        int Version { get; }
        string AliasName { get; }
        string VersionedName { get; }
        IDictionary<Type, IElasticIndexType> Types { get; }
        CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx);
    }

    public interface ITemplatedElasticIndex : IElasticIndex {
        PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template);
    }

    public class ElasticIndex : IElasticIndex {
        public ElasticIndex(string name, int version = 1) {
            AliasName = name;
        }

        public int Version { get; }
        public string AliasName { get; }
        public string VersionedName => String.Concat(AliasName, "-v", Version);

        public void AddIndexType<T>(IElasticIndexType indexType) {
            Types.Add(typeof(T), indexType);
        }

        public IDictionary<Type, IElasticIndexType> Types { get; } = new Dictionary<Type, IElasticIndexType>();

        public virtual CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            foreach (var t in Types)
                t.Value.ConfigureIndex(idx);
            
            return idx;
        }
    }

    public abstract class TemplatedElasticIndex: ElasticIndex, ITemplatedElasticIndex {
        public TemplatedElasticIndex(string name, int version = 1): base(name, version) {}

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            foreach (var t in Types)
                t.Value.ConfigureTemplate(template);

            return template;
        }
    }

    public abstract class MonthlyElasticIndex : TemplatedElasticIndex {
        public MonthlyElasticIndex(string name, int version = 1) : base(name, version) { }
    }
}
