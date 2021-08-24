using System;
using System.Collections.Generic;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public interface IQueryOptions {
        bool? OnlyIds { get; set; }
        ICollection<string> Ids { get; set; }
        ICollection<string> ExcludedIds { get; set; }
        IOptionsDictionary Values { get; }
        T Clone<T>() where T : IQueryOptions, new();
    }

    public class QueryOptions : IQueryOptions {
        public bool? OnlyIds { get; set; }
        public ICollection<string> Ids { get; set; }
        public ICollection<string> ExcludedIds { get; set; }
        public IOptionsDictionary Values { get; internal set; } = new OptionsDictionary();

        public virtual T Clone<T>() where T : IQueryOptions, new() {
            var clone = new T();

            foreach (var kvp in Values)
                clone.Values.Add(kvp.Key, kvp.Value);

            var queryOptionsClone = clone as IQueryOptions;
            if (queryOptionsClone == null)
                throw new ArgumentException("Target type must implement ICommandOptions");

            queryOptionsClone.OnlyIds = OnlyIds;
            
            foreach (var id in Ids)
                queryOptionsClone.Ids.Add(id);
            foreach (var excludedId in ExcludedIds)
                queryOptionsClone.ExcludedIds.Add(excludedId);

            return clone;
        }
    }

    public interface IQueryOptionsBuilder<out T> where T : IQueryOptions {
        T Build();
    }
    
    public abstract class QueryOptionsBuilder<TQuery, TBuilder> : IQueryOptionsBuilder<TQuery>
        where TQuery : class, IQueryOptions
        where TBuilder : QueryOptionsBuilder<TQuery, TBuilder> {

        protected readonly TBuilder _builder;
        protected readonly TQuery _target;

        public QueryOptionsBuilder(TQuery target) {
            _builder = (TBuilder)this;
            _target = target ?? throw new ArgumentNullException(nameof(target));
        }

        public TBuilder OnlyIds() {
            _target.OnlyIds = true;
            return _builder;
        }

        public TBuilder Id(string id) {
            _target.Ids.Add(id);
            return _builder;
        }

        public TBuilder Id(params string[] ids) {
            foreach (string id in ids)
                _target.Ids.Add(id);
            return _builder;
        }

        public TBuilder Id(IEnumerable<string> ids) {
            foreach (string id in ids)
                _target.ExcludedIds.Add(id);
            return _builder;
        }

        public TBuilder ExcludeId(string id) {
            _target.ExcludedIds.Add(id);
            return _builder;
        }

        public TBuilder ExcludeId(params string[] ids) {
            foreach (string id in ids)
                _target.ExcludedIds.Add(id);
            return _builder;
        }

        public TBuilder ExcludeId(IEnumerable<string> ids) {
            foreach (string id in ids)
                _target.ExcludedIds.Add(id);
            return _builder;
        }

        public TBuilder Set(string key, object value) {
            _target.Values.Add(key, value);
            return _builder;
        }
        
        TQuery IQueryOptionsBuilder<TQuery>.Build() {
            return _target;
        }
    }

    public class QueryOptionsBuilder : QueryOptionsBuilder<QueryOptions, QueryOptionsBuilder> {
        public QueryOptionsBuilder() : base(new QueryOptions()) {}
        public QueryOptionsBuilder(QueryOptions target) : base(target ?? new QueryOptions()) {}
    }
}
