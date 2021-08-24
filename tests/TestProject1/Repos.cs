using System;

namespace Foundatio.Repositories {
    public interface IRepo<in TDoc, in TOptions, TOptionsBuilder, in TQuery, TQueryBuilder>
        where TDoc: class
        where TOptions: class, ICommandOptions
        where TOptionsBuilder : ICommandOptionsBuilder<TOptions>
        where TQuery: class, IQueryOptions
        where TQueryBuilder : IQueryOptionsBuilder<TQuery>
    {
        void Add(TDoc doc, Func<TOptionsBuilder, TOptionsBuilder> options);
        void Add(TDoc doc, TOptions options = null);
        void Query(Func<TQueryBuilder, TQueryBuilder> query, Func<TOptionsBuilder, TOptionsBuilder> options);
        void Query(TQuery query, TOptions options = null);
    }
    
    public class Repo<TDoc> : Repo<TDoc, CommandOptions, CommandOptionsBuilder, QueryOptions, QueryOptionsBuilder> where TDoc : class {}
    
    public class Repo<TDoc, TOptions, TOptionsBuilder, TQuery, TQueryBuilder> : IRepo<TDoc, TOptions, TOptionsBuilder, TQuery, TQueryBuilder>
        where TDoc: class
        where TOptions: class, ICommandOptions, new()
        where TOptionsBuilder : ICommandOptionsBuilder<TOptions>, new()
        where TQuery : class, IQueryOptions, new()
        where TQueryBuilder : IQueryOptionsBuilder<TQuery>, new()
    {
        public void Add(TDoc doc, Func<TOptionsBuilder, TOptionsBuilder> configure) {
            var options = BuildOptions(configure);
            Add(doc, options);
        }

        public void Add(TDoc doc, TOptions options = null) {
            options = ConfigureOptions(options);
            ConfiguredOptions = options.Clone<TOptions>();
        }

        public void Query(Func<TQueryBuilder, TQueryBuilder> query, Func<TOptionsBuilder, TOptionsBuilder> options) {
            var configuredOptions = BuildOptions(options);
            var configuredQuery = BuildQuery(query);
            Query(configuredQuery, configuredOptions);
        }

        public void Query(TQuery query, TOptions options = default(TOptions)) {
            options = ConfigureOptions(options);
            query = ConfigureQuery(query);
            ConfiguredOptions = options.Clone<TOptions>();
            ConfiguredQuery = query.Clone<TQuery>();
        }

        protected virtual TOptions ConfigureOptions(TOptions options) {
            return options;
        }

        protected virtual TQuery ConfigureQuery(TQuery query) {
            return query;
        }

        private static TOptions BuildOptions(Func<TOptionsBuilder, TOptionsBuilder> configure) {
            var builder = new TOptionsBuilder();
            if (configure != null)
                builder = configure(builder);
            
            return builder.Build();
        }

        private static TQuery BuildQuery(Func<TQueryBuilder, TQueryBuilder> configure) {
            var builder = new TQueryBuilder();
            if (configure != null)
                builder = configure(builder);
            
            return builder.Build();
        }
        
        public TOptions ConfiguredOptions { get; private set; }
        public TQuery ConfiguredQuery { get; private set; }
    }
}