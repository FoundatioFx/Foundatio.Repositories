using System;
using Elasticsearch.Net;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SetElasticOptionsExtensions {
        internal const string SnapshotPagingKey = "@SnapshotPaging";
        internal const string SnapshotPagingScrollIdKey = "@SnapshotPagingScrollId";

        public static T SnapshotPaging<T>(this T options) where T : ICommandOptions {
            return options.BuildOption(SnapshotPagingKey, true);
        }

        internal const string SnapshotPagingLifetimeKey = "@SnapshotPagingLifetime";

        public static T SnapshotPagingLifetime<T>(this T options, TimeSpan? snapshotLifetime) where T : ICommandOptions {
            if (snapshotLifetime.HasValue) {
                options.Values.Set(SnapshotPagingKey, true);
                options.Values.Set(SnapshotPagingLifetimeKey, snapshotLifetime.Value);
            }

            return options;
        }

        public static T SnapshotPagingScrollId<T>(this T options, string scrollId) where T : ICommandOptions {
            if (scrollId != null) {
                options.Values.Set(SnapshotPagingKey, true);
                options.Values.Set(SnapshotPagingScrollIdKey, scrollId);
            }

            return options;
        }

        public static T SnapshotPagingScrollId<T>(this T options, IHaveData target) where T : ICommandOptions {
            options.Values.Set(SnapshotPagingKey, true);
            options.Values.Set(SnapshotPagingScrollIdKey, target.GetScrollId());

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadElasticOptionsExtensions {
        internal const string ElasticIndexKey = "@ElasticIndex";
        public static T ElasticIndex<T>(this T options, IIndex index) where T: ICommandOptions {
            options.Values.Set(ElasticIndexKey, index);
            return options;
        }

        public static IIndex GetElasticIndex(this ICommandOptions options) {
            return options.SafeGetOption<IIndex>(ElasticIndexKey);
        }

        public static ElasticMappingResolver GetMappingResolver(this ICommandOptions options) {
            return options.GetElasticIndex()?.MappingResolver;
        }

        internal const string SupportsSoftDeletesKey = "@SupportsSoftDeletesKey";
        public static T SupportsSoftDeletes<T>(this T options, bool supportsSoftDeletes) where T: ICommandOptions {
            options.Values.Set(SupportsSoftDeletesKey, supportsSoftDeletes);
            return options;
        }

        public static bool SupportsSoftDeletes(this ICommandOptions options) {
            return options.SafeGetOption<bool>(SupportsSoftDeletesKey);
        }

        internal const string DocumentTypeKey = "@DocumentType";
        public static T DocumentType<T>(this T options, Type documentType) where T : ICommandOptions {
            options.Values.Set(DocumentTypeKey, documentType);
            return options;
        }

        public static Type DocumentType(this ICommandOptions options) {
            return options.SafeGetOption<Type>(DocumentTypeKey);
        }

        internal const string ParentDocumentTypeKey = "@ParentDocumentType";
        public static T ParentDocumentType<T>(this T options, Type parentDocumentType) where T : ICommandOptions {
            if (parentDocumentType != null)
                return options.BuildOption(ParentDocumentTypeKey, parentDocumentType);

            options.Values?.Remove(ParentDocumentTypeKey);

            return options;
        }

        public static Type ParentDocumentType(this ICommandOptions options) {
            return options.SafeGetOption<Type>(ParentDocumentTypeKey, typeof(object));
        }

        internal const string QueryFieldResolverKey = "@QueryFieldResolver";
        public static T QueryFieldResolver<T>(this T options, QueryFieldResolver rootAliasResolver) where T : ICommandOptions {
            options.Values.Set(QueryFieldResolverKey, rootAliasResolver);
            return options;
        }

        public static QueryFieldResolver GetQueryFieldResolver(this ICommandOptions options) {
            return options.SafeGetOption<QueryFieldResolver>(QueryFieldResolverKey);
        }

        internal const string IncludeResolverKey = "@IncludeResolver";
        public static T IncludeResolver<T>(this T options, IncludeResolver includeResolver) where T : ICommandOptions {
            options.Values.Set(IncludeResolverKey, includeResolver);
            return options;
        }

        public static IncludeResolver GetIncludeResolver(this ICommandOptions options) {
            return options.SafeGetOption<IncludeResolver>(IncludeResolverKey);
        }

        public static bool ShouldUseSnapshotPaging(this ICommandOptions options) {
            return options.SafeGetOption<bool>(SetElasticOptionsExtensions.SnapshotPagingKey, false);
        }

        public static bool HasSnapshotScrollId(this ICommandOptions options) {
            return options.SafeHasOption(SetElasticOptionsExtensions.SnapshotPagingScrollIdKey);
        }

        public static string GetSnapshotScrollId(this ICommandOptions options) {
            return options.SafeGetOption<string>(SetElasticOptionsExtensions.SnapshotPagingScrollIdKey, null);
        }

        public static bool HasSnapshotLifetime(this ICommandOptions options) {
            return options.SafeHasOption(SetElasticOptionsExtensions.SnapshotPagingLifetimeKey);
        }

        public static TimeSpan GetSnapshotLifetime(this ICommandOptions options) {
            return options.SafeGetOption<TimeSpan>(SetElasticOptionsExtensions.SnapshotPagingLifetimeKey, TimeSpan.FromMinutes(1));
        }

        public static Refresh GetRefreshMode(this ICommandOptions options, Consistency defaultMode = Consistency.Eventual) {
            return ToRefresh(options.GetConsistency(defaultMode));
        }

        private static Refresh ToRefresh(Consistency mode) {
            if (mode == Consistency.Immediate)
                return Refresh.True;
            if (mode == Consistency.Wait)
                return Refresh.WaitFor;

            return Refresh.False;
        }
    }
}

