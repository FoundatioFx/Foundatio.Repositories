using System;
using Elasticsearch.Net;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SetElasticOptionsExtensions {
        internal const string SnapshotPagingKey = "@SnapshotPaging";
        internal const string SearchAfterPagingKey = "@SearchAfterPaging";
        internal const string SnapshotPagingScrollIdKey = "@SnapshotPagingScrollId";
        internal const string SearchAfterKey = "@SearchAfter";

        public static T SnapshotPaging<T>(this T options) where T : ICommandOptions {
            return options.BuildOption(SnapshotPagingKey, true);
        }

        public static T SearchAfterPaging<T>(this T options) where T : ICommandOptions {
            return options.BuildOption(SearchAfterPagingKey, true);
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

        public static T SearchAfter<T>(this T options, params object[] values) where T : ICommandOptions {
            if (values.Length > 0)
                options.Values.Set(SearchAfterKey, values);

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadElasticOptionsExtensions {
        internal const string ElasticTypeSettingsKey = "@ElasticTypeSettings";
        public static T ElasticType<T>(this T options, IIndexType indexType) where T: ICommandOptions {
            options.Values.Set(ElasticTypeSettingsKey, new ElasticTypeSettings(indexType));
            return options;
        }

        public static ElasticTypeSettings GetElasticTypeSettings(this ICommandOptions options) {
            return options.SafeGetOption<ElasticTypeSettings>(ElasticTypeSettingsKey);
        }

        internal const string RootAliasResolverKey = "@RootAliasResolver";
        public static T RootAliasResolver<T>(this T options, AliasResolver rootAliasResolver) where T : ICommandOptions {
            options.Values.Set(RootAliasResolverKey, rootAliasResolver);
            return options;
        }

        public static AliasResolver GetRootAliasResolver(this ICommandOptions options) {
            return options.SafeGetOption<AliasResolver>(RootAliasResolverKey);
        }

        public static bool ShouldUseSnapshotPaging(this ICommandOptions options) {
            return options.SafeGetOption<bool>(SetElasticOptionsExtensions.SnapshotPagingKey, false);
        }

        public static bool ShouldUseSearchAfterPaging(this ICommandOptions options) {
            return options.SafeGetOption<bool>(SetElasticOptionsExtensions.SearchAfterPagingKey, false);
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

        public static object[] GetSearchAfter(this ICommandOptions options) {
            return options.SafeGetOption<object[]>(SetElasticOptionsExtensions.SearchAfterKey);
        }

        public static bool HasSearchAfter(this ICommandOptions options) {
            return options.SafeHasOption(SetElasticOptionsExtensions.SearchAfterKey);
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

    public class ElasticTypeSettings {
        public ElasticTypeSettings(IIndexType indexType) {
            IndexType = indexType;
            Index = indexType.Index;
            ChildType = indexType as IChildIndexType;
            TimeSeriesType = indexType as ITimeSeriesIndexType;
            HasParent = ChildType != null;
            HasMultipleIndexes = TimeSeriesType != null;
            SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(indexType.Type);
            HasIdentity = typeof(IIdentity).IsAssignableFrom(indexType.Type);
            ParentSupportsSoftDeletes = ChildType != null && typeof(ISupportSoftDeletes).IsAssignableFrom(ChildType.GetParentIndexType().Type);
        }

        public IIndexType IndexType { get; }
        public bool SupportsSoftDeletes { get; }
        public bool HasIdentity { get; }
        public IIndex Index { get; }
        public bool HasParent { get; }
        public bool ParentSupportsSoftDeletes { get; }
        public IChildIndexType ChildType { get; }
        public bool HasMultipleIndexes { get; }
        public ITimeSeriesIndexType TimeSeriesType { get; }
    }
}

