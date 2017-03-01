using System;
using System.Collections.Generic;
using Elasticsearch.Net;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public enum Consistency {
        Eventual,
        Immediate,
        Wait
    }

    public static class SetElasticOptionsExtensions {
        internal const string ExcludesKey = "@Excludes";
        public static T Exclude<T>(this T options, string exclude) where T : ICommandOptions {
            return options.AddSetOptionValue(ExcludesKey, exclude);
        }

        public static T Exclude<T>(this T options, IEnumerable<string> excludes) where T : ICommandOptions {
            return options.AddSetOptionValue(ExcludesKey, excludes);
        }

        internal const string ConsistencyModeKey = "@ConsistencyMode";
        public static T Consistency<T>(this T options, Consistency mode) where T : ICommandOptions {
            options.SetOption(ConsistencyModeKey, mode);
            return options;
        }

        internal const string SnapshotPagingKey = "@SnapshotPaging";
        internal const string SnapshotPagingScrollIdKey = "@SnapshotPagingScrollId";
        public static T SnapshotPaging<T>(this T options) where T : ICommandOptions {
            return options.BuildOption(SnapshotPagingKey, true);
        }

        internal const string SnapshotPagingLifetimeKey = "@SnapshotPagingLifetime";

        public static T SnapshotPagingLifetime<T>(this T options, TimeSpan? snapshotLifetime) where T : ICommandOptions {
            if (snapshotLifetime.HasValue) {
                options.SetOption(SnapshotPagingKey, true);
                options.SetOption(SnapshotPagingLifetimeKey, snapshotLifetime.Value);
            }

            return options;
        }

        public static T SnapshotPagingScrollId<T>(this T options, string scrollId) where T : ICommandOptions {
            if (scrollId != null) {
                options.SetOption(SnapshotPagingKey, true);
                options.SetOption(SnapshotPagingScrollIdKey, scrollId);
            }

            return options;
        }

        public static T SnapshotPagingScrollId<T>(this T options, IHaveData target) where T : ICommandOptions {
            options.SetOption(SnapshotPagingKey, true);
            options.SetOption(SnapshotPagingScrollIdKey, target.GetScrollId());

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadElasticOptionsExtensions {
        internal const string ElasticTypeSettingsKey = "@ElasticTypeSettings";
        public static T ElasticType<T>(this T options, IIndexType indexType) where T : ICommandOptions {
            options.SetOption(ElasticTypeSettingsKey, new ElasticTypeSettings(indexType));
            return options;
        }

        public static ElasticTypeSettings GetElasticTypeSettings<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<ElasticTypeSettings>(ElasticTypeSettingsKey);
        }

        public static ISet<string> GetExcludes<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<ISet<string>>(SetElasticOptionsExtensions.ExcludesKey, new HashSet<string>());
        }

        internal const string RootAliasResolverKey = "@RootAliasResolver";
        public static T RootAliasResolver<T>(this T options, AliasResolver rootAliasResolver) where T : ICommandOptions {
            options.SetOption(RootAliasResolverKey, rootAliasResolver);
            return options;
        }

        public static AliasResolver GetRootAliasResolver<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<AliasResolver>(RootAliasResolverKey);
        }

        public static bool ShouldUseSnapshotPaging<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<bool>(SetElasticOptionsExtensions.SnapshotPagingKey, false);
        }

        public static bool HasSnapshotScrollId<T>(this T options) where T : ICommandOptions {
            return options.SafeHasOption(SetElasticOptionsExtensions.SnapshotPagingScrollIdKey);
        }

        public static string GetSnapshotScrollId<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<string>(SetElasticOptionsExtensions.SnapshotPagingScrollIdKey, null);
        }

        public static bool HasSnapshotLifetime<T>(this T options) where T : ICommandOptions {
            return options.SafeHasOption(SetElasticOptionsExtensions.SnapshotPagingLifetimeKey);
        }

        public static TimeSpan GetSnapshotLifetime<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<TimeSpan>(SetElasticOptionsExtensions.SnapshotPagingLifetimeKey, TimeSpan.FromMinutes(1));
        }

        public static Refresh GetRefreshMode<T>(this T options, Consistency defaultMode = Consistency.Eventual) where T : ICommandOptions {
            return ToRefresh(options.SafeGetOption<Consistency>(SetElasticOptionsExtensions.ConsistencyModeKey, defaultMode));
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

