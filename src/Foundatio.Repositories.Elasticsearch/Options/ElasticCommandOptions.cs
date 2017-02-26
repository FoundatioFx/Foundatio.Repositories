using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SetElasticOptionsExtensions {
        internal const string ElasticTypeSettingsKey = "@ElasticTypeSettings";
        public static T SetElasticType<T>(this T options, IIndexType indexType) where T : ICommandOptions {
            options.SetOption(ElasticTypeSettingsKey, new ElasticTypeSettings(indexType));
            return options;
        }

        internal const string DefaultExcludesKey = "@DefaultExcludes";
        public static T AddDefaultExclude<T>(this T options, string exclude) where T : ICommandOptions {
            var excludes = options.GetOption(DefaultExcludesKey, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            excludes.Add(exclude);
            options.SetOption(DefaultExcludesKey, excludes);

            return options;
        }

        public static T AddDefaultExcludes<T>(this T options, IEnumerable<string> excludes) where T : ICommandOptions {
            var excludesValue = options.GetOption(DefaultExcludesKey, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            excludesValue.AddRange(excludes);
            options.SetOption(DefaultExcludesKey, excludesValue);

            return options;
        }

        internal const string RootAliasResolverKey = "@RootAliasResolver";
        public static T SetRootAliasResolver<T>(this T options, AliasResolver rootAliasResolver) where T : ICommandOptions {
            options.SetOption(RootAliasResolverKey, rootAliasResolver);
            return options;
        }

        internal const string UseSnapshotPagingKey = "@UseSnapshotPaging";
        internal const string SnapshotPagingScrollIdKey = "@SnapshotPagingScrollId";
        public static T WithSnapshotPaging<T>(this T options, string scrollId) where T : ICommandOptions {
            options.SetOption(UseSnapshotPagingKey, true);
            options.SetOption(SnapshotPagingScrollIdKey, scrollId);

            return options;
        }

        internal const string SnapshotPagingLifetimeKey = "@SnapshotPagingLifetime";
        public static T WithSnapshotPaging<T>(this T options, TimeSpan? snapshotLifetime = null) where T : ICommandOptions {
            options.SetOption(UseSnapshotPagingKey, true);
            if (snapshotLifetime.HasValue)
                options.SetOption(SnapshotPagingLifetimeKey, snapshotLifetime.Value);

            return options;
        }

        public static T WithSnapshotLifetime<T>(this T options, TimeSpan snapshotLifetime) where T : ICommandOptions {
            options.SetOption(UseSnapshotPagingKey, true);
            options.SetOption(SnapshotPagingLifetimeKey, snapshotLifetime);

            return options;
        }

        public static T WithScrollId<T>(this T options, string scrollId) where T : ICommandOptions {
            options.SetOption(UseSnapshotPagingKey, true);
            options.SetOption(SnapshotPagingScrollIdKey, scrollId);

            return options;
        }

        public static T WithScrollId<T>(this T options, IHaveData target) where T : ICommandOptions {
            options.SetOption(UseSnapshotPagingKey, true);
            options.SetOption(SnapshotPagingScrollIdKey, target.GetScrollId());

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadElasticOptionsExtensions {
        public static ElasticTypeSettings GetElasticTypeSettings<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return null;

            return options.GetOption<ElasticTypeSettings>(SetElasticOptionsExtensions.ElasticTypeSettingsKey);
        }

        public static ISet<string> GetDefaultExcludes<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return new HashSet<string>();

            return options.GetOption<ISet<string>>(SetElasticOptionsExtensions.DefaultExcludesKey, new HashSet<string>());
        }

        public static AliasResolver GetRootAliasResolver<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return null;

            return options.GetOption<AliasResolver>(SetElasticOptionsExtensions.RootAliasResolverKey);
        }

        public static bool ShouldUseSnapshotPaging<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return false;

            return options.GetOption<bool>(SetElasticOptionsExtensions.UseSnapshotPagingKey, false);
        }

        public static bool HasSnapshotScrollId<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return false;

            return options.HasOption(SetElasticOptionsExtensions.SnapshotPagingScrollIdKey);
        }

        public static string GetSnapshotScrollId<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return null;

            return options.GetOption<string>(SetElasticOptionsExtensions.SnapshotPagingScrollIdKey, null);
        }

        public static bool HasSnapshotLifetime<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return false;

            return options.HasOption(SetElasticOptionsExtensions.SnapshotPagingLifetimeKey);
        }

        public static TimeSpan GetSnapshotLifetime<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return TimeSpan.FromMinutes(1);

            return options.GetOption<TimeSpan>(SetElasticOptionsExtensions.SnapshotPagingLifetimeKey);
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

