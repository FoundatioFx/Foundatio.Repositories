using System;

namespace Foundatio.Repositories.Options {
    public interface IPagingOptions : ICommandOptions {
        int? Limit { get; set; }
        int? Page { get; set; }
    }

    public class PagingOptions : IPagingOptions {
        public int? Limit { get; set; }
        public int? Page { get; set; }

        public static implicit operator PagingOptions(int limit) {
            return new PagingOptions { Limit = limit };
        }
    }

    public static class PagingOptionsExtensions {
        public static T WithLimit<T>(this T options, int? limit) where T : IPagingOptions {
            options.Limit = limit;
            return options;
        }

        public static T WithPaging<T>(this T options, PagingOptions paging) where T : IPagingOptions {
            options.Limit = paging.Limit;
            options.Page = paging.Page;
            return options;
        }
        
        public static bool ShouldUseLimit<T>(this T options) where T : ICommandOptions {
            var pagingOptions = options as IPagingOptions;
            if (pagingOptions == null)
                return false;

            return pagingOptions.Limit.HasValue && pagingOptions.Limit.Value > 0;
        }

        public static int GetLimit<T>(this T options) where T : ICommandOptions {
            var pagingOptions = options as IPagingOptions;
            if (pagingOptions == null)
                return RepositoryConstants.DEFAULT_LIMIT;

            if (pagingOptions.Limit.Value > RepositoryConstants.MAX_LIMIT)
                return RepositoryConstants.MAX_LIMIT;

            return pagingOptions.Limit.Value;
        }

        public static T WithPage<T>(this T options, int? page) where T : IPagingOptions {
            options.Page = page;
            return options;
        }

        public static bool ShouldUsePage<T>(this T options) where T : ICommandOptions {
            var pagingOptions = options as IPagingOptions;
            if (pagingOptions == null)
                return false;

            return pagingOptions.Page.HasValue && pagingOptions.Page.Value > 1;
        }

        public static bool ShouldUseSkip<T>(this T options) where T : ICommandOptions {
            return options.ShouldUsePage();
        }

        public static int? GetPage<T>(this T options) where T : ICommandOptions {
            var pagingOptions = options as IPagingOptions;
            if (pagingOptions == null)
                return null;

            return pagingOptions.Page;
        }

        public static int GetSkip<T>(this T options) where T : ICommandOptions {
            var pagingOptions = options as IPagingOptions;
            if (pagingOptions == null)
                return 0;

            if (pagingOptions.Page == null || pagingOptions.Page.Value < 1)
                return 0;

            int skip = (pagingOptions.Page.Value - 1) * pagingOptions.GetLimit();
            if (skip < 0)
                skip = 0;

            return skip;
        }
    }
}