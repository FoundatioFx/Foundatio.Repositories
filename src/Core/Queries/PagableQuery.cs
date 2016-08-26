using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Queries {
    public interface IPagableQuery : IRepositoryQuery {
        IPagingOptions Options { get; set; }
    }

    public static class PagableQueryExtensions {
        public static bool ShouldUseLimit<T>(this T query) where T : IPagableQuery {
            return query.Options?.Limit != null;
        }

        public static bool ShouldUseSkip<T>(this T query) where T : IPagableQuery {
            if (query.Options == null)
                return false;

            return query.Options.Page.HasValue && query.Options.Page.Value > 1;
        }

        public static int GetLimit<T>(this T query) where T : IPagableQuery {
            if (query.Options?.Limit == null || query.Options.Limit.Value < 1)
                return RepositoryConstants.DEFAULT_LIMIT;

            if (query.Options.Limit.Value > RepositoryConstants.MAX_LIMIT)
                return RepositoryConstants.MAX_LIMIT;

            return query.Options.Limit.Value;
        }

        public static int GetSkip<T>(this T query) where T : IPagableQuery {
            if (query.Options?.Page == null || query.Options.Page.Value < 1)
                return 0;

            int skip = (query.Options.Page.Value - 1) * query.GetLimit();
            if (skip < 0)
                skip = 0;

            return skip;
        }

        public static T WithLimit<T>(this T query, int? limit) where T : IPagableQuery {
            if (query.Options == null)
                query.Options = new PagingOptions();

            query.Options.Limit = limit;
            return query;
        }

        public static T WithPage<T>(this T query, int? page) where T : IPagableQuery {
            if (query.Options == null)
                query.Options = new PagingOptions();

            query.Options.Page = page;
            return query;
        }

        public static T WithPaging<T>(this T query, IPagingOptions paging) where T : IPagableQuery {
            if (paging == null)
                return query;

            query.Options = paging;

            return query;
        }
    }
}
