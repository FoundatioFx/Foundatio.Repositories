using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SetPagingOptionsExtensions {
        internal const string PageLimitKey = "@PageLimit";
        internal const string PageNumberKey = "@PageNumber";

        public static T UsePaging<T>(this T options, int limit, int? page = null) where T : ICommandOptions {
            options.SetOption(PageLimitKey, limit);
            if (page.HasValue)
                options.SetOption(PageNumberKey, page.Value);

            return options;
        }

        public static T WithPage<T>(this T options, int? page) where T : ICommandOptions {
            if (page.HasValue)
                options.SetOption(PageNumberKey, page.Value);

            return options;
        }

        public static T WithLimit<T>(this T options, int? limit) where T : ICommandOptions {
            if (limit.HasValue)
                options.SetOption(PageLimitKey, limit.Value);

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadPagingOptionsExtensions {
        public static bool ShouldUseLimit<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return false;

            return options.HasOption(SetPagingOptionsExtensions.PageLimitKey);
        }

        public static int GetLimit<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return RepositoryConstants.DEFAULT_LIMIT;

            var limit = options.GetOption(SetPagingOptionsExtensions.PageLimitKey, RepositoryConstants.DEFAULT_LIMIT);

            if (limit > RepositoryConstants.MAX_LIMIT)
                return RepositoryConstants.MAX_LIMIT;

            return limit;
        }

        public static bool ShouldUsePage<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return false;

            return options.HasOption(SetPagingOptionsExtensions.PageNumberKey);
        }

        public static int GetPage<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return 1;

            return options.GetOption(SetPagingOptionsExtensions.PageNumberKey, 1);
        }

        public static bool ShouldUseSkip<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return false;

            return options.ShouldUseLimit() && options.GetPage() > 1;
        }

        public static int GetSkip<T>(this T options) where T : ICommandOptions {
            if (options == null)
                return 0;

            if (!options.ShouldUseLimit() && !options.ShouldUsePage())
                return 0;

            int limit = options.GetLimit();
            int page = options.GetPage();

            int skip = (page - 1) * limit;
            if (skip < 0)
                skip = 0;

            return skip;
        }
    }
}
