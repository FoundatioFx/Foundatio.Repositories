﻿namespace Foundatio.Repositories {
    public static class SetPagingOptionsExtensions {
        internal const string PageLimitKey = "@PageLimit";
        internal const string PageNumberKey = "@PageNumber";

        public static T PageNumber<T>(this T options, int? page) where T : ICommandOptions {
            if (page.HasValue)
                options.SetOption(PageNumberKey, page.Value);

            return options;
        }

        public static T PageLimit<T>(this T options, int? limit) where T : ICommandOptions {
            if (limit.HasValue)
                options.SetOption(PageLimitKey, limit.Value);

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadPagingOptionsExtensions {
        public static bool ShouldUseLimit(this ICommandOptions options) {
            return options.SafeHasOption(SetPagingOptionsExtensions.PageLimitKey);
        }

        public static int GetLimit(this ICommandOptions options) {
            var limit = options.SafeGetOption(SetPagingOptionsExtensions.PageLimitKey, RepositoryConstants.DEFAULT_LIMIT);

            if (limit > RepositoryConstants.MAX_LIMIT)
                return RepositoryConstants.MAX_LIMIT;

            return limit;
        }

        public static bool ShouldUsePage(this ICommandOptions options) {
            return options.SafeHasOption(SetPagingOptionsExtensions.PageNumberKey);
        }

        public static int GetPage(this ICommandOptions options) {
            return options.SafeGetOption(SetPagingOptionsExtensions.PageNumberKey, 1);
        }

        public static bool ShouldUseSkip(this ICommandOptions options) {
            return options.ShouldUseLimit() && options.GetPage() > 1;
        }

        public static int GetSkip(this ICommandOptions options) {
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