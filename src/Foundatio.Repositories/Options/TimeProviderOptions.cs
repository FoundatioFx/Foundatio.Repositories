using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class SetTimeProviderOptionsExtensions
    {
        internal const string TimeProviderKey = "@TimeProvider";

        public static T TimeProvider<T>(this T options, TimeProvider timeProvider) where T : IOptions
        {
            return options.BuildOption(TimeProviderKey, timeProvider);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadTimeProviderOptionsExtensions
    {
        public static TimeProvider GetTimeProvider(this IOptions options)
        {
            return options.SafeGetOption(SetTimeProviderOptionsExtensions.TimeProviderKey, TimeProvider.System);
        }
    }
}
