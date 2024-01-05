using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class SetValidationOptionsExtensions
    {
        internal const string ValidationKey = "@Validation";

        public static T Validation<T>(this T options, bool shouldValidate) where T : ICommandOptions
        {
            return options.BuildOption(ValidationKey, shouldValidate);
        }

        public static T SkipValidation<T>(this T options) where T : ICommandOptions
        {
            return options.Validation(false);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadValidationOptionsExtensions
    {
        public static bool ShouldValidate(this ICommandOptions options)
        {
            return options.SafeGetOption(SetValidationOptionsExtensions.ValidationKey, true);
        }
    }
}
