using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    /// <summary>
    /// Options that control the behavior of the repositories
    /// </summary>
    public interface ICommandOptions : IOptions {}
    public interface ICommandOptions<T> : ICommandOptions where T: class { }

    public class CommandOptions : OptionsBase, ICommandOptions {}
    public class CommandOptions<T> : CommandOptions, ICommandOptions<T> where T: class { }
}

namespace Foundatio.Repositories.Options {
    public static class CommandOptionsExtensions {
        public static ICommandOptions<T> As<T>(this ICommandOptions options) where T : class {
            if (options == null)
                return new CommandOptions<T>();

            if (options is ICommandOptions<T> typedOptions)
                return typedOptions;

            var o = new CommandOptions<T> {
                Values = options.Values
            };

            return o;
        }
    }
}
