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