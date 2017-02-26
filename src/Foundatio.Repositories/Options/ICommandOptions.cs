using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public interface ICommandOptions : IOptions {}

    public class CommandOptions : OptionsBase, ICommandOptions {}
}
