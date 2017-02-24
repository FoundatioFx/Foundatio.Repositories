using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public interface ICommandOptions : IOptions {}

    public class CommandOptions : OptionsBase, ICommandOptions {}

    [Obsolete("Use CommandOptions")]
    public class PagingOptions : CommandOptions {
        public static implicit operator PagingOptions(int limit) {
            return new PagingOptions().WithLimit(limit);
        }
    }
}
