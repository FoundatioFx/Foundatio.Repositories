using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public interface IRepositoryQuery : IOptions {}

    public class Query : OptionsBase, IRepositoryQuery { }
}
