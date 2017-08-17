using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    /// <summary>
    /// Query options that control the result of the repository query operations
    /// </summary>
    public interface IRepositoryQuery : IOptions {}
    public interface IRepositoryQuery<T> : IRepositoryQuery where T: class { }

    public class RepositoryQuery : OptionsBase, IRepositoryQuery, ISystemFilter {
        IRepositoryQuery ISystemFilter.GetQuery() {
            return this;
        }
    }

    public class RepositoryQuery<T> : RepositoryQuery, IRepositoryQuery<T> where T : class { }
}
