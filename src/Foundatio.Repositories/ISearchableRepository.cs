using Foundatio.Repositories.Models;

namespace Foundatio.Repositories {
    public interface ISearchableRepository<T> : IRepository<T>, ISearchableReadOnlyRepository<T> where T : class, IIdentity, new() {}
}