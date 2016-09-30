using System;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IChildIndexType : IIndexType {
        IIndexType GetParentIndexType();
    }

    public interface IChildIndexType<TChild> : IChildIndexType {
        string GetParentId(TChild document);
    }

    public interface IChildIndexType<TChild, TParent> : IChildIndexType<TChild> where TParent : class {
        IIndexType<TParent> ParentIndexType { get; }
    }

    public class ChildIndexType<TChild, TParent> : IndexTypeBase<TChild>, IChildIndexType<TChild, TParent> where TChild : class where TParent : class {
        protected readonly Func<TChild, string> _getParentId;
        private readonly Lazy<IIndexType<TParent>> _parentIndexType;

        public ChildIndexType(Func<TChild, string> getParentId, IIndex index, string name = null): base(index, name) {
            if (getParentId == null)
                throw new ArgumentNullException(nameof(getParentId));

            _getParentId = getParentId;
            _parentIndexType = new Lazy<IIndexType<TParent>>(() => Configuration.GetIndexType<TParent>());
        }

        public IIndexType GetParentIndexType() => _parentIndexType.Value;
        public IIndexType<TParent> ParentIndexType => _parentIndexType.Value;

        public virtual string GetParentId(TChild document) {
            return _getParentId(document);
        }
    }
}