using System;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IChildIndexType : IIndexType {
        string ParentPath { get; }
        string ParentIndexTypeName { get; }
        Type ParentType { get; }
    }

    public interface IChildIndexType<T> : IChildIndexType {
        string GetParentId(T document);
    }

    public class ChildIndexType<TChild, TParent> : IndexTypeBase<TChild>, IChildIndexType<TChild> where TChild : class {
        protected readonly Func<TChild, string> _getParentId;

        public ChildIndexType(string parentIndexTypeName, string parentPath, Func<TChild, string> getParentId, string name = null, IIndex index = null): base(index, name) {
            if (getParentId == null)
                throw new ArgumentNullException(nameof(getParentId));

            ParentIndexTypeName = parentIndexTypeName;
            ParentPath = parentPath;
            ParentType = typeof(TParent);
            _getParentId = getParentId;
        }

        public string ParentPath { get; }
        public string ParentIndexTypeName { get; }
        public Type ParentType { get; }

        public virtual string GetParentId(TChild document) {
            return _getParentId(document);
        }
    }
}