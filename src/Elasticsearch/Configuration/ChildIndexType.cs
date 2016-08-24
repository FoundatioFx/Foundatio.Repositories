using System;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IChildIndexType : IIndexType {
        string ParentPath { get; }
    }

    public interface IChildIndexType<T> : IChildIndexType {
        string GetParentId(T document);
    }

    public class ChildIndexType<T> : IndexTypeBase<T>, IChildIndexType<T> where T : class {
        protected readonly Func<T, string> _getParentId;

        public ChildIndexType(string parentPath, Func<T, string> getParentId, string name = null, IIndex index = null): base(index, name) {
            if (getParentId == null)
                throw new ArgumentNullException(nameof(getParentId));

            ParentPath = parentPath;
            _getParentId = getParentId;
        }

        public string ParentPath { get; }

        public virtual string GetParentId(T document) {
            return _getParentId(document);
        }
    }
}