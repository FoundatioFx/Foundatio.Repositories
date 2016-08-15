namespace Foundatio.Repositories.JsonPatch {
    public abstract class AbstractPatcher<TDoc> where TDoc : class {
        public virtual void Patch(ref TDoc target, PatchDocument document) {
            foreach (var operation in document.Operations) {
                target = ApplyOperation(operation, target);
            }
        }

        public virtual TDoc ApplyOperation(Operation operation, TDoc target) {
            if (operation is AddOperation)
                Add((AddOperation)operation, target);
            else if (operation is CopyOperation)
                Copy((CopyOperation)operation, target);
            else if (operation is MoveOperation)
                Move((MoveOperation)operation, target);
            else if (operation is RemoveOperation)
                Remove((RemoveOperation)operation, target);
            else if (operation is ReplaceOperation)
                target = Replace((ReplaceOperation)operation, target) ?? target;
            else if (operation is TestOperation)
                Test((TestOperation)operation, target);
            return target;
        }

        protected abstract void Add(AddOperation operation, TDoc target);
        protected abstract void Copy(CopyOperation operation, TDoc target);
        protected abstract void Move(MoveOperation operation, TDoc target);
        protected abstract void Remove(RemoveOperation operation, TDoc target);
        protected abstract TDoc Replace(ReplaceOperation operation, TDoc target);
        protected abstract void Test(TestOperation operation, TDoc target);
    }
}