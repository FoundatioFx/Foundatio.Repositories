namespace Foundatio.Repositories.Utility;

public abstract class AbstractPatcher<TDoc> where TDoc : class
{
    public virtual void Patch(ref TDoc target, PatchDocument document)
    {
        foreach (var operation in document.Operations)
        {
            target = ApplyOperation(operation, target);
        }
    }

    public virtual TDoc ApplyOperation(Operation operation, TDoc target)
    {
        switch (operation)
        {
            case AddOperation add:
                Add(add, target);
                break;
            case CopyOperation copy:
                Copy(copy, target);
                break;
            case MoveOperation move:
                Move(move, target);
                break;
            case RemoveOperation remove:
                Remove(remove, target);
                break;
            case ReplaceOperation replace:
                target = Replace(replace, target) ?? target;
                break;
            case TestOperation test:
                Test(test, target);
                break;
        }
        return target;
    }

    protected abstract void Add(AddOperation operation, TDoc target);
    protected abstract void Copy(CopyOperation operation, TDoc target);
    protected abstract void Move(MoveOperation operation, TDoc target);
    protected abstract void Remove(RemoveOperation operation, TDoc target);
    protected abstract TDoc Replace(ReplaceOperation operation, TDoc target);
    protected abstract void Test(TestOperation operation, TDoc target);
}
