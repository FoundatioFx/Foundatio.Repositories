namespace Foundatio.Repositories.Exceptions;

public class AsyncQueryNotFoundException : RepositoryException
{
    public AsyncQueryNotFoundException() { }

    public AsyncQueryNotFoundException(string id) : base($"Async query \"{id}\" could not be found")
    {
        Id = id;
    }

    public string Id { get; private set; }
}
