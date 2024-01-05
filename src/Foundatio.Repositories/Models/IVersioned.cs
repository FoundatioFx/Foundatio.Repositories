namespace Foundatio.Repositories.Models;

public interface IVersioned
{
    /// <summary>
    /// Current modification version for the document.
    /// </summary>
    string Version { get; set; }
}
