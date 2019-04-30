namespace Foundatio.Repositories.Models {
    public interface IVersioned {
        /// <summary>
        /// Current modification version for the document.
        /// </summary>
        long Version { get; set; }
        // TODO: Convert this to a string and make it composed from sequence number and primary term.
    }
}
