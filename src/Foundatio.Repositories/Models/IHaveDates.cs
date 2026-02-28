using System;

namespace Foundatio.Repositories.Models;

/// <summary>
/// Indicates that a document tracks both creation and last update timestamps.
/// </summary>
/// <remarks>
/// Repository implementations automatically set <see cref="IHaveCreatedDate.CreatedUtc"/> when a document
/// is first added and update <see cref="UpdatedUtc"/> on each save operation.
/// </remarks>
public interface IHaveDates : IHaveCreatedDate
{
    /// <summary>
    /// Gets or sets the UTC timestamp when this document was last updated.
    /// </summary>
    DateTime UpdatedUtc { get; set; }
}

/// <summary>
/// Indicates that a document tracks its creation timestamp.
/// </summary>
/// <remarks>
/// Repository implementations automatically set <see cref="CreatedUtc"/> when a document is first added.
/// </remarks>
public interface IHaveCreatedDate
{
    /// <summary>
    /// Gets or sets the UTC timestamp when this document was created.
    /// </summary>
    DateTime CreatedUtc { get; set; }
}
