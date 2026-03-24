namespace Foundatio.Repositories.Exceptions;

/// <summary>
/// Thrown when a repository query contains an invalid field condition that would produce
/// incorrect or empty results at query execution time.
/// </summary>
/// <remarks>
/// <para>Common causes include:</para>
/// <list type="bullet">
///   <item>Using <c>FieldEquals</c>/<c>FieldNotEquals</c> on an analyzed text field
///   that has no <c>.keyword</c> sub-field (TermQuery on analyzed fields almost never matches).</item>
///   <item>Using <c>FieldContains</c>/<c>FieldNotContains</c> on a non-analyzed (keyword) field.</item>
///   <item>Targeting the <c>IsDeleted</c> field on a soft-delete entity with <c>ActiveOnly</c> mode,
///   which creates a contradictory filter.</item>
///   <item>Passing a collection value to a range operator (ranges require scalar values).</item>
///   <item>Passing a null value to a range operator (null range bounds are meaningless).</item>
/// </list>
/// <para>Check the <see cref="Field"/> and <see cref="Operator"/> properties to identify
/// the offending condition, and read the exception message for the specific fix.</para>
/// </remarks>
public class QueryValidationException : RepositoryException
{
    public QueryValidationException() { }

    public QueryValidationException(string message) : base(message) { }

    public QueryValidationException(string message, string field, string op = null) : base(message)
    {
        Field = field;
        Operator = op;
    }

    /// <summary>The resolved Elasticsearch field name that caused the validation failure.</summary>
    public string Field { get; }

    /// <summary>The comparison operator that was used (e.g., "Equals", "Contains", "GreaterThan"), if applicable.</summary>
    public string Operator { get; }
}
