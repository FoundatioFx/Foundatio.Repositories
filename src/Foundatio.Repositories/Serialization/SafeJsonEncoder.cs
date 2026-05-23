using System.Text.Encodings.Web;
using System.Text.Unicode;

namespace Foundatio.Repositories.Serialization;

/// <summary>
/// Provides a pre-configured <see cref="JavaScriptEncoder"/> that balances security with
/// readability for Foundatio.Repositories document serialization.
/// </summary>
/// <remarks>
/// <para>
/// The default <see cref="JavaScriptEncoder.Default"/> aggressively escapes non-ASCII characters,
/// which bloats stored documents when they contain CJK, emoji, or accented text. However, using
/// <see cref="JavaScriptEncoder.UnsafeRelaxedJsonEscaping"/> disables HTML-sensitive character
/// escaping (&lt;, &gt;, &amp;, '), creating XSS risk if JSON is embedded in HTML.
/// </para>
/// <para>
/// This encoder allows all Unicode ranges (preserving BMP non-ASCII) while still escaping
/// HTML-sensitive characters, matching the behavior recommended by Microsoft for
/// "defense in depth" scenarios. Note that supplementary-plane characters (code points above
/// U+FFFF) are still escaped as UTF-16 surrogate pairs by the underlying encoder.
/// </para>
/// </remarks>
public static class SafeJsonEncoder
{
    /// <summary>
    /// A <see cref="JavaScriptEncoder"/> that passes through BMP non-ASCII characters (CJK, accented
    /// Latin, BMP emoji, etc.) while escaping HTML-sensitive characters (&lt;, &gt;, &amp;, ')
    /// to their <c>\uXXXX</c> equivalents. Supplementary-plane characters (above U+FFFF, such as
    /// most emoji) are still escaped as surrogate pairs (<c>\uD800\uDC00</c> form) due to the
    /// underlying <see cref="JavaScriptEncoder"/> implementation.
    /// </summary>
    public static JavaScriptEncoder Instance { get; } = JavaScriptEncoder.Create(UnicodeRanges.All);
}
