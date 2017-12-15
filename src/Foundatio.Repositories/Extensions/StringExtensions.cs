using System;
using System.Text;

namespace Foundatio.Repositories.Extensions {
    public static class StringExtensions {
        public static string ToJTokenPath(this string path) {
            if (path.StartsWith("$"))
                return path;

            var sb = new StringBuilder();
            var parts = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < parts.Length; i++) {
                if (parts[i].IsNumeric())
                    sb.Append("[").Append(parts[i]).Append("]");
                else {
                    sb.Append(parts[i]);
                }

                if (i < parts.Length - 1 && !parts[i + 1].IsNumeric())
                    sb.Append(".");
            }

            return sb.ToString();
        }

        public static bool IsNumeric(this string value) {
            if (String.IsNullOrEmpty(value))
                return false;

            for (int i = 0; i < value.Length; i++) {
                if (Char.IsNumber(value[i]))
                    continue;

                if (i == 0 && value[i] == '-')
                    continue;

                return false;
            }

            return true;
        }
    }
}
