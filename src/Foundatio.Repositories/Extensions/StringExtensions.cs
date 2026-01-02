using System;

namespace Foundatio.Repositories.Extensions;

public static class StringExtensions
{
    public static bool IsNumeric(this string value)
    {
        if (String.IsNullOrEmpty(value))
            return false;

        for (int i = 0; i < value.Length; i++)
        {
            if (Char.IsNumber(value[i]))
                continue;

            if (i == 0 && value[i] == '-')
                continue;

            return false;
        }

        return true;
    }
}
