﻿using Foundatio.Utility;

namespace Foundatio.Repositories.Extensions;

public static class FindResultsExtensions
{
    public static string GetAsyncQueryId(this IHaveData results)
    {
        return results.Data.GetString(AsyncQueryDataKeys.AsyncQueryId, null);
    }

    /// <summary>
    /// Whether the query completed successfully or was interrupted (will also be true while the query is still running)
    /// </summary>
    public static bool IsAsyncQueryPartial(this IHaveData results)
    {
        return results.Data.GetBoolean(AsyncQueryDataKeys.IsPartial, false);
    }

    /// <summary>
    /// Whether the query is still running
    /// </summary>
    public static bool IsAsyncQueryRunning(this IHaveData results)
    {
        return results.Data.GetBoolean(AsyncQueryDataKeys.IsRunning, false);
    }
}

public static class AsyncQueryDataKeys
{
    public const string AsyncQueryId = "@AsyncQueryId";
    public const string IsRunning = "@IsRunning";
    public const string IsPartial = "@IsPending";
}
