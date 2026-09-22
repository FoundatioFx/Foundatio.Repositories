    /// <summary>
    /// Eagerly collects per-document errors without throwing for valid Elasticsearch item errors.
    /// Ordinary missing documents are not errors. Malformed null response items are rejected.
    /// </summary>
    /// <exception cref="ArgumentNullException">The response is null.</exception>
    /// <exception cref="DocumentException">The response contains an invalid item.</exception>
    public static IReadOnlyCollection<MultiGetError> GetItemErrors<T>(this MultiGetResponse<T> response, ILogger? logger = null) where T : class
    {
        return response.GetItemErrors(null, logger);
    }

    internal static IReadOnlyCollection<MultiGetError> GetItemErrors<T>(this MultiGetResponse<T> response, IReadOnlyList<MultiGetOperation>? operations, ILogger? logger) where T : class
    {
        ArgumentNullException.ThrowIfNull(response);
        if (response.Docs is null || (operations is not null && response.Docs.Count != operations.Count))
            throw new DocumentException("Elasticsearch returned an unexpected number of multi-get items.");

        List<MultiGetError>? errors = null;
        int position = 0;
        foreach (var doc in response.Docs)
        {
            if (doc is null)
                throw new DocumentException("Elasticsearch returned an invalid multi-get item.");

            // Elasticsearch returns items in request order, including failures. Validate metadata,
            // not the source ID, because field projections may intentionally omit the source ID.
            string? expectedId = operations is not null ? operations[position].Id.ToString() : null;
            doc.Match(
                result =>
                {
                    if (result is null)
                        throw new DocumentException("Elasticsearch returned an invalid multi-get result item.");

                    if (operations is not null)
                    {
                        if (!String.Equals(result.Id, expectedId, StringComparison.Ordinal))
                            throw new DocumentException($"Elasticsearch returned an unexpected multi-get result ID; expected {expectedId}, received {result.Id}.");

                        if (result.Found && result.Source is null)
                            throw new DocumentException($"Elasticsearch returned multi-get document {expectedId} without a source.");

                        if (!result.Found && result.Source is not null)
                            throw new DocumentException($"Elasticsearch returned a source for missing multi-get document {expectedId}.");
                    }
                },
                error =>
                {
                    if (error is null || error.Error is null)
                    {
                        logger?.LogWarning("Elasticsearch returned an invalid multi-get error item.");
                        throw new DocumentException("Elasticsearch returned an invalid multi-get error item.");
                    }

                    if (operations is not null && !String.Equals(error.Id?.ToString(), expectedId, StringComparison.Ordinal))
                        throw new DocumentException($"Elasticsearch returned an unexpected multi-get error ID; expected {expectedId}, received {error.Id}.");

                    errors ??= [];
                    errors.Add(error);
                }
            );
            position++;
        }

        return errors ?? (IReadOnlyCollection<MultiGetError>)[];
    }

