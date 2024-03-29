﻿using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories;
/// <summary>
/// Query options that control the result of the repository query operations
/// </summary>
public interface IRepositoryQuery : IOptions
{
    Type GetDocumentType();
}

public interface IRepositoryQuery<T> : IRepositoryQuery where T : class { }

public interface ISetDocumentType
{
    void SetDocumentType(Type documentType);
}

public class RepositoryQuery : OptionsBase, IRepositoryQuery, ISystemFilter, ISetDocumentType
{
    protected Type _documentType;

    public RepositoryQuery()
    {
        _documentType = typeof(object);
    }

    public RepositoryQuery(Type documentType)
    {
        _documentType = documentType ?? typeof(object);
    }

    IRepositoryQuery ISystemFilter.GetQuery()
    {
        return this;
    }

    public Type GetDocumentType()
    {
        return _documentType;
    }

    void ISetDocumentType.SetDocumentType(Type documentType)
    {
        _documentType = documentType;
    }
}

public class RepositoryQuery<T> : RepositoryQuery, ISetDocumentType, IRepositoryQuery<T> where T : class
{
    public RepositoryQuery()
    {
        _documentType = typeof(T);
    }

    void ISetDocumentType.SetDocumentType(Type documentType) { }
}

public static class RepositoryQueryExtensions
{
    public static T DocumentType<T>(this T query, Type documentType) where T : IRepositoryQuery
    {
        if (query is ISetDocumentType setDocumentType)
            setDocumentType.SetDocumentType(documentType);

        return query;
    }

    public static IRepositoryQuery<T> As<T>(this IRepositoryQuery query) where T : class
    {
        if (query == null)
            return new RepositoryQuery<T>();

        if (query is IRepositoryQuery<T> typedQuery)
            return typedQuery;

        return new WrappedRepositoryQuery<T>(query);
    }

    public static IRepositoryQuery Unwrap(this IRepositoryQuery query)
    {
        if (query == null)
            return new RepositoryQuery();

        if (query is WrappedRepositoryQuery wrappedQuery)
            return wrappedQuery.InnerQuery;

        return query;
    }
}

internal class WrappedRepositoryQuery<T> : WrappedRepositoryQuery, IRepositoryQuery<T> where T : class
{
    public WrappedRepositoryQuery(IRepositoryQuery innerQuery) : base(innerQuery) { }
}

internal class WrappedRepositoryQuery : IRepositoryQuery
{
    public WrappedRepositoryQuery(IRepositoryQuery innerQuery)
    {
        InnerQuery = innerQuery;
    }

    public IOptionsDictionary Values => InnerQuery.Values;

    public IRepositoryQuery InnerQuery { get; }

    public Type GetDocumentType()
    {
        return InnerQuery.GetDocumentType();
    }
}
