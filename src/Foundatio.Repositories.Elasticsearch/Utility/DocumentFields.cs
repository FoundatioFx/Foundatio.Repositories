using System;
using System.Linq.Expressions;
using System.Reflection;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Resolves standard interface properties to their concrete members, leaving names to the client's inferrer.</summary>
internal static class DocumentFields<T> where T : class
{
    public static readonly Expression<Func<T, object?>>? Id = Resolve<IIdentity>(nameof(IIdentity.Id));
    public static readonly Expression<Func<T, object?>>? CreatedUtc = Resolve<IHaveCreatedDate>(nameof(IHaveCreatedDate.CreatedUtc));
    public static readonly Expression<Func<T, object?>>? UpdatedUtc = Resolve<IHaveDates>(nameof(IHaveDates.UpdatedUtc));
    public static readonly Expression<Func<T, object?>>? IsDeleted = Resolve<ISupportSoftDeletes>(nameof(ISupportSoftDeletes.IsDeleted));

    private static Expression<Func<T, object?>>? Resolve<TInterface>(string propertyName)
    {
        if (!typeof(TInterface).IsAssignableFrom(typeof(T)))
            return null;

        var interfaceProperty = typeof(TInterface).GetProperty(propertyName)!;
        PropertyInfo? concreteProperty = null;
        if (!typeof(T).IsInterface)
        {
            var map = typeof(T).GetInterfaceMap(typeof(TInterface));
            var getter = map.TargetMethods[Array.IndexOf(map.InterfaceMethods, interfaceProperty.GetMethod!)];
            concreteProperty = FindConcreteProperty(getter);
        }

        var document = Expression.Parameter(typeof(T), "document");
        Expression owner = concreteProperty is not null ? document : Expression.Convert(document, typeof(TInterface));
        var member = Expression.Property(owner, concreteProperty ?? interfaceProperty);
        return Expression.Lambda<Func<T, object?>>(Expression.Convert(member, typeof(object)), document);
    }

    private static PropertyInfo? FindConcreteProperty(MethodInfo implementation)
    {
        for (var type = typeof(T); type is not null; type = type.BaseType)
        {
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                if (property.GetMethod is not { IsPublic: true } getter)
                    continue;

                if (getter == implementation || (getter.IsVirtual && implementation.IsVirtual && getter.GetBaseDefinition() == implementation.GetBaseDefinition()))
                    return property;
            }
        }

        return null;
    }
}
