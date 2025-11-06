using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Foundatio.Repositories;

public record struct Id
{
    public static readonly Id Null = new();

    public Id(string id, string routing = null)
    {
        Value = id;
        Routing = routing;
    }

    public string Value { get; }
    public string Routing { get; }

    public static implicit operator Id(string id) => new(id);
    public static implicit operator string(Id id) => id.ToString();

    public override string ToString()
    {
        if (Routing == null)
            return Value;

        return String.Concat(Routing, "-", Value);
    }

}

public class Ids : List<Id>
{
    public static readonly Ids Empty = new();

    public Ids() { }

    public Ids(IEnumerable<string> ids) : base(ids != null ? ids.Select(i => (Id)i) : new Id[] { }) { }

    public Ids(IEnumerable<Id> ids) : base(ids) { }

    public Ids(params string[] ids) : base(ids != null ? ids.Select(i => (Id)i) : new Id[] { }) { }

    public Ids(params Id[] ids) : base(ids) { }

    public static implicit operator Ids(Id id)
    {
        return new Ids { id };
    }

    public static implicit operator Ids(List<string> ids)
    {
        var result = new Ids();

        foreach (string id in ids)
            result.Add(id);

        return result;
    }

    public static implicit operator Ids(HashSet<string> ids)
    {
        var result = new Ids();

        foreach (string id in ids)
            result.Add(id);

        return result;
    }

    public static implicit operator Ids(Collection<string> ids)
    {
        var result = new Ids();

        foreach (string id in ids)
            result.Add(id);

        return result;
    }

    public static implicit operator Ids(string[] ids)
    {
        var result = new Ids();

        foreach (string id in ids)
            result.Add(id);

        return result;
    }

    public static implicit operator List<string>(Ids ids)
    {
        var result = new List<string>();
        for (int i = 0; i < ids.Count; i++)
            result[i] = ids[i].ToString();

        return result;
    }

    public static implicit operator Collection<string>(Ids ids)
    {
        var result = new Collection<string>();
        for (int i = 0; i < ids.Count; i++)
            result[i] = ids[i].ToString();

        return result;
    }

    public static implicit operator string[](Ids ids)
    {
        string[] result = new string[ids.Count];
        for (int i = 0; i < ids.Count; i++)
            result[i] = ids[i].ToString();

        return result;
    }
}
