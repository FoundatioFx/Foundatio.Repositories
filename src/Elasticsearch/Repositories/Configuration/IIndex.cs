using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IIndex {
        int Version { get; }
        string AliasName { get; }
        string VersionedName { get; }
        ICollection<IIndexType> IndexTypes { get; }
        CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx);
    }

    public interface ITimeSeriesIndex : IIndex {
        PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template);
        string GetIndex(DateTime utcDate);
        string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd);
    }

    public class Index : IIndex {
        public Index(string name, int version = 1) {
            AliasName = name;
            Version = version;
            VersionedName = String.Concat(AliasName, "-v", Version);
        }

        public int Version { get; }
        public string AliasName { get; }
        public string VersionedName { get; }

        public ICollection<IIndexType> IndexTypes { get; } = new List<IIndexType>();

        public virtual CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            idx.AddAlias(AliasName);

            foreach (var t in IndexTypes)
                t.ConfigureIndex(idx);
            
            return idx;
        }
    }

    public class MonthlyIndex: Index, ITimeSeriesIndex {
        public MonthlyIndex(string name, int version = 1): base(name, version) {}

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            template.Template(VersionedName + "-*");
            template.AddAlias(AliasName);

            foreach (var t in IndexTypes) {
                var type = t as ITimeSeriesIndexType;
                type?.ConfigureTemplate(template);
            }

            return template;
        }

        public string GetIndex(DateTime utcDate) {
            return $"{VersionedName}-{utcDate:yyyy.MM}";
        }

        public string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = DateTime.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = DateTime.UtcNow;

            var utcEndOfDay = utcEnd.Value.EndOfDay();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value; current <= utcEndOfDay; current = current.AddMonths(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }
    }

    public class DailyIndex : Index, ITimeSeriesIndex {
        public DailyIndex(string name, int version = 1) : base(name, version) { }

        public PutTemplateDescriptor ConfigureTemplate(PutTemplateDescriptor template) {
            template.Template(VersionedName + "-*");
            template.AddAlias(AliasName);

            foreach (var t in IndexTypes) {
                var type = t as ITimeSeriesIndexType;
                type?.ConfigureTemplate(template);
            }

            return template;
        }

        public string GetIndex(DateTime utcDate) {
            return $"{VersionedName}-{utcDate:yyyy.MM.dd}";
        }

        public string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = DateTime.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = DateTime.UtcNow;

            var utcEndOfDay = utcEnd.Value.EndOfDay();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value; current <= utcEndOfDay; current = current.AddDays(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }
    }
}
