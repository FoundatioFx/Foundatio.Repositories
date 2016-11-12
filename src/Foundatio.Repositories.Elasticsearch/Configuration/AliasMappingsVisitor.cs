using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class AliasMappingVisitor : NoopMappingVisitor {
        private readonly Inferrer _inferrer;
        private readonly Stack<AliasMapValue> _stack = new Stack<AliasMapValue>();

        public AliasMappingVisitor(Inferrer inferrer) {
            _inferrer = inferrer;
        }

        public AliasMap RootAliasMap { get; } = new AliasMap();

        public override void Visit(ITextProperty property) => AddAlias(property);

        public override void Visit(IKeywordProperty property) => AddAlias(property);

        public override void Visit(IDateProperty property) => AddAlias(property);

        public override void Visit(IBooleanProperty property) => AddAlias(property);

        public override void Visit(IBinaryProperty property) => AddAlias(property);

        public override void Visit(IObjectProperty property) => AddAlias(property);

        public override void Visit(INestedProperty property) => AddAlias(property);

        public override void Visit(IIpProperty property) => AddAlias(property);

        public override void Visit(IGeoPointProperty property) => AddAlias(property);

        public override void Visit(IGeoShapeProperty property) => AddAlias(property);

        public override void Visit(IAttachmentProperty property) => AddAlias(property);

        public override void Visit(INumberProperty property) => AddAlias(property);

        public override void Visit(ICompletionProperty property) => AddAlias(property);

        public override void Visit(IMurmur3HashProperty property) => AddAlias(property);

        public override void Visit(ITokenCountProperty property) => AddAlias(property);

        private void AddAlias(IProperty property) {
            while (Depth > _stack.Count)
                _stack.Pop();

            string name = _inferrer.PropertyName(property.Name);
            var aliasMap = new AliasMapValue { Name = property.GetAlias() };
            if (Depth == 0)
                RootAliasMap.Add(name, aliasMap);
            else
                _stack.Peek().ChildMap.Add(name, aliasMap);

            bool hasChildren = property is IObjectProperty;
            if (hasChildren)
                _stack.Push(aliasMap);
        }
    }
}