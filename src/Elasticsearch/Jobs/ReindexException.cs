using System;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class ReindexException : Exception {
        public ReindexException(string message, Exception ex) : base(message, ex) {
        }
    }
}
