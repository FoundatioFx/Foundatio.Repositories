using System;

namespace Foundatio.Repositories.Models {
    public class PagingOptions : IPagingOptions {
        public int? Limit { get; set; }
        public int? Page { get; set; }

        public static implicit operator PagingOptions(int limit) {
            return new PagingOptions { Limit = limit };
        }
    }

    public static class PagingOptionsExtensions {
        public static PagingOptions WithLimit(this PagingOptions options, int? limit) {
            options.Limit = limit;
            return options;
        }

        public static PagingOptions WithPage(this PagingOptions options, int? page) {
            options.Page = page;
            return options;
        }
    }
}
