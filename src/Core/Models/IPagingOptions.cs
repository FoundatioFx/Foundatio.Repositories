namespace Foundatio.Repositories.Models {
    public interface IPagingOptions {
        int? Limit { get; set; }
        int? Page { get; set; }
    }
}