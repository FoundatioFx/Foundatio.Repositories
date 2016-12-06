namespace Foundatio.Repositories.Models {
    public class KeyedAggregation<T> : ValueAggregate {
        public T Key { get; set; }
    }
}
