namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Counts task status read failures that happen back to back. A successful read resets the count, so only a
/// sustained outage gives up on a long running task.
/// </summary>
internal sealed class ConsecutiveFailureCounter(int limit)
{
    public int Count { get; private set; }

    public void RecordSuccess() => Count = 0;

    /// <summary>Records a failure and returns <c>true</c> when the limit of consecutive failures was reached.</summary>
    public bool RecordFailure() => ++Count >= limit;
}
