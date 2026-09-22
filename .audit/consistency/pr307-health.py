from pathlib import Path
import sys
root=Path(sys.argv[1])
p=root/'src/Foundatio.Repositories.Elasticsearch/Configuration/CompatibilityTargetHealth.cs'
s=p.read_text();old='            if (!response.IsValidResponse)';assert s.count(old)==1
s=s.replace(old, '''            // Elasticsearch returns HTTP 408 for an otherwise valid health observation whose requested
            // status has not been reached yet. Continue bounded polling, never treat this as readiness.
            bool pollTimedOut = response.ApiCallDetails?.HttpStatusCode is 408 && response.TimedOut;
            if (!response.IsValidResponse && !pollTimedOut)''');p.write_text(s)
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.Consistency.cs'
s=p.read_text().replace('new StubResponse(200, yellow), new StubResponse(200, green)', 'new StubResponse(408, yellow), new StubResponse(200, green)');p.write_text(s)
