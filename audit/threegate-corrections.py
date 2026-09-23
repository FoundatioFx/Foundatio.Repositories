from pathlib import Path

def replace(path, old, new, count=1):
    p=Path(path)
    s=p.read_text()
    assert s.count(old)==count,(path,old,s.count(old))
    p.write_text(s.replace(old,new))

replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexDispatchTests.cs', 'new SystemTextJsonSerializer()', 'new Foundatio.Serializer.SystemTextJsonSerializer()', 2)
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs', 'GetAliasAsync(partition, TestCancellationToken)', 'GetAliasAsync(Indices.Index(partition), TestCancellationToken)')
