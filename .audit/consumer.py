from pathlib import Path
import re
import subprocess

packages = Path('../consumer-packages').resolve()
consumer = Path('../consumer').resolve()
packages.mkdir(exist_ok=True)
consumer.mkdir(exist_ok=True)
version = '8.0.2-pr308audit.0'
for project in ('Foundatio.Repositories', 'Foundatio.Repositories.Elasticsearch'):
    subprocess.run(['dotnet', 'pack', f'src/{project}/{project}.csproj', '--configuration', 'Release', f'-p:MinVerVersionOverride={version}', '-o', str(packages)], check=True)

(consumer / 'Consumer.csproj').write_text(f'''<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net8.0;net10.0</TargetFrameworks>
    <OutputType>Exe</OutputType>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Foundatio.Repositories.Elasticsearch" Version="{version}" />
  </ItemGroup>
</Project>
''')
fixtures = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/QueryBuilderTestTypes.cs').read_text()
fixtures = fixtures.replace('namespace Foundatio.Repositories.Elasticsearch.Tests;', 'namespace Audit;')
(consumer / 'LegacyIndex.cs').write_text(fixtures)
section = Path('docs/guide/index-management.md').read_text().split('### Externally-Managed Indexes', 1)[1]
example = re.search(r'```csharp\n(.*?)\n\s*```', section, re.S).group(1)
example = '\n'.join(line[3:] if line.startswith('   ') else line for line in example.splitlines())
(consumer / 'ExternalIndex.cs').write_text(example + '\npublic class LogEvent { public DateTimeOffset Date { get; set; } }\n')
(consumer / 'Program.cs').write_text('''using Audit;
using Foundatio.Repositories;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Serializer;

IIndex index = new FakeIndex { MappingResolver = null! };
if (!index.HasSortableIdField)
    throw new InvalidOperationException("Legacy IIndex implementation lost its default.");
var options = new CommandOptions<NonIdentityDocument>().SearchAfter("after");
object[]? after = options.GetSearchAfter();
options.SearchBefore("before");
object[]? before = options.GetSearchBefore();
var serializer = new SystemTextJsonSerializer();
object[]? decoded = FindHitExtensions.DecodeSortToken("W251bGxd", serializer);
object[]? sorts = new FindHit<NonIdentityDocument>(null, null, 0).GetSorts();
options.SearchAfter((object?[]?)null);
if (after is not ["after"] || before is not ["before"] || decoded is not [null] || options.HasSearchBefore() || options.HasSearchAfter())
    throw new InvalidOperationException("Cursor API compatibility failed.");
Console.WriteLine($"Packaged consumer passed on {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
''')
subprocess.run(['dotnet', 'restore', str(consumer / 'Consumer.csproj'), '--source', str(packages), '--source', 'https://api.nuget.org/v3/index.json'], check=True)
subprocess.run(['dotnet', 'build', str(consumer / 'Consumer.csproj'), '--configuration', 'Release', '--no-restore'], check=True)
for framework in ('net8.0', 'net10.0'):
    subprocess.run(['dotnet', str(consumer / 'bin' / 'Release' / framework / 'Consumer.dll')], check=True)
