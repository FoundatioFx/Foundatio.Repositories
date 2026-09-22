#!/usr/bin/env bash
set -euo pipefail
feed="$RUNNER_TEMP/pr307-packages"
consumer="$RUNNER_TEMP/pr307-consumer"
mkdir -p "$feed" "$consumer"
for project in Foundatio.Repositories Foundatio.Repositories.Elasticsearch; do
  dotnet pack "src/$project/$project.csproj" -c Release --no-build --no-restore -p:ReferenceFoundatioSource=false -p:ReferenceFoundatioRepositoriesSource=false -o "$feed"
done
cat > "$consumer/Consumer.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net8.0;net10.0</TargetFrameworks>
    <OutputType>Exe</OutputType>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <LangVersion>latest</LangVersion>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>
  <ItemGroup><PackageReference Include="Foundatio.Repositories.Elasticsearch" Version="$MINVERVERSIONOVERRIDE" /></ItemGroup>
</Project>
EOF
cat > "$consumer/Program.cs" <<'CS'
using Foundatio.Repositories.Elasticsearch.Configuration;
using var configuration = new ElasticConfiguration();
var index = new ConsumerIndex(configuration);
configuration.AddIndex(index);
IElasticConfigurationCompatibility compatibility = configuration;
await compatibility.UpgradeIndexCompatibilityAsync(Array.Empty<IIndex>(), progressCallbackAsync: (_, _) => Task.CompletedTask);
Console.WriteLine("External packaged consumer passed.");
sealed class ConsumerIndex(IElasticConfiguration configuration) : Foundatio.Repositories.Elasticsearch.Configuration.Index<object>(configuration, "consumer")
{
    protected override string GetCompatibilityIndexPattern() => "consumer-*";
    protected override bool IsNativeIndexName(ReadOnlySpan<char> sourceIndex) => sourceIndex.StartsWith("consumer-", StringComparison.Ordinal);
}
CS
dotnet restore "$consumer/Consumer.csproj" --source "$feed" --source https://api.nuget.org/v3/index.json | tee .audit/results/consumer-restore.log
dotnet build "$consumer/Consumer.csproj" -c Release --no-restore | tee .audit/results/consumer-build.log
for framework in net8.0 net10.0; do
  dotnet run --project "$consumer/Consumer.csproj" -c Release -f "$framework" --no-build | tee ".audit/results/consumer-$framework.log"
done
