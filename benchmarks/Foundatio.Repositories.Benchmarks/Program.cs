using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Foundatio.Repositories.Benchmarks.ConflictRetryBenchmarks).Assembly).Run(args);
