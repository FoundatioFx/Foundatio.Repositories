```

BenchmarkDotNet v0.15.8, macOS 27.2 (26B123) [Darwin 27.2.0]
Apple M1 Max, 1 CPU, 10 logical and 10 physical cores
.NET SDK 11.0.100
  [Host] : .NET 10.0.14 (10.0.14, 10.0.1426.52326), Arm64 RyuJIT armv8.0-a

Job=ShortRun  Toolchain=InProcessEmitToolchain  InvocationCount=1  
IterationCount=10  LaunchCount=1  UnrollFactor=1  
WarmupCount=2  

```
| Method       | ConflictingDocuments | Mean        | Error      | StdDev     | Ratio | RatioSD | Gen0      | Allocated   | Alloc Ratio |
|------------- |--------------------- |------------:|-----------:|-----------:|------:|--------:|----------:|------------:|------------:|
| **PerIdRetry**   | **10**                   |    **48.76 ms** |   **8.494 ms** |   **5.618 ms** |  **1.01** |    **0.15** |         **-** |   **500.78 KB** |        **1.00** |
| GroupedRetry | 10                   |    79.73 ms |  13.690 ms |   9.055 ms |  1.65 |    0.25 |         - |   232.71 KB |        0.46 |
|              |                      |             |            |            |       |         |           |             |             |
| **PerIdRetry**   | **100**                  |   **276.40 ms** |  **27.669 ms** |  **16.466 ms** |  **1.00** |    **0.08** |         **-** |  **4196.67 KB** |        **1.00** |
| GroupedRetry | 100                  |    76.48 ms |  11.390 ms |   7.534 ms |  0.28 |    0.03 |         - |  1356.12 KB |        0.32 |
|              |                      |             |            |            |       |         |           |             |             |
| **PerIdRetry**   | **500**                  | **2,569.35 ms** | **395.139 ms** | **261.360 ms** |  **1.01** |    **0.14** | **3000.0000** | **20784.04 KB** |        **1.00** |
| GroupedRetry | 500                  |   103.07 ms |  13.660 ms |   9.035 ms |  0.04 |    0.01 | 1000.0000 |  6331.25 KB |        0.30 |
