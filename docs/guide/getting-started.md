# Getting Started

This guide will help you install Foundatio.Repositories and create your first repository.

## Prerequisites

- .NET 8.0 or later
- Elasticsearch 7.x or 8.x (for the Elasticsearch implementation)
- Basic understanding of the repository pattern

## Installation

Install the NuGet packages:

::: code-group

```bash [.NET CLI]
dotnet add package Foundatio.Repositories.Elasticsearch
```

```bash [Package Manager]
Install-Package Foundatio.Repositories.Elasticsearch
```

```xml [PackageReference]
<PackageReference Include="Foundatio.Repositories.Elasticsearch" Version="*" />
```

:::

The `Foundatio.Repositories.Elasticsearch` package includes the core `Foundatio.Repositories` package as a dependency.

## Quick Start

### 1. Define Your Entity

Create a model class that implements the required interfaces:

```csharp
using Foundatio.Repositories.Models;

public class Employee : IIdentity, IHaveDates
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public int Age { get; set; }
    public string CompanyId { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
}
```

**Available interfaces:**

| Interface | Purpose |
|-----------|---------|
| `IIdentity` | Provides `Id` property (required) |
| `IHaveCreatedDate` | Provides `CreatedUtc` property |
| `IHaveDates` | Provides `CreatedUtc` and `UpdatedUtc` properties |
| `ISupportSoftDeletes` | Provides `IsDeleted` property for soft delete support |
| `IVersioned` | Provides `Version` property for optimistic concurrency |

### 2. Create an Index Configuration

Define how your entity is indexed in Elasticsearch:

```csharp
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;

public sealed class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration) 
        : base(configuration, "employees", version: 1) { }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        return base.ConfigureIndex(idx.Settings(s => s
            .NumberOfReplicas(0)
            .NumberOfShards(1)));
    }

    public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(
        TypeMappingDescriptor<Employee> map)
    {
        return map
            .Dynamic(false)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(f => f.Name(e => e.CompanyId))
                .Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())
                .Text(f => f.Name(e => e.Email).AddKeywordAndSortFields())
                .Number(f => f.Name(e => e.Age).Type(NumberType.Integer))
            );
    }
}
```

### 3. Create the Elasticsearch Configuration

Set up the connection to Elasticsearch:

```csharp
using Elasticsearch.Net;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Microsoft.Extensions.Logging;
using Nest;

public class MyElasticConfiguration : ElasticConfiguration
{
    public MyElasticConfiguration(ILoggerFactory loggerFactory) 
        : base(loggerFactory: loggerFactory)
    {
        AddIndex(Employees = new EmployeeIndex(this));
    }

    protected override IConnectionPool CreateConnectionPool()
    {
        return new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
    }

    public EmployeeIndex Employees { get; }
}
```

### 4. Create the Repository Interface and Implementation

```csharp
using Foundatio.Repositories;
using Foundatio.Repositories.Elasticsearch;

public interface IEmployeeRepository : ISearchableRepository<Employee> { }

public class EmployeeRepository : ElasticRepositoryBase<Employee>, IEmployeeRepository
{
    public EmployeeRepository(MyElasticConfiguration configuration) 
        : base(configuration.Employees) { }
}
```

### 5. Register Services and Use

Register your services with dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Register logging
services.AddLogging();

// Register Elasticsearch configuration
services.AddSingleton<MyElasticConfiguration>();

// Register repository
services.AddSingleton<IEmployeeRepository, EmployeeRepository>();

var provider = services.BuildServiceProvider();
```

Configure the indexes (creates them if they don't exist):

```csharp
var config = provider.GetRequiredService<MyElasticConfiguration>();
await config.ConfigureIndexesAsync();
```

Use the repository:

```csharp
var repository = provider.GetRequiredService<IEmployeeRepository>();

// Add an employee
var employee = await repository.AddAsync(new Employee
{
    Name = "John Doe",
    Email = "john@example.com",
    Age = 30,
    CompanyId = "acme"
}, o => o.ImmediateConsistency());

Console.WriteLine($"Created employee with ID: {employee.Id}");

// Find employees
var results = await repository.FindAsync(q => q
    .FilterExpression("age:>=25")
    .SortExpression("name"));

Console.WriteLine($"Found {results.Total} employees");

// Update an employee
employee.Age = 31;
await repository.SaveAsync(employee);

// Delete an employee
await repository.RemoveAsync(employee);
```

## ASP.NET Core Integration

For ASP.NET Core applications, configure indexes on startup:

```csharp
using Foundatio.Extensions.Hosting.Startup;

var builder = WebApplication.CreateBuilder(args);

// Register services
builder.Services.AddSingleton<MyElasticConfiguration>();
builder.Services.AddSingleton<IEmployeeRepository, EmployeeRepository>();

// Configure indexes on startup
builder.Services.AddStartupAction("ConfigureIndexes", async sp =>
{
    var configuration = sp.GetRequiredService<MyElasticConfiguration>();
    await configuration.ConfigureIndexesAsync();
});

var app = builder.Build();

// Wait for startup actions before serving requests
app.UseWaitForStartupActionsBeforeServingRequests();

app.Run();
```

## Running Elasticsearch

For local development, use Docker:

```bash
docker run -d --name elasticsearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  docker.elastic.co/elasticsearch/elasticsearch:8.11.0
```

Or use the provided `docker-compose.yml`:

```bash
docker compose up -d
```

## Next Steps

- [Repository Pattern](/guide/repository-pattern) - Learn about the core interfaces and event handlers
- [Elasticsearch Setup](/guide/elasticsearch-setup) - Advanced Elasticsearch configuration
- [CRUD Operations](/guide/crud-operations) - Detailed guide to data operations
- [Querying](/guide/querying) - Build dynamic queries with filters and aggregations

## ID Generation

Foundatio.Repositories includes an `ObjectId` utility for generating unique, sortable IDs (similar to MongoDB's ObjectId):

```csharp
using Foundatio.Repositories.Utility;

// Generate a new ID
string id = ObjectId.GenerateNewId().ToString();
// Example: "507f1f77bcf86cd799439011"

// IDs are time-sortable
var id1 = ObjectId.GenerateNewId();
var id2 = ObjectId.GenerateNewId();
// id2 > id1 (chronologically)

// Extract creation time from ID
var objectId = new ObjectId("507f1f77bcf86cd799439011");
DateTime createdAt = objectId.CreationTime;
```

By default, when you add a document without an ID, the repository will generate one automatically. You can customize ID generation by setting the ID before calling `AddAsync`:

```csharp
var employee = new Employee
{
    Id = ObjectId.GenerateNewId().ToString(),  // Custom ID
    Name = "John Doe"
};
await repository.AddAsync(employee);
```
