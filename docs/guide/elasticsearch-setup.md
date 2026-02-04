# Elasticsearch Setup

This guide covers configuring Foundatio.Repositories for Elasticsearch, including connection setup, index configuration, and advanced options.

## Elasticsearch Configuration

The `ElasticConfiguration` class manages your Elasticsearch connection and indexes.

### Basic Configuration

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
        // Register indexes
        AddIndex(Employees = new EmployeeIndex(this));
        AddIndex(Projects = new ProjectIndex(this));
    }

    protected override IConnectionPool CreateConnectionPool()
    {
        return new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
    }

    public EmployeeIndex Employees { get; }
    public ProjectIndex Projects { get; }
}
```

### Connection Pool Options

#### Single Node

For development or single-node clusters:

```csharp
protected override IConnectionPool CreateConnectionPool()
{
    return new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
}
```

#### Multiple Nodes

For production clusters with multiple nodes:

```csharp
protected override IConnectionPool CreateConnectionPool()
{
    var nodes = new[]
    {
        new Uri("http://es-node1:9200"),
        new Uri("http://es-node2:9200"),
        new Uri("http://es-node3:9200")
    };
    return new StaticConnectionPool(nodes);
}
```

#### Sniffing Connection Pool

Automatically discovers cluster nodes:

```csharp
protected override IConnectionPool CreateConnectionPool()
{
    var nodes = new[] { new Uri("http://es-node1:9200") };
    return new SniffingConnectionPool(nodes);
}
```

### Connection Settings

Override `ConfigureSettings` to customize the NEST client:

```csharp
protected override void ConfigureSettings(ConnectionSettings settings)
{
    base.ConfigureSettings(settings);
    
    // Enable detailed logging in development
    if (_environment.IsDevelopment())
    {
        settings.DisableDirectStreaming();
        settings.PrettyJson();
        settings.EnableDebugMode();
    }
    
    // Set default timeout
    settings.RequestTimeout(TimeSpan.FromSeconds(30));
    
    // Configure authentication
    settings.BasicAuthentication("username", "password");
    
    // Or use API key
    settings.ApiKeyAuthentication("api-key-id", "api-key");
}
```

### Serializer Configuration

NEST 7.x uses its own internal serializer by default. For custom JSON serialization, you have several options:

#### Option 1: Configure Field Name Inference

```csharp
protected override void ConfigureSettings(ConnectionSettings settings)
{
    base.ConfigureSettings(settings);
    
    // Use property names as-is (no camelCase conversion)
    settings.DefaultFieldNameInferrer(p => p);
}
```

#### Option 2: Use JSON.NET Serializer (Separate Package)

If you need full control over JSON serialization, install the `NEST.JsonNetSerializer` NuGet package:

```bash
dotnet add package NEST.JsonNetSerializer
```

Then configure it in `CreateElasticClient`:

```csharp
using Nest.JsonNetSerializer;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

protected override IElasticClient CreateElasticClient()
{
    var pool = CreateConnectionPool() ?? new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
    
    // Use JSON.NET serializer with custom settings
    var settings = new ConnectionSettings(pool, sourceSerializer: (builtin, connectionSettings) =>
        new JsonNetSerializer(builtin, connectionSettings, () => new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        },
        resolver => resolver.NamingStrategy = new CamelCaseNamingStrategy()));
    
    settings.EnableApiVersioningHeader();
    ConfigureSettings(settings);
    
    foreach (var index in Indexes)
        index.ConfigureSettings(settings);
    
    return new ElasticClient(settings);
}
```

#### Option 3: Custom Serializer Class

For more complex scenarios, create a custom serializer class:

```csharp
using Elasticsearch.Net;
using Nest;
using Nest.JsonNetSerializer;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

public class CustomJsonNetSerializer : ConnectionSettingsAwareSerializerBase
{
    public CustomJsonNetSerializer(
        IElasticsearchSerializer builtinSerializer, 
        IConnectionSettingsValues connectionSettings)
        : base(builtinSerializer, connectionSettings) { }

    protected override JsonSerializerSettings CreateJsonSerializerSettings() =>
        new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Include,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

    protected override void ModifyContractResolver(ConnectionSettingsAwareContractResolver resolver)
    {
        resolver.NamingStrategy = new CamelCaseNamingStrategy();
    }
}
```

Then use it:

```csharp
protected override IElasticClient CreateElasticClient()
{
    var pool = CreateConnectionPool() ?? new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
    var settings = new ConnectionSettings(pool, 
        sourceSerializer: (builtin, connSettings) => new CustomJsonNetSerializer(builtin, connSettings));
    
    settings.EnableApiVersioningHeader();
    ConfigureSettings(settings);
    
    foreach (var index in Indexes)
        index.ConfigureSettings(settings);
    
    return new ElasticClient(settings);
}
```

::: tip When to Use JSON.NET Serializer
- You need specific `JsonSerializerSettings` (null handling, date formatting, etc.)
- You have custom `JsonConverter` implementations
- You need to match serialization behavior with other parts of your application
- You're migrating from an older codebase that relies on Newtonsoft.Json behavior
:::

### Configuration with Dependency Injection

```csharp
public class MyElasticConfiguration : ElasticConfiguration
{
    private readonly IConfiguration _config;
    private readonly IWebHostEnvironment _env;

    public MyElasticConfiguration(
        IConfiguration config,
        IWebHostEnvironment env,
        ILoggerFactory loggerFactory) 
        : base(loggerFactory: loggerFactory)
    {
        _config = config;
        _env = env;
        
        AddIndex(Employees = new EmployeeIndex(this));
    }

    protected override IConnectionPool CreateConnectionPool()
    {
        var connectionString = _config.GetConnectionString("Elasticsearch") 
            ?? "http://localhost:9200";
        return new SingleNodeConnectionPool(new Uri(connectionString));
    }

    protected override void ConfigureSettings(ConnectionSettings settings)
    {
        base.ConfigureSettings(settings);
        
        if (_env.IsDevelopment())
        {
            settings.DisableDirectStreaming();
            settings.PrettyJson();
        }
    }

    public EmployeeIndex Employees { get; }
}
```

## Index Configuration

### IElasticConfiguration Interface

```csharp
public interface IElasticConfiguration : IDisposable
{
    IElasticClient Client { get; }
    ICacheClient Cache { get; }
    IMessageBus MessageBus { get; }
    ILoggerFactory LoggerFactory { get; }
    IReadOnlyCollection<IIndex> Indexes { get; }

    Task ConfigureIndexesAsync(IEnumerable<IIndex> indexes = null);
    Task MaintainIndexesAsync(IEnumerable<IIndex> indexes = null);
    Task DeleteIndexesAsync(IEnumerable<IIndex> indexes = null);
    Task ReindexAsync(IEnumerable<IIndex> indexes = null);
}
```

### Configuring Indexes

Call `ConfigureIndexesAsync` to create indexes:

```csharp
var config = new MyElasticConfiguration(loggerFactory);
await config.ConfigureIndexesAsync();
```

This will:
1. Create indexes that don't exist
2. Update mappings for existing indexes (if compatible)
3. Create aliases
4. Start reindexing for outdated indexes (if `beginReindexingOutdated` is true)

### Index Types

Foundatio.Repositories provides several index types:

| Type | Description | Use Case |
|------|-------------|----------|
| `Index<T>` | Basic index | Simple entities |
| `VersionedIndex<T>` | Schema versioning | Evolving schemas |
| `DailyIndex<T>` | Daily partitioning | Time-series data |
| `MonthlyIndex<T>` | Monthly partitioning | Time-series data |

See [Index Management](/guide/index-management) for detailed documentation.

## Index Mapping

### Basic Mapping

```csharp
public sealed class EmployeeIndex : VersionedIndex<Employee>
{
    public EmployeeIndex(IElasticConfiguration configuration) 
        : base(configuration, "employees", version: 1) { }

    public override TypeMappingDescriptor<Employee> ConfigureIndexMapping(
        TypeMappingDescriptor<Employee> map)
    {
        return map
            .Dynamic(false)  // Disable dynamic mapping
            .Properties(p => p
                .SetupDefaults()  // Configure Id, CreatedUtc, UpdatedUtc, IsDeleted
                .Keyword(f => f.Name(e => e.CompanyId))
                .Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())
                .Number(f => f.Name(e => e.Age).Type(NumberType.Integer))
            );
    }
}
```

### SetupDefaults Extension

The `SetupDefaults()` extension automatically configures common fields:

```csharp
.Properties(p => p.SetupDefaults())
```

This configures:
- `Id` as keyword
- `CreatedUtc` as date
- `UpdatedUtc` as date
- `IsDeleted` as boolean (if `ISupportSoftDeletes`)
- `Version` as keyword (if `IVersioned`)

### Field Types

#### Keyword Fields

For exact matching and aggregations:

```csharp
.Keyword(f => f.Name(e => e.Status))
```

#### Text Fields with Keywords

For full-text search with exact matching:

```csharp
.Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())
```

This creates:
- `name` - Analyzed text field
- `name.keyword` - Exact match keyword field
- `name.sort` - Normalized for sorting

#### Nested Objects

```csharp
.Nested<Address>(n => n
    .Name(e => e.Addresses)
    .Properties(ap => ap
        .Keyword(f => f.Name(a => a.City))
        .Keyword(f => f.Name(a => a.Country))
    ))
```

### Index Settings

```csharp
public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
{
    return base.ConfigureIndex(idx.Settings(s => s
        .NumberOfShards(3)
        .NumberOfReplicas(1)
        .RefreshInterval(TimeSpan.FromSeconds(5))
        .Analysis(a => a
            .AddSortNormalizer()
            .Analyzers(an => an
                .Custom("my_analyzer", ca => ca
                    .Tokenizer("standard")
                    .Filters("lowercase", "asciifolding")
                ))
        )));
}
```

## Caching and Messaging

### Adding Cache Support

```csharp
using Foundatio.Caching;

public class MyElasticConfiguration : ElasticConfiguration
{
    public MyElasticConfiguration(
        ICacheClient cache,
        ILoggerFactory loggerFactory) 
        : base(cache: cache, loggerFactory: loggerFactory)
    {
        AddIndex(Employees = new EmployeeIndex(this));
    }
    
    // ...
}
```

### Adding Message Bus Support

```csharp
using Foundatio.Messaging;

public class MyElasticConfiguration : ElasticConfiguration
{
    public MyElasticConfiguration(
        ICacheClient cache,
        IMessageBus messageBus,
        ILoggerFactory loggerFactory) 
        : base(cache: cache, messageBus: messageBus, loggerFactory: loggerFactory)
    {
        AddIndex(Employees = new EmployeeIndex(this));
    }
    
    // ...
}
```

### Full Configuration Example

```csharp
public class MyElasticConfiguration : ElasticConfiguration
{
    private readonly IConfiguration _config;
    private readonly IWebHostEnvironment _env;

    public MyElasticConfiguration(
        IConfiguration config,
        IWebHostEnvironment env,
        ICacheClient cache,
        IMessageBus messageBus,
        ILoggerFactory loggerFactory) 
        : base(cache: cache, messageBus: messageBus, loggerFactory: loggerFactory)
    {
        _config = config;
        _env = env;
        
        AddIndex(Employees = new EmployeeIndex(this));
        AddIndex(Projects = new ProjectIndex(this));
        AddIndex(AuditLogs = new AuditLogIndex(this));
    }

    protected override IConnectionPool CreateConnectionPool()
    {
        var connectionString = _config.GetConnectionString("Elasticsearch");
        if (string.IsNullOrEmpty(connectionString))
            connectionString = "http://localhost:9200";
            
        var uris = connectionString.Split(',').Select(s => new Uri(s.Trim()));
        
        if (uris.Count() == 1)
            return new SingleNodeConnectionPool(uris.First());
            
        return new StaticConnectionPool(uris);
    }

    protected override void ConfigureSettings(ConnectionSettings settings)
    {
        base.ConfigureSettings(settings);
        
        if (_env.IsDevelopment())
        {
            settings.DisableDirectStreaming();
            settings.PrettyJson();
        }
        
        var username = _config["Elasticsearch:Username"];
        var password = _config["Elasticsearch:Password"];
        if (!string.IsNullOrEmpty(username))
            settings.BasicAuthentication(username, password);
    }

    public EmployeeIndex Employees { get; }
    public ProjectIndex Projects { get; }
    public AuditLogIndex AuditLogs { get; }
}
```

## Dependency Injection Registration

### Basic Registration

```csharp
services.AddSingleton<MyElasticConfiguration>();
services.AddSingleton<IEmployeeRepository, EmployeeRepository>();
```

### With Foundatio Services

```csharp
// Register Foundatio services
services.AddSingleton<ICacheClient>(new InMemoryCacheClient());
services.AddSingleton<IMessageBus>(new InMemoryMessageBus());

// Register Elasticsearch configuration
services.AddSingleton<MyElasticConfiguration>();

// Register repositories
services.AddSingleton<IEmployeeRepository, EmployeeRepository>();
services.AddSingleton<IProjectRepository, ProjectRepository>();
```

### Startup Configuration

```csharp
// In Program.cs or Startup.cs
var config = app.Services.GetRequiredService<MyElasticConfiguration>();
await config.ConfigureIndexesAsync();
```

Or use a startup action:

```csharp
services.AddStartupAction("ConfigureElasticsearch", async sp =>
{
    var config = sp.GetRequiredService<MyElasticConfiguration>();
    await config.ConfigureIndexesAsync();
});
```

## Parent-Child Relationships

Elasticsearch supports parent-child relationships using join fields. This allows you to model hierarchical data where children are stored in the same index as parents but can be queried independently.

### Defining Parent-Child Documents

Implement `IParentChildDocument` for both parent and child entities:

```csharp
using Foundatio.Repositories.Elasticsearch;
using Foundatio.Repositories.Models;
using Nest;

// Parent document
public class Organization : IParentChildDocument, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; }
    
    // IParentChildDocument - parent doesn't need a ParentId
    string IParentChildDocument.ParentId { get; set; }
    JoinField IParentChildDocument.Discriminator { get; set; }
    
    public string Name { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}

// Child document
public class Employee : IParentChildDocument, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; }
    
    // Child must have ParentId
    public string ParentId { get; set; }
    JoinField IParentChildDocument.Discriminator { get; set; }
    
    public string Name { get; set; }
    public string Email { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}
```

### Configuring the Index

Create a single index with a join field mapping:

```csharp
public sealed class OrganizationIndex : VersionedIndex
{
    public OrganizationIndex(IElasticConfiguration configuration) 
        : base(configuration, "organizations", version: 1) { }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        return base.ConfigureIndex(idx
            .Settings(s => s.NumberOfReplicas(0).NumberOfShards(1))
            .Map<IParentChildDocument>(m => m
                .AutoMap<Organization>()
                .AutoMap<Employee>()
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(k => k.Name(o => ((Organization)o).Name))
                    .Keyword(k => k.Name(e => ((Employee)e).Email))
                    // Configure the join field
                    .Join(j => j
                        .Name(n => n.Discriminator)
                        .Relations(r => r.Join<Organization, Employee>())
                    )
                )));
    }
}
```

### Creating Repositories

Create separate repositories for parent and child:

```csharp
// Parent repository
public class OrganizationRepository : ElasticRepositoryBase<Organization>
{
    public OrganizationRepository(OrganizationIndex index) : base(index) { }
}

// Child repository - must set HasParent and GetParentIdFunc
public class EmployeeRepository : ElasticRepositoryBase<Employee>
{
    public EmployeeRepository(OrganizationIndex index) : base(index)
    {
        HasParent = true;
        GetParentIdFunc = e => e.ParentId;
        
        // Required for soft delete filtering on parent
        DocumentType = typeof(Employee);
        ParentDocumentType = typeof(Organization);
    }
}
```

### Working with Parent-Child Documents

```csharp
// Add parent
var org = await orgRepository.AddAsync(new Organization { Name = "Acme Corp" });

// Add child with parent reference
var employee = await employeeRepository.AddAsync(new Employee 
{ 
    Name = "John Doe",
    Email = "john@acme.com",
    ParentId = org.Id  // Link to parent
});

// Get child by ID (requires routing for efficiency)
var emp = await employeeRepository.GetByIdAsync(new Id(employee.Id, org.Id));

// Or without routing (uses search fallback)
var emp = await employeeRepository.GetByIdAsync(employee.Id);

// Query children by parent
var employees = await employeeRepository.FindAsync(q => q.ParentId("organization", org.Id));
```

### Parent-Child Soft Delete Behavior

When a parent is soft-deleted, children are automatically filtered from queries:

```csharp
// Soft delete the parent
org.IsDeleted = true;
await orgRepository.SaveAsync(org);

// Children are now filtered (even though they're not deleted)
var count = await employeeRepository.CountAsync();  // Returns 0

// Restore parent
org.IsDeleted = false;
await orgRepository.SaveAsync(org);

// Children are visible again
var count = await employeeRepository.CountAsync();  // Returns children count
```

### Querying with Parent Filters

```csharp
// Find children where parent matches criteria
var results = await employeeRepository.FindAsync(q => q
    .ParentQuery(pq => pq
        .DocumentType<Organization>()
        .FieldEquals(o => o.Name, "Acme Corp")));
```

::: warning Routing Considerations
- Child documents are routed to the same shard as their parent using `ParentId`
- For best performance, always provide routing when getting children by ID: `new Id(childId, parentId)`
- Without routing, the repository falls back to a search query which is slower
:::

## Health Checks

Add Elasticsearch health checks:

```csharp
services.AddHealthChecks()
    .AddCheck("elasticsearch", () =>
    {
        var config = services.BuildServiceProvider()
            .GetRequiredService<MyElasticConfiguration>();
        var response = config.Client.Ping();
        return response.IsValid 
            ? HealthCheckResult.Healthy() 
            : HealthCheckResult.Unhealthy("Elasticsearch is not responding");
    });
```

## Next Steps

- [CRUD Operations](/guide/crud-operations) - Working with documents
- [Querying](/guide/querying) - Building queries
- [Index Management](/guide/index-management) - Advanced index configuration
- [Configuration](/guide/configuration) - Repository configuration options
