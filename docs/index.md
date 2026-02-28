---
layout: home

hero:
  name: Foundatio Repositories
  text: Production-grade repository pattern for .NET
  tagline: Build robust data access layers with Elasticsearch, caching, messaging, and more
  image:
    light: https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio.svg
    dark: https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio-dark-bg.svg
    alt: Foundatio
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: View on GitHub
      link: https://github.com/FoundatioFx/Foundatio.Repositories

features:
  - icon: 📦
    title: Repository Pattern
    details: Clean abstraction over data access with IRepository, IReadOnlyRepository, and ISearchableRepository interfaces.
    link: /guide/repository-pattern

  - icon: ⚡
    title: CRUD Operations
    details: Full async CRUD support with Add, Save, Get, Remove operations and lifecycle events.
    link: /guide/crud-operations

  - icon: 🔧
    title: Patch Operations
    details: Flexible document updates with JSON Patch, partial patches, Painless scripts, and bulk patching.
    link: /guide/patch-operations

  - icon: 💾
    title: Caching
    details: Built-in distributed caching with automatic invalidation and real-time cache consistency.
    link: /guide/caching

  - icon: 📡
    title: Message Bus
    details: Real-time entity change notifications via message bus for event-driven architectures.
    link: /guide/message-bus

  - icon: 🔍
    title: Searchable Queries
    details: Dynamic querying with Foundatio.Parsers for filtering, sorting, and aggregations.
    link: /guide/querying

  - icon: 🗑️
    title: Soft Deletes
    details: Built-in soft delete support with automatic query filtering and restore capabilities.
    link: /guide/soft-deletes

  - icon: 🔢
    title: Versioning
    details: Optimistic concurrency control with document versioning and conflict detection.
    link: /guide/versioning

  - icon: 📊
    title: Index Management
    details: Schema versioning with daily, monthly, and versioned index strategies.
    link: /guide/index-management

  - icon: 🔄
    title: Migrations
    details: Document migration infrastructure for evolving your data schema over time.
    link: /guide/migrations

  - icon: ⏰
    title: Jobs
    details: Built-in jobs for index maintenance, snapshots, cleanup, and reindexing.
    link: /guide/jobs

  - icon: 🎛️
    title: Custom Fields
    details: Dynamic field support for tenant-specific or user-defined fields with type safety.
    link: /guide/custom-fields
---

## Quick Example

```csharp
// Define your entity
public class Employee : IIdentity, IHaveDates, ISupportSoftDeletes
{
    public string Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}

// Create a repository
public class EmployeeRepository : ElasticRepositoryBase<Employee>, IEmployeeRepository
{
    public EmployeeRepository(EmployeeIndex index) : base(index) { }
}

// Use the repository
var employee = await repository.AddAsync(new Employee { Name = "John", Age = 30 });
var found = await repository.FindAsync(q => q.FilterExpression("age:>=25"));
```
