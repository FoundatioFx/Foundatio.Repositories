![Foundatio](https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio.png "Foundatio")

[![Build status](https://ci.appveyor.com/api/projects/status/fhuovj9tddvjgxja?svg=true)](https://ci.appveyor.com/project/Exceptionless/foundatio-repositories)
[![NuGet Version](http://img.shields.io/nuget/v/Foundatio.Repositories.svg?style=flat)](https://www.nuget.org/packages/Foundatio.Repositories/)
[![Slack Status](https://slack.exceptionless.com/badge.svg)](https://slack.exceptionless.com)

Generic repository contract and implementations. Currently only implemented for Elasticsearch, but there are plans for other implementations.

# Features

- Simple document repository pattern
  - CRUD operations: Add, Save, Remove, Get
- Supports patch operations
  - JSON patch
  - Partial document patch
  - Painless script patch (Elasticsearch)
  - Can be applied in bulk using queries
- Async events that can be wired up and listened to outside of the repos
- Caching (real-time invalidated before save and stored in distributed cache)
- Message bus support (enables real-time apps sends messages like doc updated up to the client so they know to update the UI)
- Searchable that works with Foundatio.Parsers lib for dynamic querying, filtering, sorting and aggregations
- Document validation
- Document versioning
- Soft deletes
- Auto document created and updated dates
- Document migrations
- Elasticsearch implementation
  - Plan to add additional implementations (Postgres with Marten would be a good fit)
- Elasticsearch index configuration allows simpler and more organized configuration
  - Schema versioning
  - Parent child indexes
  - Daily and monthly index strategies
- Supports different consistency models (immediate, wait or eventual)
  - Can be configured at the index type or individual query level
- Query builders used to make common ways of querying data easier and more portable between repo implementations
- Can still use raw Elasticsearch queries
- Field includes and excludes to make the response size smaller
- Field conditions query builder
- Paging including snapshot paging support
- Automatic alias support right in the index type mappings
- Aliases can be dynamic as well
- Jobs for index maintenance, snapshots, reindex
- Strongly typed field access (using lambda expressions) to enable refactoring
