using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public sealed class SearchableReadOnlyRepositoryTests : ElasticRepositoryTestBase {
        private readonly EmployeeRepository _employeeRepository;

        public SearchableReadOnlyRepositoryTests(ITestOutputHelper output) : base(output) {
            _employeeRepository = new EmployeeRepository(_configuration);
            RemoveDataAsync().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task SortByNumberAsync() {
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(age: 19),
                EmployeeGenerator.Generate(age: 9),
                EmployeeGenerator.Generate(age: 119),
                EmployeeGenerator.Generate(age: 20)
            });

            await _client.RefreshAsync(Indices.All);
            var searchRepository = (ISearchableReadOnlyRepository<Employee>)_employeeRepository;
            var results = await searchRepository.SearchAsync(new MyAppQuery(), sort: "age");
            var employees = results.Documents.ToArray();
            Assert.Equal(4, employees.Length);
            Assert.Equal(9, employees[0].Age);
            Assert.Equal(19, employees[1].Age);
            Assert.Equal(20, employees[2].Age);
            Assert.Equal(119, employees[3].Age);

            results = await searchRepository.SearchAsync(new MyAppQuery(), sort: "-age");
            employees = results.Documents.ToArray();
            Assert.Equal(4, employees.Length);
            Assert.Equal(119, employees[0].Age);
            Assert.Equal(20, employees[1].Age);
            Assert.Equal(19, employees[2].Age);
            Assert.Equal(9, employees[3].Age);
        }

        [Fact]
        public async Task SearchByAnalyzedTextFieldAsync() {
            await _employeeRepository.AddAsync(new List<Employee> {
                EmployeeGenerator.Generate(age: 19, name: "Blake Niemyjski")
            });

            await _client.RefreshAsync(Indices.All);
            var searchRepository = (ISearchableReadOnlyRepository<Employee>)_employeeRepository;
            var results = await searchRepository.SearchAsync(new MyAppQuery(), "name:Blake");
            var employees = results.Documents.ToArray();
            Assert.Equal(1, employees.Length);

            results = await searchRepository.SearchAsync(new MyAppQuery(), "name:\"Blake Niemyjski\"");
            employees = results.Documents.ToArray();
            Assert.Equal(1, employees.Length);

            results = await searchRepository.SearchAsync(new MyAppQuery(), "name:Eric");
            employees = results.Documents.ToArray();
            Assert.Equal(0, employees.Length);
        }
    }
}