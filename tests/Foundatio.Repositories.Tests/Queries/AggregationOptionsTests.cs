using System;
using Foundatio.Logging.Xunit;
using Foundatio.Repositories.Queries;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Tests.Queries {
    public class AggregationOptionsTests : TestWithLoggingBase {
        public AggregationOptionsTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        public void AggregationOptions_ParseSimple() {
            var options = AggregationOptions.Parse("test");
            Assert.NotNull(options);
            Assert.Equal(1, options.Fields.Count);
            Assert.Equal("test", options.Fields[0].Field);
        }

        [Fact]
        public void AggregationOptions_ParseSimpleWithSize() {
            var options = AggregationOptions.Parse("test:43");
            Assert.NotNull(options);
            Assert.Equal(1, options.Fields.Count);
            Assert.Equal("test", options.Fields[0].Field);
            Assert.Equal(43, options.Fields[0].Size);
        }

        [Fact]
        public void AggregationOptions_ParseSimple2Fields() {
            var options = AggregationOptions.Parse("test,test2");
            Assert.NotNull(options);
            Assert.Equal(2, options.Fields.Count);
            Assert.Equal("test", options.Fields[0].Field);
            Assert.Equal("test2", options.Fields[1].Field);
        }

        [Fact]
        public void AggregationOptions_ShouldTrim() {
            var options = AggregationOptions.Parse("  test ,       test2 ");
            Assert.NotNull(options);
            Assert.Equal(2, options.Fields.Count);
            Assert.Equal("test", options.Fields[0].Field);
            Assert.Equal("test2", options.Fields[1].Field);
        }

        [Fact]
        public void AggregationOptions_ParseSimple2FieldsWithSize() {
            var options = AggregationOptions.Parse("test:1,test2:2");
            Assert.NotNull(options);
            Assert.Equal(2, options.Fields.Count);
            Assert.Equal("test", options.Fields[0].Field);
            Assert.Equal(1, options.Fields[0].Size);
            Assert.Equal("test2", options.Fields[1].Field);
            Assert.Equal(2, options.Fields[1].Size);
        }

        [Fact]
        public void AggregationOptions_ParseSingleNested() {
            var options = AggregationOptions.Parse("test(test2)");
            Assert.NotNull(options);
            Assert.Equal(1, options.Fields.Count);
            Assert.Equal("test", options.Fields[0].Field);
            Assert.NotNull(options.Fields[0].Nested);
            Assert.Equal(1, options.Fields[0].Nested.Fields.Count);
            Assert.Equal("test2", options.Fields[0].Nested.Fields[0].Field);
        }

        [Fact]
        public void AggregationOptions_ParseSingleNested2() {
            var options = AggregationOptions.Parse("geo_country(geo_level1,test2),test3");

            Assert.NotNull(options);
            Assert.Equal(2, options.Fields.Count);
            var firstField = options.Fields[0];
            var secondField = options.Fields[1];
            Assert.Equal("geo_country", firstField.Field);
            Assert.Equal("test3", secondField.Field);

            var nestedOptions = firstField.Nested;

            Assert.NotNull(nestedOptions);
            Assert.Equal(2, nestedOptions.Fields.Count);

            Assert.Equal("geo_level1", nestedOptions.Fields[0].Field);
            Assert.Equal("test2", nestedOptions.Fields[1].Field);
        }

        [Fact]
        public void AggregationOptions_ParseSuperDeepNestedWithSize() {
            var options = AggregationOptions.Parse("test1:1(test1-1:11(test1-1-1:111,test1-1-2:112),test1-2:12),test2:2");

            Assert.NotNull(options);
            Assert.Equal(2, options.Fields.Count);
            var firstField = options.Fields[0];
            var secondField = options.Fields[1];
            Assert.Equal("test1", firstField.Field);
            Assert.Equal(1, firstField.Size);
            Assert.Equal("test2", secondField.Field);
            Assert.Equal(2, secondField.Size);

            var level1 = firstField.Nested;

            Assert.NotNull(level1);
            Assert.Equal(2, level1.Fields.Count);

            Assert.Equal("test1-1", level1.Fields[0].Field);
            Assert.Equal(11, level1.Fields[0].Size);
            Assert.Equal("test1-2", level1.Fields[1].Field);
            Assert.Equal(12, level1.Fields[1].Size);

            var level2 = level1.Fields[0].Nested;
            Assert.NotNull(level2);
            Assert.Equal(2, level2.Fields.Count);

            Assert.Equal("test1-1-1", level2.Fields[0].Field);
            Assert.Equal(111, level2.Fields[0].Size);
            Assert.Equal("test1-1-2", level2.Fields[1].Field);
            Assert.Equal(112, level2.Fields[1].Size);
        }
    }
}