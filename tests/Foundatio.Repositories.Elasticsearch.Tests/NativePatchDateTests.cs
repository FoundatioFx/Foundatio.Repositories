using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Elastic.Transport;
using Elastic.Transport.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class NativePatchDateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Apply_BulkJsonPatchWithNativeDateConverter_PreservesTheDateContract(bool propertyConverter)
    {
        if (propertyConverter)
            AssertPatch<PropertyDateDocument>(useDocumentDates: true, partial: false, globalConverter: false);
        else
            AssertPatch<DateDocument>(useDocumentDates: true, partial: false, globalConverter: true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Apply_PartialPatchWithExplicitNativeDate_PreservesTheDateContract(bool propertyConverter)
    {
        if (propertyConverter)
            AssertPatch<PropertyDateDocument>(useDocumentDates: false, partial: true, globalConverter: false);
        else
            AssertPatch<DateDocument>(useDocumentDates: false, partial: true, globalConverter: true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Apply_ActionPatchWithNativeDateConverter_PreservesTheDateContract(bool propertyConverter)
    {
        if (propertyConverter)
            AssertPatch<PropertyDateDocument>(useDocumentDates: false, partial: false, globalConverter: false, action: true);
        else
            AssertPatch<DateDocument>(useDocumentDates: false, partial: false, globalConverter: true, action: true);
    }

    private static void AssertPatch<TDocument>(bool useDocumentDates, bool partial, bool globalConverter, bool action = false)
        where TDocument : DateDocument, new()
    {
        var utcNow = new DateTimeOffset(2026, 11, 14, 12, 0, 0, TimeSpan.Zero);
        using var configuration = new DateConfiguration<TDocument>(new FakeTimeProvider(utcNow), globalConverter);
        using var repository = new DateRepository<TDocument>(configuration.Index);
        var source = new TDocument
        {
            Id = "one",
            Name = "original",
            CreatedUtc = utcNow.AddDays(-2).UtcDateTime,
            UpdatedUtc = utcNow.AddDays(-1).UtcDateTime
        };
        var hit = new FindHit<TDocument>(source.Id, source, 0);
        var applier = new ClientPatchApplier<TDocument>(configuration.Client.ElasticsearchClientSettings.SourceSerializer,
            repository.ApplyJsonDates, repository.ApplyDocumentDates);
        IPatchOperation patch;
        if (action)
        {
            patch = new ActionPatch<TDocument>(document =>
            {
                document.Name = "patched";
            });
        }
        else if (partial)
        {
            var partialPatch = repository.PreparePartialPatch(new PartialPatch(new
            {
                name = "patched",
                updatedUtc = utcNow.ToUnixTimeMilliseconds()
            }));
            var request = new Elastic.Clients.Elasticsearch.UpdateRequest<TDocument, object>(configuration.Index.Name, source.Id)
            {
                Doc = partialPatch.Document
            };
            string requestJson = configuration.Client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(request);
            using var parsedRequest = JsonDocument.Parse(requestJson);
            Assert.Equal(utcNow.ToUnixTimeMilliseconds(), parsedRequest.RootElement.GetProperty("doc").GetProperty("updatedUtc").GetInt64());
            patch = partialPatch;
        }
        else
        {
            patch = new JsonPatch(new PatchDocument(new ReplaceOperation { Path = "/name", Value = "patched" }));
        }

        var patched = applier.Apply(hit, patch, useDocumentDates);

        Assert.NotNull(patched);
        Assert.Equal("patched", patched.Name);
        Assert.Equal(source.CreatedUtc, patched.CreatedUtc);
        Assert.Equal(utcNow.UtcDateTime, patched.UpdatedUtc);
        if (action)
        {
            Assert.Same(source, patched);
        }
        else
        {
            Assert.Equal("original", source.Name);
            Assert.Equal(utcNow.AddDays(-1).UtcDateTime, source.UpdatedUtc);
        }

        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        using var stream = new MemoryStream(serializer.SerializeToBytes(patched));
        var roundTripped = serializer.Deserialize<TDocument>(stream);
        Assert.NotNull(roundTripped);
        Assert.Equal(utcNow.UtcDateTime, roundTripped.UpdatedUtc);
        Assert.Equal(source.CreatedUtc, roundTripped.CreatedUtc);
        Assert.Equal("patched", roundTripped.Name);
    }

    private sealed class DateConfiguration<TDocument> : ElasticConfiguration where TDocument : class
    {
        private readonly bool _globalConverter;

        public DateConfiguration(TimeProvider timeProvider, bool globalConverter) : base(timeProvider: timeProvider)
        {
            _globalConverter = globalConverter;
            AddIndex(Index = new Index<TDocument>(this, "native-patch-dates"));
        }

        public Index<TDocument> Index { get; }

        protected override IRequestInvoker CreateRequestInvoker() => new InMemoryRequestInvoker();

        protected override void ConfigureSourceSerializerOptions(JsonSerializerOptions options)
        {
            base.ConfigureSourceSerializerOptions(options);
            if (_globalConverter)
                options.Converters.Add(new EpochDateTimeConverter());
        }
    }

    private sealed class DateRepository<TDocument>(IIndex index) : ElasticRepositoryBase<TDocument>(index)
        where TDocument : DateDocument, new()
    {
        public void ApplyJsonDates(JsonNode target) => ApplyDateTracking(target);

        public void ApplyDocumentDates(TDocument document) => SetDocumentDates(document, ElasticIndex.Configuration.TimeProvider);

        public PartialPatch PreparePartialPatch(PartialPatch patch) => ApplyDateTracking(patch);
    }

    public class DateDocument : IIdentity, IHaveDates
    {
        public string Id { get; set; } = null!;
        public string? Name { get; set; }
        public DateTime CreatedUtc { get; set; }
        public virtual DateTime UpdatedUtc { get; set; }
    }

    public sealed class PropertyDateDocument : DateDocument
    {
        [JsonConverter(typeof(EpochDateTimeConverter))]
        public override DateTime UpdatedUtc { get; set; }
    }

    public sealed class EpochDateTimeConverter : JsonConverter<DateTime>
    {
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64()).UtcDateTime;
        }

        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(new DateTimeOffset(value, TimeSpan.Zero).ToUnixTimeMilliseconds());
        }
    }
}
