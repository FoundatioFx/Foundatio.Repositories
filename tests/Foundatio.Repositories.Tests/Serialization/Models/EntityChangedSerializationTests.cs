using System;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Repositories.Models;
using Foundatio.Serializer;
using Foundatio.Tests.Extensions;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization.Models;

public class EntityChangedSerializationTests
{
    [Fact]
    public void EntityChanged_WithPopulatedData_RoundTripsCorrectly()
    {
        var original = new EntityChanged
        {
            Type = "contact",
            Id = "abc123",
            ChangeType = ChangeType.Saved
        };
        original.Data["orgId"] = "org456";
        original.Data["count"] = 42;

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<EntityChanged>(json);

            Assert.NotNull(roundTripped);
            Assert.Equal("contact", roundTripped.Type);
            Assert.Equal("abc123", roundTripped.Id);
            Assert.Equal(ChangeType.Saved, roundTripped.ChangeType);
            Assert.NotNull(roundTripped.Data);
            Assert.True(roundTripped.Data.ContainsKey("orgId"), $"Serializer {serializer.GetType().Name}: Data missing 'orgId' key");
            Assert.True(roundTripped.Data.TryGetValue("orgId", out var orgIdValue));
            Assert.Equal("org456", orgIdValue?.ToString());
        }
    }

    [Fact]
    public void EntityChanged_WithEmptyData_RoundTripsAsNonNull()
    {
        var original = new EntityChanged
        {
            Type = "contact",
            Id = "abc123",
            ChangeType = ChangeType.Removed
        };

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<EntityChanged>(json);

            Assert.NotNull(roundTripped);
            Assert.Equal("contact", roundTripped.Type);
            Assert.Equal(ChangeType.Removed, roundTripped.ChangeType);
            Assert.NotNull(roundTripped.Data);
            Assert.Empty(roundTripped.Data);
        }
    }

    [Fact]
    public async Task EntityChanged_WithPopulatedData_SurvivesMessageBusRoundTrip()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            await using var messageBus = new InMemoryMessageBus(o => o.Serializer(serializer));

            var original = new EntityChanged
            {
                Type = "contact",
                Id = "abc123",
                ChangeType = ChangeType.Saved
            };
            original.Data["orgId"] = "org456";
            original.Data["count"] = 42;

            EntityChanged? received = null;
            var countdown = new AsyncCountdownEvent(1);

            await messageBus.SubscribeAsync<EntityChanged>((msg, _) =>
            {
                received = msg;
                countdown.Signal();
                return Task.CompletedTask;
            }, cancellationToken);

            await messageBus.PublishAsync(original, cancellationToken: cancellationToken);
            await countdown.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.NotNull(received);
            Assert.Equal("contact", received.Type);
            Assert.Equal("abc123", received.Id);
            Assert.Equal(ChangeType.Saved, received.ChangeType);
            Assert.NotNull(received.Data);
            Assert.True(received.Data.ContainsKey("orgId"), $"Serializer {serializer.GetType().Name}: Data missing 'orgId' key after message bus round-trip");
            Assert.True(received.Data.TryGetValue("orgId", out var orgIdValue));
            Assert.Equal("org456", orgIdValue?.ToString());
        }
    }

    [Fact]
    public async Task EntityChanged_WithEmptyData_SurvivesMessageBusRoundTrip()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            await using var messageBus = new InMemoryMessageBus(o => o.Serializer(serializer));

            var original = new EntityChanged
            {
                Type = "activity",
                ChangeType = ChangeType.Removed
            };

            EntityChanged? received = null;
            var countdown = new AsyncCountdownEvent(1);

            await messageBus.SubscribeAsync<EntityChanged>((msg, _) =>
            {
                received = msg;
                countdown.Signal();
                return Task.CompletedTask;
            }, cancellationToken);

            await messageBus.PublishAsync(original, cancellationToken: cancellationToken);
            await countdown.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.NotNull(received);
            Assert.Equal("activity", received.Type);
            Assert.Equal(ChangeType.Removed, received.ChangeType);
            Assert.NotNull(received.Data);
            Assert.Empty(received.Data);
        }
    }
}
