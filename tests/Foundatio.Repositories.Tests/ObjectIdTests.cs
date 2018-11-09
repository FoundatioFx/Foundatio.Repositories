using System;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Tests {
    public class ObjectIdTests {
        [Fact]
        public void CanParseDate() {
            var time = SystemClock.UtcNow.Round(TimeSpan.FromSeconds(1));
            var id = ObjectId.GenerateNewId(time);
            Assert.Equal(time, id.CreationTime);

            var parsedId = ObjectId.Parse(id.ToString());
            Assert.Equal(id, parsedId);
            Assert.Equal(time, parsedId.CreationTime);
        }

        [Fact]
        public void CanParseOldDate() {
            var time = SystemClock.UtcNow.SubtractMonths(1).Round(TimeSpan.FromSeconds(1));
            var id = ObjectId.GenerateNewId(time);
            Assert.Equal(time, id.CreationTime);

            var parsedId = ObjectId.Parse(id.ToString());
            Assert.Equal(id, parsedId);
            Assert.Equal(time, parsedId.CreationTime);
        }

        [Fact]
        public void CanCreateUniqueIdsFromSameDateTime() {
            var utcNow = SystemClock.UtcNow.Round(TimeSpan.FromSeconds(1));
            var id = ObjectId.GenerateNewId(utcNow);
            Assert.Equal(utcNow, id.CreationTime);

            var id2 = ObjectId.GenerateNewId(utcNow);
            Assert.Equal(utcNow, id2.CreationTime);

            Assert.NotEqual(id, id2);
        }
    }
}
