using System;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace Foundatio.Repositories.Elasticsearch.Tests.Extensions {
    internal static class TaskExtensions {
        public static Task WaitAsync(this AsyncCountdownEvent countdownEvent, CancellationToken cancellationToken = default(CancellationToken)) {
            return Task.WhenAny(countdownEvent.WaitAsync(), cancellationToken.AsTask());
        }
    }
}