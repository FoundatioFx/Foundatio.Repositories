namespace Foundatio.Repositories {
    public enum Consistency {
        Eventual,
        Immediate,
        Wait
    }

    public static class ConsistencyOptionsExtensions {
        internal const string ConsistencyModeKey = "@ConsistencyMode";
        public static T Consistency<T>(this T options, Consistency mode) where T : ICommandOptions {
            options.Values.Set(ConsistencyModeKey, mode);
            return options;
        }

        public static T ImmediateConsistency<T>(this T options, bool shouldWait = false) where T : ICommandOptions {
            options.Values.Set(ConsistencyModeKey, shouldWait ? Repositories.Consistency.Wait : Repositories.Consistency.Immediate);
            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadConsistencyOptionsExtensions {
        public static Consistency GetConsistency(this ICommandOptions options, Consistency defaultMode = Consistency.Eventual) {
            return options.SafeGetOption(ConsistencyOptionsExtensions.ConsistencyModeKey, defaultMode);
        }
    }
}

