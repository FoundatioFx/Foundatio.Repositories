using System;
using Foundatio.Repositories.Options;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    /// <summary>
    /// Options that control the behavior of the repositories
    /// </summary>
    public interface ICommandOptions {
        bool? CacheEnabled { get; set; }
        bool? ReadCacheEnabled { get; set; }
        string CacheKey { get; set; }
        TimeSpan? CacheExpiresIn { get; set; }
        string DefaultCacheKey { get; set; }
        TimeSpan? DefaultCacheExpiresIn { get; set; }
        IOptionsDictionary Values { get; }
        T Clone<T>() where T : ICommandOptions, new();
    }

    public class CommandOptions : ICommandOptions {
        public bool? CacheEnabled { get; set; }
        public bool? ReadCacheEnabled { get; set; }
        public string CacheKey { get; set; }
        public TimeSpan? CacheExpiresIn { get; set; }
        public string DefaultCacheKey { get; set; }
        public TimeSpan? DefaultCacheExpiresIn { get; set; }
        public IOptionsDictionary Values { get; internal set; } = new OptionsDictionary();

        public virtual T Clone<T>() where T : ICommandOptions, new() {
            var clone = new T();

            foreach (var kvp in Values)
                clone.Values.Add(kvp.Key, kvp.Value);

            var commandOptionsClone = clone as ICommandOptions;
            if (commandOptionsClone == null)
                throw new ArgumentException("Target type must implement ICommandOptions");

            commandOptionsClone.CacheEnabled = CacheEnabled;
            commandOptionsClone.ReadCacheEnabled = ReadCacheEnabled;
            commandOptionsClone.CacheKey = CacheKey;
            commandOptionsClone.CacheExpiresIn = CacheExpiresIn;
            commandOptionsClone.DefaultCacheKey = DefaultCacheKey;
            commandOptionsClone.DefaultCacheExpiresIn = DefaultCacheExpiresIn;

            return clone;
        }
    }

    public interface ICommandOptionsBuilder<out T> where T : ICommandOptions {
        T Build();
    }
    
    public abstract class CommandOptionsBuilder<TOptions, TBuilder> : ICommandOptionsBuilder<TOptions>
        where TOptions : class, ICommandOptions
        where TBuilder : CommandOptionsBuilder<TOptions, TBuilder> {

        protected readonly TBuilder _builder;
        protected readonly TOptions _target;

        public CommandOptionsBuilder(TOptions target) {
            _builder = (TBuilder)this;
            _target = target ?? throw new ArgumentNullException(nameof(target));
        }

        public TBuilder Cache(string cacheKey) {
            _target.CacheEnabled = true;
            _target.CacheKey = cacheKey;
            return _builder;
        }

        public TBuilder Cache(string cacheKey, TimeSpan? expiresIn) {
            _target.CacheEnabled = true;
            _target.CacheKey = cacheKey;
            _target.CacheExpiresIn = expiresIn;
            return _builder;
        }

        public TBuilder Cache(string cacheKey, DateTime? expiresAtUtc) {
            return Cache(cacheKey, expiresAtUtc.Value.Subtract(SystemClock.UtcNow));
        }

        public TBuilder ReadCache() {
            _target.ReadCacheEnabled = true;
            return _builder;
        }

        public TBuilder CacheKey(string cacheKey) {
            if (!String.IsNullOrEmpty(cacheKey))
                _target.CacheKey = cacheKey;

            return _builder;
        }

        public TBuilder DefaultCacheKey(string defaultCacheKey) {
            if (!String.IsNullOrEmpty(defaultCacheKey))
                _target.DefaultCacheKey = defaultCacheKey;

            return _builder;
        }

        public TBuilder CacheExpiresIn(TimeSpan? expiresIn) {
            if (expiresIn.HasValue) {
                _target.CacheEnabled = true;
                _target.CacheExpiresIn = expiresIn;
            }

            return _builder;
        }

        public TBuilder CacheExpiresAt(DateTime? expiresAtUtc) {
            if (expiresAtUtc.HasValue) {
                _target.CacheEnabled = true;
                _target.CacheExpiresIn = expiresAtUtc.Value.Subtract(SystemClock.UtcNow);
            }

            return _builder;
        }

        public TBuilder DefaultCacheExpiresIn(TimeSpan expiresIn) {
            _target.DefaultCacheExpiresIn = expiresIn;
            return _builder;
        }

        public TBuilder Set(string key, object value) {
            _target.Values.Add(key, value);
            return _builder;
        }
        
        TOptions ICommandOptionsBuilder<TOptions>.Build() {
            return _target;
        }
    }

    public class CommandOptionsBuilder : CommandOptionsBuilder<CommandOptions, CommandOptionsBuilder> {
        public CommandOptionsBuilder() : base(new CommandOptions()) {}
        public CommandOptionsBuilder(CommandOptions target) : base(target ?? new CommandOptions()) {}
    }
}
