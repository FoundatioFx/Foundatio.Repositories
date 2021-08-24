using System;
using Foundatio.Repositories;

namespace TestProject1 {
    public class MyDoc {}

    public class MySimpleRepo : Repo<MyDoc> {
        protected override CommandOptions ConfigureOptions(CommandOptions options) {
            options.DefaultCacheKey = "defaultkey";
            return base.ConfigureOptions(options);
        }
    }

    public class MyCustomOptionsRepo : Repo<MyDoc, AppCommandOptions, AppCommandOptionsBuilder, AppQueryOptions, AppQueryOptionsBuilder> {
        protected override AppCommandOptions ConfigureOptions(AppCommandOptions options) {
            options.DefaultCacheKey = "defaultkey";
            if (String.IsNullOrEmpty(options.AppLevelOption))
                options.AppLevelOption = "myappoption";
            
            return options;
        }
    }

    public class AppCommandOptions : CommandOptions {
        public string AppLevelOption { get; set; }
        
        public override T Clone<T>() {
            var clone = base.Clone<T>();
            if (clone is AppCommandOptions appCommandOptions)
                appCommandOptions.AppLevelOption = AppLevelOption;

            return clone;
        }
    }

    public class AppCommandOptionsBuilder : CommandOptionsBuilder<AppCommandOptions, AppCommandOptionsBuilder> {
        public AppCommandOptionsBuilder() : base(new AppCommandOptions()) { }
        public AppCommandOptionsBuilder AppLevelOption(string appLevelOption) {
            _target.AppLevelOption = appLevelOption;
            return _builder;
        }
    }

    public class AppQueryOptions : QueryOptions {
        public string AppLevelOption { get; set; }
        
        public override T Clone<T>() {
            var clone = base.Clone<T>();
            if (clone is AppQueryOptions appQueryOptions)
                appQueryOptions.AppLevelOption = AppLevelOption;

            return clone;
        }
    }

    public class AppQueryOptionsBuilder : QueryOptionsBuilder<AppQueryOptions, AppQueryOptionsBuilder> {
        public AppQueryOptionsBuilder() : base(new AppQueryOptions()) { }

        public AppQueryOptionsBuilder AppLevelOption(string appLevelOption) {
            _target.AppLevelOption = appLevelOption;
            return _builder;
        }
    }
}