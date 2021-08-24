using System;
using Foundatio.Repositories;
using Xunit;

namespace TestProject1 {
    public class AppRepoTests {
        [Fact]
        public void SimpleRepo() {
            var myRepo = new MySimpleRepo();
            myRepo.Add(new MyDoc(), o => o.Cache("stuff"));
            Assert.Equal("stuff", myRepo.ConfiguredOptions.CacheKey);
            Assert.Equal("defaultkey", myRepo.ConfiguredOptions.DefaultCacheKey);
            
            var options = new CommandOptions { CacheKey = "stuff" };
            myRepo.Add(new MyDoc(), options);
            Assert.Equal("stuff", myRepo.ConfiguredOptions.CacheKey);
            Assert.Equal("defaultkey", myRepo.ConfiguredOptions.DefaultCacheKey);
            
            var appOptions = new CommandOptions { CacheKey = "stuff" };
            myRepo.Add(new MyDoc(), appOptions);
            Assert.Equal("stuff", myRepo.ConfiguredOptions.CacheKey);
            Assert.Equal("defaultkey", myRepo.ConfiguredOptions.DefaultCacheKey);
        }
        
        [Fact]
        public void CustomOptionsRepo() {
            var myRepo = new MyCustomOptionsRepo();
            myRepo.Add(new MyDoc(), o => o.Cache("stuff").AppLevelOption("appoption").Set("Custom", "somecustomvalue"));
            myRepo.Query(q => q.Id("123").AppLevelOption("blah"), o => o.Cache("struf"));
            Assert.Equal("stuff", myRepo.ConfiguredOptions.CacheKey);
            Assert.Equal("defaultkey", myRepo.ConfiguredOptions.DefaultCacheKey);
            Assert.Equal("appoption", myRepo.ConfiguredOptions.AppLevelOption);
            Assert.Equal("somecustomvalue", myRepo.ConfiguredOptions.Values.Get("Custom", "notset"));
            
            var options = new AppCommandOptions { CacheKey = "stuff" };
            myRepo.Add(new MyDoc(), options);
            Assert.Equal("stuff", myRepo.ConfiguredOptions.CacheKey);
            Assert.Equal("defaultkey", myRepo.ConfiguredOptions.DefaultCacheKey);
            Assert.Equal("myappoption", myRepo.ConfiguredOptions.AppLevelOption);
            
            var appOptions = new AppCommandOptions { CacheKey = "stuff", AppLevelOption = "mystuff" };
            myRepo.Add(new MyDoc(), appOptions);
            Assert.Equal("stuff", myRepo.ConfiguredOptions.CacheKey);
            Assert.Equal("defaultkey", myRepo.ConfiguredOptions.DefaultCacheKey);
            Assert.Equal("mystuff", myRepo.ConfiguredOptions.AppLevelOption);
        }
    }
}