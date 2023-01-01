using Foundatio.Extensions.Hosting.Startup;
using Foundatio.Repositories;
using Foundatio.SampleApp.Server.Repositories;
using Foundatio.SampleApp.Shared;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllersWithViews();
builder.Services.AddRazorPages();

// add elastic configuration and repository to DI
builder.Services.AddSingleton<SampleAppElasticConfiguration>();
builder.Services.AddSingleton<IGameReviewRepository, GameReviewRepository>();

// configure the elasticsearch indexes
builder.Services.AddConfigureIndexesStartupAction();

// add sample data if there is none
builder.Services.AddSampleDataStartupAction();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseWebAssemblyDebugging();
}
else
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseBlazorFrameworkFiles();
app.UseStaticFiles();

app.UseRouting();

app.MapRazorPages();

app.UseWaitForStartupActionsBeforeServingRequests();

// add endpoint to get game reviews
app.MapGet("GameReviews", async (IGameReviewRepository gameReviewRepository, string? search, string? filter, string? sort, int? page, int? limit, string? fields, string? aggs) =>
{
    var reviews = await gameReviewRepository.FindAsync(q => q
        .FilterExpression(filter)
        .SortExpression(sort)
        .SearchExpression(search)
        .IncludeMask(fields)
        .AggregationsExpression(aggs ?? "terms:category terms:tags"),
        o => o.PageNumber(page).PageLimit(limit).QueryLogLevel(LogLevel.Warning));

    var result = GameReviewSearchResult.From(reviews);

    return result;
});

app.MapFallbackToFile("index.html");

app.Run();
