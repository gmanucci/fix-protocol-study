using Fix.Consumer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<ConsumerOptions>(builder.Configuration.GetSection(ConsumerOptions.SectionName));
builder.Services.AddSingleton<MarketConnectionFactory>();
builder.Services.AddSignalR();

var consumerOptions = builder.Configuration.GetSection(ConsumerOptions.SectionName).Get<ConsumerOptions>() ?? new ConsumerOptions();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p
    .WithOrigins(consumerOptions.CorsOrigins)
    .AllowAnyHeader()
    .AllowAnyMethod()
    .AllowCredentials()));

var app = builder.Build();

app.UseCors();
app.MapHub<MarketHub>("/hub/market");

// Simple liveness endpoint.
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();

/// <summary>Public marker for tests (WebApplicationFactory&lt;Program&gt;).</summary>
public partial class Program { }
