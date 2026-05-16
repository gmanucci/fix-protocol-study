using Fix.Consumer;
using Fix.Consumer.QuickFix;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<ConsumerOptions>(builder.Configuration.GetSection(ConsumerOptions.SectionName));
builder.Services.AddSingleton<MarketConnectionFactory>();
builder.Services.AddQuickFixConsumer();
builder.Services.AddSignalR();

var consumerOptions = builder.Configuration.GetSection(ConsumerOptions.SectionName).Get<ConsumerOptions>() ?? new ConsumerOptions();

// --- Outbound publishers (pluggable: SignalR / gRPC / Redis, any combination). ---
foreach (var kind in consumerOptions.Publishers.Distinct())
{
    switch (kind)
    {
        case PublisherKind.SignalR:
            builder.Services.AddSingleton<SignalREventPublisher>();
            break;
        case PublisherKind.Grpc:
            builder.Services.AddGrpc();
            builder.Services.AddSingleton<GrpcEventPublisher>();
            break;
        case PublisherKind.Redis:
            builder.Services.AddSingleton<RedisEventPublisher>();
            break;
    }
}
builder.Services.AddSingleton<IEventPublisher>(sp =>
{
    var inner = new List<IEventPublisher>();
    foreach (var kind in consumerOptions.Publishers.Distinct())
    {
        switch (kind)
        {
            case PublisherKind.SignalR: inner.Add(sp.GetRequiredService<SignalREventPublisher>()); break;
            case PublisherKind.Grpc: inner.Add(sp.GetRequiredService<GrpcEventPublisher>()); break;
            case PublisherKind.Redis: inner.Add(sp.GetRequiredService<RedisEventPublisher>()); break;
        }
    }
    if (inner.Count == 0) inner.Add(new SignalREventPublisher(sp.GetRequiredService<Microsoft.AspNetCore.SignalR.IHubContext<MarketHub>>()));
    return inner.Count == 1 ? inner[0] : new CompositeEventPublisher(inner);
});

builder.Services.AddCors(o => o.AddDefaultPolicy(p => p
    .WithOrigins(consumerOptions.CorsOrigins)
    .AllowAnyHeader()
    .AllowAnyMethod()
    .AllowCredentials()));

var app = builder.Build();

app.UseCors();
app.MapHub<MarketHub>("/hub/market");
if (consumerOptions.Publishers.Contains(PublisherKind.Grpc))
{
    app.MapGrpcService<MarketEventsService>();
}

// Simple liveness endpoint.
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();

/// <summary>Public marker for tests (WebApplicationFactory&lt;Program&gt;).</summary>
public partial class Program { }
