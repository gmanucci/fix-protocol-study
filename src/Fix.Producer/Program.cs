using Fix.Producer;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<ProducerOptions>(builder.Configuration.GetSection(ProducerOptions.SectionName));

// Singleton market data generator + IHostedService.
builder.Services.AddSingleton<MarketDataSource>();
builder.Services.AddSingleton<IMarketDataSource>(sp => sp.GetRequiredService<MarketDataSource>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<MarketDataSource>());

// Transports — separate IHostedServices, both behind IFixTransport.
builder.Services.AddSingleton<TcpFixTransport>();
builder.Services.AddSingleton<UdpFixTransport>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<TcpFixTransport>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<UdpFixTransport>());

var host = builder.Build();
host.Run();
