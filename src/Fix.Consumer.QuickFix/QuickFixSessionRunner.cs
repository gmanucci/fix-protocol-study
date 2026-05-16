using System.Text;
using Fix.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QuickFix;
using QuickFix.Logger;
using QuickFix.Store;
using QuickFix.Transport;

namespace Fix.Consumer.QuickFix;

/// <summary>
/// Singleton owner of the shared QuickFIX/n initiator. Created lazily — the underlying
/// <see cref="SocketInitiator"/> is started on the first call to <see cref="EnsureStarted"/>.
/// </summary>
public sealed class QuickFixSessionRunner : IDisposable
{
    private readonly QuickFixOptions _options;
    private readonly ILogger<QuickFixSessionRunner> _logger;
    private readonly object _gate = new();
    public QuickFixApplication Application { get; }
    private SocketInitiator? _initiator;

    public QuickFixSessionRunner(IOptions<ConsumerOptions> options, ILoggerFactory loggerFactory)
    {
        _options = options.Value.QuickFix;
        _logger = loggerFactory.CreateLogger<QuickFixSessionRunner>();
        Application = new QuickFixApplication(loggerFactory.CreateLogger<QuickFixApplication>());
    }

    public void EnsureStarted()
    {
        if (_initiator is { IsStopped: false }) return;
        lock (_gate)
        {
            if (_initiator is { IsStopped: false }) return;
            try
            {
                var settings = LoadSettings();
                var storeFactory = new FileStoreFactory(settings);
                var logFactory = new ScreenLogFactory(settings);
                _initiator = new SocketInitiator(Application, storeFactory, settings, logFactory);
                _initiator.Start();
                _logger.LogInformation("QuickFIX initiator started");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "QuickFIX initiator failed to start; QuickFix transport will be inert until configured");
                _initiator = null;
            }
        }
    }

    private SessionSettings LoadSettings()
    {
        if (!string.IsNullOrWhiteSpace(_options.ConfigPath) && File.Exists(_options.ConfigPath))
            return new SessionSettings(_options.ConfigPath);

        var storeDir = _options.StoreDir
            ?? Path.Combine(Path.GetTempPath(), "fix-consumer-quickfix");
        Directory.CreateDirectory(storeDir);

        var sb = new StringBuilder()
            .AppendLine("[DEFAULT]")
            .AppendLine("ConnectionType=initiator")
            .AppendLine("ReconnectInterval=5")
            .AppendLine($"FileStorePath={storeDir}/store")
            .AppendLine($"FileLogPath={storeDir}/log")
            .AppendLine("StartTime=00:00:00")
            .AppendLine("EndTime=00:00:00")
            .AppendLine("UseDataDictionary=N")
            .AppendLine("SocketNodelay=Y")
            .AppendLine()
            .AppendLine("[SESSION]")
            .AppendLine("BeginString=FIX.4.4")
            .AppendLine($"SenderCompID={_options.SenderCompId}")
            .AppendLine($"TargetCompID={_options.TargetCompId}")
            .AppendLine($"HeartBtInt={_options.HeartBtInt}")
            .AppendLine($"SocketConnectHost={_options.SocketConnectHost}")
            .AppendLine($"SocketConnectPort={_options.SocketConnectPort}");

        using var reader = new StringReader(sb.ToString());
        return new SessionSettings(reader);
    }

    public void Dispose()
    {
        try { _initiator?.Stop(); } catch { }
        _initiator?.Dispose();
    }
}

/// <summary>
/// Implementation of <see cref="IQuickFixConnectionProvider"/> registered in DI by
/// <see cref="QuickFixServiceCollectionExtensions.AddQuickFixConsumer"/>.
/// </summary>
public sealed class QuickFixConnectionProvider : IQuickFixConnectionProvider
{
    private readonly QuickFixSessionRunner _runner;

    public QuickFixConnectionProvider(QuickFixSessionRunner runner) => _runner = runner;

    public IMarketConnection Create(string symbol, ConsumerOptions options)
    {
        _runner.EnsureStarted();
        return new QuickFixMarketConnection(symbol, _runner.Application, options.ChannelCapacity);
    }
}

public static class QuickFixServiceCollectionExtensions
{
    /// <summary>
    /// Registers the QuickFIX/n-backed transport so that
    /// <see cref="MarketConnectionFactory"/> can create <see cref="Transport.QuickFix"/> connections.
    /// </summary>
    public static IServiceCollection AddQuickFixConsumer(this IServiceCollection services)
    {
        services.AddSingleton<QuickFixSessionRunner>();
        services.AddSingleton<IQuickFixConnectionProvider, QuickFixConnectionProvider>();
        return services;
    }
}
