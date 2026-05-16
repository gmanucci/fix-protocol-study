using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Fix.Consumer;

/// <summary>
/// Singleton factory that creates per-symbol scoped <see cref="IMarketConnection"/> instances.
/// Multiple connections may exist concurrently (one per market) per the design requirement.
/// </summary>
/// <remarks>
/// The QuickFIX/n-backed connection lives in the <c>Fix.Consumer.QuickFix</c> assembly to keep
/// the QuickFIX/n dependency out of the core consumer host. It is wired in by registering an
/// <see cref="IQuickFixConnectionProvider"/> in DI; if no provider is registered, requesting
/// the QuickFix transport throws.
/// </remarks>
public sealed class MarketConnectionFactory
{
    private readonly ConsumerOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IQuickFixConnectionProvider? _quickFix;

    public MarketConnectionFactory(
        IOptions<ConsumerOptions> options,
        ILoggerFactory loggerFactory,
        IQuickFixConnectionProvider? quickFix = null)
    {
        _options = options.Value;
        _loggerFactory = loggerFactory;
        _quickFix = quickFix;
    }

    public IMarketConnection Create(string symbol, Transport transport) => transport switch
    {
        Transport.Tcp => new TcpMarketConnection(symbol, _options, _loggerFactory.CreateLogger<TcpMarketConnection>()),
        Transport.Udp => new UdpMarketConnection(symbol, _options, _loggerFactory.CreateLogger<UdpMarketConnection>()),
        Transport.QuickFix => _quickFix is null
            ? throw new InvalidOperationException("QuickFix transport is not registered. Reference Fix.Consumer.QuickFix and call AddQuickFixConsumer() in DI.")
            : _quickFix.Create(symbol, _options),
        _ => throw new ArgumentOutOfRangeException(nameof(transport)),
    };
}
