using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Fix.Consumer;

/// <summary>
/// Singleton factory that creates per-symbol scoped <see cref="IMarketConnection"/> instances.
/// Multiple connections may exist concurrently (one per market) per the design requirement.
/// </summary>
public sealed class MarketConnectionFactory
{
    private readonly ConsumerOptions _options;
    private readonly ILoggerFactory _loggerFactory;

    public MarketConnectionFactory(IOptions<ConsumerOptions> options, ILoggerFactory loggerFactory)
    {
        _options = options.Value;
        _loggerFactory = loggerFactory;
    }

    public IMarketConnection Create(string symbol, Transport transport) => transport switch
    {
        Transport.Tcp => new TcpMarketConnection(symbol, _options, _loggerFactory.CreateLogger<TcpMarketConnection>()),
        Transport.Udp => new UdpMarketConnection(symbol, _options, _loggerFactory.CreateLogger<UdpMarketConnection>()),
        _ => throw new ArgumentOutOfRangeException(nameof(transport)),
    };
}
