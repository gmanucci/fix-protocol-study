using System.Threading.Channels;
using Fix.Protocol;

namespace Fix.Consumer;

/// <summary>
/// Per-market subscription to a producer. Implementations are scoped — created on demand by the
/// <see cref="MarketConnectionFactory"/> so multiple subscribers (one per market) coexist.
/// </summary>
public interface IMarketConnection : IAsyncDisposable
{
    string Symbol { get; }
    string Transport { get; }
    ChannelReader<MarketTick> Ticks { get; }
    Task StartAsync(CancellationToken ct);
}

/// <summary>Supported transports.</summary>
public enum Transport
{
    Tcp,
    Udp,
}
