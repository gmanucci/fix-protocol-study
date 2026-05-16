using System.Threading.Channels;
using Fix.Protocol;

namespace Fix.Consumer;

/// <summary>
/// Per-market subscription to a producer. Implementations are scoped — created on demand by the
/// <see cref="MarketConnectionFactory"/> so multiple subscribers (one per market) coexist.
/// </summary>
/// <remarks>
/// The connection emits <see cref="FixEvent"/> envelopes (not raw <see cref="MarketTick"/>) so
/// callers can subscribe to any FIX message kind, not just market-data refreshes. Implementations
/// must respect the <c>subscribedKinds</c> filter passed to <see cref="StartAsync"/> and only
/// publish events whose <see cref="FixEvent.Kind"/> is in the set.
/// </remarks>
public interface IMarketConnection : IAsyncDisposable
{
    string Symbol { get; }
    string Transport { get; }

    /// <summary>Stream of FIX events produced by this connection.</summary>
    ChannelReader<FixEvent> Events { get; }

    /// <summary>
    /// Starts the connection. <paramref name="subscribedKinds"/> filters which event kinds are
    /// written to <see cref="Events"/>; if empty, the implementation defaults to
    /// <see cref="FixEventKind.MarketDataIncrementalRefresh"/> only (matching the original
    /// study-grade behaviour).
    /// </summary>
    Task StartAsync(IReadOnlySet<FixEventKind> subscribedKinds, CancellationToken ct);
}

/// <summary>Supported transports for <see cref="IMarketConnection"/>.</summary>
public enum Transport
{
    Tcp,
    Udp,
    /// <summary>QuickFIX/n initiator-backed connection (see <c>Fix.Consumer.QuickFix</c>).</summary>
    QuickFix,
}
