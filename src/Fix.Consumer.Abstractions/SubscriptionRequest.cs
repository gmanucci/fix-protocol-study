using Fix.Protocol;

namespace Fix.Consumer;

/// <summary>
/// Subscription request sent from a client to <see cref="MarketHub.Subscribe"/>.
/// </summary>
/// <param name="Symbol">Symbol to subscribe to (e.g. <c>EURUSD</c>).</param>
/// <param name="Transport">Transport string: <c>Tcp</c>, <c>Udp</c>, or <c>QuickFix</c>.</param>
/// <param name="EventKinds">
/// Event kinds the client wants to receive. Each value is a <see cref="FixEventKind"/> name
/// (e.g. <c>MarketDataIncrementalRefresh</c>). Empty / null defaults to
/// <see cref="FixEventKind.MarketDataIncrementalRefresh"/> only — the original behaviour.
/// </param>
public sealed record SubscriptionRequest(string Symbol, string Transport, string[]? EventKinds = null);
