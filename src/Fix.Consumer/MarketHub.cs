using System.Collections.Concurrent;
using Fix.Protocol;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace Fix.Consumer;

/// <summary>
/// Plain DTO sent over SignalR for backwards compatibility with the original Angular client
/// — kept alongside the richer <see cref="FixEvent"/> envelope. Records are fine here — this
/// is a cold-ish path that crosses the JSON serializer anyway, and the producer-side hot path
/// keeps using <c>MarketTick</c> structs internally.
/// </summary>
public sealed record TickDto(string Symbol, double Price, double Quantity, long TimestampTicks, string EntryType, string Transport);

/// <summary>
/// SignalR hub. Clients call:
/// <list type="bullet">
///   <item><c>ListEventKinds</c> — to discover all FIX event types supported by the server.</item>
///   <item><c>Subscribe(SubscriptionRequest)</c> — to start receiving the chosen event kinds.</item>
///   <item><c>Unsubscribe(symbol, transport)</c> — to stop a specific subscription.</item>
/// </list>
/// The actual fan-out is handled by an <see cref="IEventPublisher"/> resolved from DI, so the
/// hub is decoupled from the wire format. Today that is a <see cref="SignalREventPublisher"/>
/// which sends an <c>"event"</c> message back to the calling SignalR connection (and a legacy
/// <c>"tick"</c> message for market-data).
/// </summary>
public sealed class MarketHub : Hub
{
    // Per-connection map of (symbol+transport → pump)
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<string, SubscriptionPump>> Subs = new();

    private readonly MarketConnectionFactory _factory;
    private readonly IEventPublisher _publisher;
    private readonly ILogger<MarketHub> _logger;

    public MarketHub(MarketConnectionFactory factory, IEventPublisher publisher, ILogger<MarketHub> logger)
    {
        _factory = factory;
        _publisher = publisher;
        _logger = logger;
    }

    /// <summary>Returns every <see cref="FixEventKind"/> name the server can emit.</summary>
    public string[] ListEventKinds() =>
        FixEventKinds.All.Select(k => k.ToString()).ToArray();

    /// <summary>New, richer subscription entry point that accepts an explicit event-kind filter.</summary>
    public async Task Subscribe(SubscriptionRequest request)
    {
        if (request is null) throw new HubException("request required");
        if (string.IsNullOrWhiteSpace(request.Symbol)) throw new HubException("symbol required");
        if (!Enum.TryParse<Transport>(request.Transport, ignoreCase: true, out var t))
            throw new HubException($"unknown transport '{request.Transport}'");

        var kinds = ParseKinds(request.EventKinds);
        var key = $"{request.Symbol}|{t}";
        var bag = Subs.GetOrAdd(Context.ConnectionId, _ => new ConcurrentDictionary<string, SubscriptionPump>());
        if (bag.ContainsKey(key)) return;

        var conn = _factory.Create(request.Symbol, t);
        var pump = new SubscriptionPump(conn, _publisher, Context.ConnectionId, _logger, Context.ConnectionAborted);
        if (!bag.TryAdd(key, pump))
        {
            await pump.DisposeAsync();
            return;
        }
        await pump.StartAsync(kinds).ConfigureAwait(false);
    }

    /// <summary>Backwards-compatible overload used by the original client.</summary>
    public Task Subscribe(string symbol, string transport) =>
        Subscribe(new SubscriptionRequest(symbol, transport, null));

    public async Task Unsubscribe(string symbol, string transport)
    {
        if (!Enum.TryParse<Transport>(transport, ignoreCase: true, out var t)) return;
        var key = $"{symbol}|{t}";
        if (Subs.TryGetValue(Context.ConnectionId, out var bag) && bag.TryRemove(key, out var pump))
        {
            await pump.DisposeAsync();
        }
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        if (Subs.TryRemove(Context.ConnectionId, out var bag))
        {
            foreach (var pump in bag.Values)
            {
                await pump.DisposeAsync();
            }
        }
        await base.OnDisconnectedAsync(exception);
    }

    private static IReadOnlySet<FixEventKind> ParseKinds(string[]? names)
    {
        if (names is null || names.Length == 0)
            return new HashSet<FixEventKind> { FixEventKind.MarketDataIncrementalRefresh };
        var set = new HashSet<FixEventKind>(names.Length);
        foreach (var n in names)
        {
            if (string.IsNullOrWhiteSpace(n)) continue;
            if (Enum.TryParse<FixEventKind>(n, ignoreCase: true, out var k) && k != FixEventKind.Unknown)
                set.Add(k);
        }
        if (set.Count == 0) set.Add(FixEventKind.MarketDataIncrementalRefresh);
        return set;
    }
}
